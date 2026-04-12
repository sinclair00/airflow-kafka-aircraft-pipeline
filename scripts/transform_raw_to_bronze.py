import json
import logging
import os
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info("Starting transform_raw_to_bronze")

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/aircraft_events/")
BRONZE_KEY = os.environ.get(
    "BRONZE_KEY", "bronze/aircraft_events/maintenance_events_bronze.csv"
)
BRONZE_PROCESSED_FILES_KEY = os.environ.get(
    "BRONZE_PROCESSED_FILES_KEY",
    "metadata/aircraft_events/bronze_processed_raw_files.json",
)

s3 = boto3.client("s3")


def load_processed_raw_files():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=BRONZE_PROCESSED_FILES_KEY)
        content = obj["Body"].read().decode("utf-8")
        processed_files = json.loads(content)
        logger.info(
            "Loaded %d previously bronze-processed raw files", len(processed_files)
        )
        return set(processed_files)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            logger.info("No bronze manifest found yet. Starting fresh.")
            return set()
        raise


def save_processed_raw_files(processed_files):
    payload = json.dumps(sorted(processed_files), indent=2)
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=BRONZE_PROCESSED_FILES_KEY,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        "Updated bronze manifest at s3://%s/%s",
        S3_BUCKET_NAME,
        BRONZE_PROCESSED_FILES_KEY,
    )


def load_existing_bronze_df():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=BRONZE_KEY)
        content = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
        logger.info("Loaded existing bronze rows: %d", len(df))
        return df
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            logger.info(
                "No existing bronze file found yet. Creating new bronze dataset."
            )
            return pd.DataFrame()
        raise


response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=RAW_PREFIX)

if "Contents" not in response or not response["Contents"]:
    raise FileNotFoundError(f"No raw files found in s3://{S3_BUCKET_NAME}/{RAW_PREFIX}")

raw_files = sorted(
    [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".jsonl")]
)

if not raw_files:
    raise FileNotFoundError(
        f"No .jsonl files found in s3://{S3_BUCKET_NAME}/{RAW_PREFIX}"
    )

processed_raw_files = load_processed_raw_files()
new_raw_files = [key for key in raw_files if key not in processed_raw_files]

if not new_raw_files:
    logger.info("No new raw files to load into bronze.")
    logger.info("Bronze step completed with no changes.")
    raise SystemExit(0)

logger.info("Found %d new raw files to load into bronze", len(new_raw_files))

new_rows = []

for raw_key in new_raw_files:
    logger.info("Reading raw file: s3://%s/%s", S3_BUCKET_NAME, raw_key)

    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=raw_key)
    content = obj["Body"].read().decode("utf-8")

    for line in content.splitlines():
        line = line.strip()
        if line:
            record = json.loads(line)
            record["source_file"] = raw_key
            new_rows.append(record)

if not new_rows:
    raise ValueError(
        f"No records found in new raw files under s3://{S3_BUCKET_NAME}/{RAW_PREFIX}"
    )

new_df = pd.DataFrame(new_rows).copy()

# Bronze = light standardization only
new_df["event_ts"] = pd.to_datetime(new_df["event_ts"], errors="coerce")
new_df["ingested_at"] = pd.Timestamp.utcnow()

existing_bronze_df = load_existing_bronze_df()

if existing_bronze_df.empty:
    bronze_df = new_df
else:
    bronze_df = pd.concat([existing_bronze_df, new_df], ignore_index=True, sort=False)

csv_buffer = StringIO()
bronze_df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=BRONZE_KEY,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv",
)

processed_raw_files.update(new_raw_files)
save_processed_raw_files(processed_raw_files)

logger.info("Bronze data written to s3://%s/%s", S3_BUCKET_NAME, BRONZE_KEY)
logger.info("New bronze rows appended: %s", len(new_df))
logger.info("Total bronze row count: %s", len(bronze_df))
