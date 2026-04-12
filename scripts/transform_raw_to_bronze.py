import json
import logging
import os
from io import StringIO

import boto3
import pandas as pd

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

s3 = boto3.client("s3")

response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=RAW_PREFIX)

if "Contents" not in response or not response["Contents"]:
    raise FileNotFoundError(f"No raw files found in s3://{S3_BUCKET_NAME}/{RAW_PREFIX}")

raw_files = [obj for obj in response["Contents"] if obj["Key"].endswith(".jsonl")]

if not raw_files:
    raise FileNotFoundError(
        f"No .jsonl files found in s3://{S3_BUCKET_NAME}/{RAW_PREFIX}"
    )

latest_obj = max(raw_files, key=lambda x: x["LastModified"])
latest_key = latest_obj["Key"]

logger.info("Reading raw file: s3://%s/%s", S3_BUCKET_NAME, latest_key)

obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=latest_key)
content = obj["Body"].read().decode("utf-8")

rows = []
for line in content.splitlines():
    line = line.strip()
    if line:
        rows.append(json.loads(line))

if not rows:
    raise ValueError(
        f"No records found in raw file: s3://{S3_BUCKET_NAME}/{latest_key}"
    )

df = pd.DataFrame(rows).copy()

# Bronze = light standardization only
df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
df["ingested_at"] = pd.Timestamp.utcnow()
df["source_file"] = latest_key

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=BRONZE_KEY,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv",
)

logger.info("Bronze data written to s3://%s/%s", S3_BUCKET_NAME, BRONZE_KEY)
logger.info("Bronze row count: %s", len(df))
