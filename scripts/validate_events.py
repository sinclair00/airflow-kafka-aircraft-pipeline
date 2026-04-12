import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info("Starting validate_events")

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/aircraft_events/")
VALIDATED_FILES_KEY = os.environ.get(
    "VALIDATED_FILES_KEY", "metadata/aircraft_events/validated_raw_files.json"
)
VALIDATION_REPORT_KEY = os.environ.get(
    "VALIDATION_REPORT_KEY", "analytics/aircraft_events/validation_report.txt"
)

s3 = boto3.client("s3")

required_cols = [
    "event_id",
    "event_ts",
    "aircraft_id",
    "component",
    "event_type",
    "severity",
    "status",
    "location",
]


def load_validated_files():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=VALIDATED_FILES_KEY)
        content = obj["Body"].read().decode("utf-8")
        validated_files = json.loads(content)
        logger.info("Loaded %d previously validated files", len(validated_files))
        return set(validated_files)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            logger.info("No validation manifest found yet. Starting fresh.")
            return set()
        raise


def save_validated_files(validated_files):
    payload = json.dumps(sorted(validated_files), indent=2)
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=VALIDATED_FILES_KEY,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        "Updated validated-files manifest at s3://%s/%s",
        S3_BUCKET_NAME,
        VALIDATED_FILES_KEY,
    )


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

validated_files = load_validated_files()
new_raw_files = [key for key in raw_files if key not in validated_files]

if not new_raw_files:
    logger.info("No new raw files to validate.")
    report_text = "No new raw files to validate."
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=VALIDATION_REPORT_KEY,
        Body=report_text.encode("utf-8"),
        ContentType="text/plain",
    )
    logger.info(
        "Validation report written to s3://%s/%s", S3_BUCKET_NAME, VALIDATION_REPORT_KEY
    )
    raise SystemExit(0)

logger.info("Found %d new raw files to validate", len(new_raw_files))

report_sections = []

for raw_key in new_raw_files:
    logger.info("Validating raw file: s3://%s/%s", S3_BUCKET_NAME, raw_key)

    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=raw_key)
    content = obj["Body"].read().decode("utf-8")

    rows = []
    for line in content.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))

    if not rows:
        raise ValueError(
            f"No records found in raw file: s3://{S3_BUCKET_NAME}/{raw_key}"
        )

    df = pd.DataFrame(rows)

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in {raw_key}: {missing_cols}")

    null_counts = df[required_cols].isnull().sum()
    dup_count = df["event_id"].duplicated().sum()

    report_sections.append(
        "\n".join(
            [
                f"Validated file: s3://{S3_BUCKET_NAME}/{raw_key}",
                f"Row count: {len(df)}",
                f"Duplicate event_id count: {dup_count}",
                "Null counts:",
                null_counts.to_string(),
                "-" * 60,
            ]
        )
    )

    validated_files.add(raw_key)

report_text = "\n".join(report_sections)

s3.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=VALIDATION_REPORT_KEY,
    Body=report_text.encode("utf-8"),
    ContentType="text/plain",
)

save_validated_files(validated_files)

logger.info("Validation complete.")
logger.info("New raw files validated: %d", len(new_raw_files))
logger.info(
    "Validation report written to s3://%s/%s", S3_BUCKET_NAME, VALIDATION_REPORT_KEY
)
