import logging
import os
from io import StringIO

import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info("Starting transform_bronze_to_silver")

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
BRONZE_KEY = os.environ.get(
    "BRONZE_KEY", "bronze/aircraft_events/maintenance_events_bronze.csv"
)
SILVER_KEY = os.environ.get(
    "SILVER_KEY", "silver/aircraft_events/maintenance_events_silver.csv"
)

s3 = boto3.client("s3")

logger.info("Reading bronze file: s3://%s/%s", S3_BUCKET_NAME, BRONZE_KEY)

obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=BRONZE_KEY)
content = obj["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(content))

if df.empty:
    raise ValueError(f"Bronze file is empty: s3://{S3_BUCKET_NAME}/{BRONZE_KEY}")

df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")

# Silver = cleaned, deduplicated, analytics-ready
df = df.drop_duplicates(subset=["event_id"]).copy()
df = df[df["event_ts"].notna()].copy()
df["event_date"] = df["event_ts"].dt.date

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=SILVER_KEY,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv",
)

logger.info("Silver data written to s3://%s/%s", S3_BUCKET_NAME, SILVER_KEY)
logger.info("Silver row count: %s", len(df))
