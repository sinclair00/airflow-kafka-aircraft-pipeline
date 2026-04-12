import logging
import os
from io import StringIO

import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info("Starting build_summary_report")

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
SILVER_KEY = os.environ.get(
    "SILVER_KEY", "silver/aircraft_events/maintenance_events_silver.csv"
)
GOLD_KEY = os.environ.get("GOLD_KEY", "gold/aircraft_events/daily_summary.csv")

s3 = boto3.client("s3")

logger.info("Reading silver file: s3://%s/%s", S3_BUCKET_NAME, SILVER_KEY)

obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=SILVER_KEY)
content = obj["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(content))

if df.empty:
    raise ValueError(f"Silver file is empty: s3://{S3_BUCKET_NAME}/{SILVER_KEY}")

summary = (
    df.groupby(["event_date", "component", "severity", "status"])
    .size()
    .reset_index(name="event_count")
)

logger.info("Writing gold file: s3://%s/%s", S3_BUCKET_NAME, GOLD_KEY)

csv_buffer = StringIO()
summary.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=GOLD_KEY,
    Body=csv_buffer.getvalue(),
    ContentType="text/csv",
)

logger.info("Gold summary written to s3://%s/%s", S3_BUCKET_NAME, GOLD_KEY)
logger.info("Silver row count: %s", len(df))
logger.info("Gold row count: %s", len(summary))
