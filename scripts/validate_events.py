import glob
import json
import os
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Starting validate_events")

raw_files = sorted(glob.glob("/opt/airflow/data/raw/*.jsonl"))
if not raw_files:
    raise FileNotFoundError("No raw files found.")

latest = raw_files[-1]
rows = []
with open(latest, "r", encoding="utf-8") as f:
    for line in f:
        rows.append(json.loads(line))

df = pd.DataFrame(rows)

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

missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

null_counts = df[required_cols].isnull().sum()
dup_count = df["event_id"].duplicated().sum()

os.makedirs("/opt/airflow/data/analytics", exist_ok=True)
with open(
    "/opt/airflow/data/analytics/validation_report.txt", "w", encoding="utf-8"
) as f:
    f.write(f"Validated file: {latest}\n")
    f.write(f"Row count: {len(df)}\n")
    f.write(f"Duplicate event_id count: {dup_count}\n")
    f.write("Null counts:\n")
    f.write(null_counts.to_string())

logger.info("Validation complete.")
