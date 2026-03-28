import glob
import json
import os
import pandas as pd

raw_files = sorted(glob.glob("/opt/airflow/data/raw/*.jsonl"))
if not raw_files:
    raise FileNotFoundError("No raw files found.")

latest = raw_files[-1]
rows = []
with open(latest, "r", encoding="utf-8") as f:
    for line in f:
        rows.append(json.loads(line))

df = pd.DataFrame(rows)
df = df.drop_duplicates(subset=["event_id"]).copy()
df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
df["event_date"] = df["event_ts"].dt.date

os.makedirs("/opt/airflow/data/curated", exist_ok=True)
outfile = "/opt/airflow/data/curated/maintenance_events.csv"
df.to_csv(outfile, index=False)

print(f"Curated data written to {outfile}")