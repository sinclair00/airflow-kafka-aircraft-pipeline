import os
import pandas as pd

df = pd.read_csv("/opt/airflow/data/curated/maintenance_events.csv")

summary = (
    df.groupby(["event_date", "component", "severity", "status"])
      .size()
      .reset_index(name="event_count")
)

os.makedirs("/opt/airflow/data/analytics", exist_ok=True)
outfile = "/opt/airflow/data/analytics/daily_summary.csv"
summary.to_csv(outfile, index=False)

print(f"Summary written to {outfile}")