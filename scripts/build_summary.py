import os
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Starting build_summary")

df = pd.read_csv("/opt/airflow/data/curated/maintenance_events.csv")

summary = (
    df.groupby(["event_date", "component", "severity", "status"])
    .size()
    .reset_index(name="event_count")
)

os.makedirs("/opt/airflow/data/analytics", exist_ok=True)
outfile = "/opt/airflow/data/analytics/daily_summary.csv"
summary.to_csv(outfile, index=False)

logger.info("Summary written to %s", outfile)
