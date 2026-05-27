# Snowflake Setup

This project loads Gold-layer aircraft maintenance summary data into Snowflake for warehouse-based analytics and downstream reporting.

## Objects

- Database: AIRCRAFT_MAINTENANCE_DB
- Schema: GOLD
- Warehouse: AIRCRAFT_WH
- Table: GOLD.DAILY_MAINTENANCE_SUMMARY

Setup SQL is stored in:

`sql/snowflake/01_create_gold_table.sql`

Validation queries are stored in:

`sql/snowflake/02_validation_queries.sql`

Analytics queries are stored in:

`sql/snowflake/03_analytics_queries.sql`

## Load Flow

Kafka event data is processed through the Airflow/Python medallion pipeline:

RAW → BRONZE → SILVER → GOLD

The Gold output file (`daily_summary.csv`) is loaded into Snowflake for SQL analytics and future Power BI reporting.

## Notes

- Snowflake currently serves as the analytics warehouse layer.
- Transformations are performed upstream in Python/Airflow.
- Initial ingestion currently uses Snowflake UI-based loading.
