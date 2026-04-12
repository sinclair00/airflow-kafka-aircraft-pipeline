from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import timedelta

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="aircraft_maintenance_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["kafka", "s3", "elt"],
) as dag:

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="set -e; python3 /opt/airflow/scripts/producer.py",
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="set -e; python3 /opt/airflow/scripts/consumer.py",
    )

    validate_raw_events = BashOperator(
        task_id="validate_raw_events",
        bash_command="set -e; python3 /opt/airflow/scripts/validate_events.py",
    )

    transform_raw_to_bronze = BashOperator(
        task_id="transform_raw_to_bronze",
        bash_command="set -e; python3 /opt/airflow/scripts/transform_raw_to_bronze.py",
    )

    transform_bronze_to_silver = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command="set -e; python3 /opt/airflow/scripts/transform_bronze_to_silver.py",
    )

    build_summary_report = BashOperator(
        task_id="build_summary_report",
        bash_command="set -e; python3 /opt/airflow/scripts/build_summary_report.py",
    )

    (
        run_producer
        >> run_consumer
        >> validate_raw_events
        >> transform_raw_to_bronze
        >> transform_bronze_to_silver
        >> build_summary_report
    )
