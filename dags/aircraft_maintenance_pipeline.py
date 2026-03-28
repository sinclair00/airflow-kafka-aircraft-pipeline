from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="aircraft_maintenance_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="python /opt/airflow/scripts/producer.py"
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="python /opt/airflow/scripts/consumer.py"
    )

    validate_raw_events = BashOperator(
        task_id="validate_raw_events",
        bash_command="python /opt/airflow/scripts/validate_events.py"
    )

    transform_curated = BashOperator(
        task_id="transform_curated",
        bash_command="python /opt/airflow/scripts/transform_events.py"
    )

    build_summary_report = BashOperator(
        task_id="build_summary_report",
        bash_command="python /opt/airflow/scripts/build_summary.py"
    )

    run_producer >> run_consumer >> validate_raw_events >> transform_curated >> build_summary_report
    