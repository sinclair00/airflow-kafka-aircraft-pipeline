# Airflow Aircraft Maintenance Pipeline Runbook

**Last updated:** 2026-07-31

## Purpose

This runbook documents how to start, trigger, validate, and troubleshoot the `aircraft_maintenance_pipeline` DAG.

The DAG orchestrates a six-step event-processing workflow:

```text
run_producer
  → run_consumer
  → validate_raw_events
  → transform_raw_to_bronze
  → transform_bronze_to_silver
  → build_summary_report
```

The current pipeline uses:

```text
Airflow 2.9.3
  → Kafka KRaft
  → S3 raw JSON Lines
  → S3 bronze CSV
  → S3 silver CSV
  → S3 gold summary CSV
```

## Repository Files

```text
dags/aircraft_maintenance_pipeline.py
scripts/producer.py
scripts/consumer.py
scripts/validate_events.py
scripts/transform_raw_to_bronze.py
scripts/transform_bronze_to_silver.py
scripts/build_summary_report.py
docker-compose.yml
dockerfile
requirements.txt
.env
```

## DAG Configuration

The DAG is defined as:

```text
DAG ID: aircraft_maintenance_pipeline
Schedule: manual only
Catchup: disabled
Retries: 1
Retry delay: 1 minute
Executor: LocalExecutor
```

Each task uses `set -e`, so the Airflow task fails when its Python command exits with a nonzero status.

## Pipeline Tasks

### 1. `run_producer`

Runs:

```text
scripts/producer.py
```

The producer:

- connects to Kafka at `kafka:29092`;
- publishes to `aircraft_maintenance_events`;
- generates exactly 100 simulated aircraft-maintenance events;
- serializes each event as JSON;
- assigns a UUID `event_id` and UTC `event_ts`;
- waits for broker acknowledgement for every message;
- flushes and closes the producer cleanly.

### 2. `run_consumer`

Runs:

```text
scripts/consumer.py
```

The consumer:

- reads from `aircraft_maintenance_events`;
- uses consumer group `aircraft-maintenance-consumer-group`;
- uses `auto_offset_reset="earliest"` when no committed offset exists;
- uses automatic offset commits;
- stops after 10 seconds without new messages;
- writes consumed events to a timestamped JSON Lines object in S3;
- skips the S3 write when no messages are consumed.

Default raw prefix:

```text
raw/aircraft_events/
```

Example raw object:

```text
raw/aircraft_events/events_20260731_195900.jsonl
```

### 3. `validate_raw_events`

Runs:

```text
scripts/validate_events.py
```

The validation task:

- lists raw `.jsonl` objects in S3;
- validates only objects not already recorded in its manifest;
- verifies required columns;
- counts null values in required columns;
- counts duplicate `event_id` values;
- writes a validation report to S3;
- records completed files in a validation manifest;
- exits successfully when there are no new raw files.

Required columns:

```text
event_id
event_ts
aircraft_id
component
event_type
severity
status
location
```

Default validation objects:

```text
metadata/aircraft_events/validated_raw_files.json
analytics/aircraft_events/validation_report.txt
```

### 4. `transform_raw_to_bronze`

Runs:

```text
scripts/transform_raw_to_bronze.py
```

The bronze task:

- reads raw `.jsonl` objects not already listed in the bronze manifest;
- adds `source_file` lineage;
- converts `event_ts` to a timestamp;
- adds `ingested_at`;
- appends new rows to the existing bronze dataset;
- records processed raw files in a manifest;
- exits successfully when there are no new files.

Default bronze objects:

```text
bronze/aircraft_events/maintenance_events_bronze.csv
metadata/aircraft_events/bronze_processed_raw_files.json
```

### 5. `transform_bronze_to_silver`

Runs:

```text
scripts/transform_bronze_to_silver.py
```

The silver task:

- reads the bronze CSV;
- fails if the bronze dataset is empty;
- converts `event_ts` to a datetime;
- removes duplicate `event_id` rows;
- removes rows with invalid or missing timestamps;
- adds `event_date`;
- rewrites the cleaned silver dataset.

Default silver object:

```text
silver/aircraft_events/maintenance_events_silver.csv
```

### 6. `build_summary_report`

Runs:

```text
scripts/build_summary_report.py
```

The gold task:

- reads the silver CSV;
- fails if the silver dataset is empty;
- groups records by `event_date`, `component`, `severity`, and `status`;
- calculates `event_count`;
- writes the analytics summary to S3.

Default gold object:

```text
gold/aircraft_events/daily_summary.csv
```

## Prerequisites

Docker Desktop must be running and connected to WSL.

From WSL, verify Docker:

```bash
docker version
```

The output must include both:

```text
Client:
Server:
```

Start from the project root:

```bash
cd ~/projects/airflow-kafka-aircraft-pipeline
```

The `.env` file must provide these required values without being committed to Git:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
S3_BUCKET_NAME
```

Do not print credential values into terminal logs or documentation.

## Configuration Note: S3 Variable Names

The current Compose file passes these variables:

```text
S3_RAW_PREFIX
S3_BRONZE_PREFIX
S3_GOLD_PREFIX
```

The current Python scripts do not read those names. They use defaults or these names instead:

```text
RAW_PREFIX
BRONZE_KEY
SILVER_KEY
GOLD_KEY
VALIDATED_FILES_KEY
VALIDATION_REPORT_KEY
BRONZE_PROCESSED_FILES_KEY
```

Therefore, the current pipeline works from script defaults unless those script-specific variables are added to the Airflow container environment. The unused `S3_*_PREFIX` variables are retained configuration debt and should be cleaned up in a separate change rather than during routine pipeline operation.

## 1. Start or Reconcile the Stack

Run:

```bash
docker compose up -d
```

This creates, starts, or reconciles the services from the current Compose definition.

## 2. Verify Airflow Services

Run:

```bash
docker compose ps
```

Expected active Airflow services:

```text
airflow-webserver
airflow-scheduler
postgres
```

`airflow-init` may show as exited with status `0`. That is normal after initialization completes.

Check scheduler logs:

```bash
docker compose logs --tail=100 airflow-scheduler
```

Check webserver logs:

```bash
docker compose logs --tail=100 airflow-webserver
```

## 3. Open the Airflow UI

Open:

```text
http://localhost:8080
```

Current local administrator credentials are created by `airflow-init` from the Compose command. Treat them as local development credentials only.

## 4. Confirm the DAG Is Loaded

Run:

```bash
docker compose exec airflow-scheduler \
  airflow dags list
```

Expected DAG:

```text
aircraft_maintenance_pipeline
```

List the task sequence:

```bash
docker compose exec airflow-scheduler \
  airflow tasks list aircraft_maintenance_pipeline --tree
```

Expected task order:

```text
run_producer
  run_consumer
    validate_raw_events
      transform_raw_to_bronze
        transform_bronze_to_silver
          build_summary_report
```

## 5. Unpause the DAG

Run:

```bash
docker compose exec airflow-scheduler \
  airflow dags unpause aircraft_maintenance_pipeline
```

The DAG is manual-only, so unpausing it does not create a schedule. It permits manual execution.

## 6. Trigger the Pipeline

Run:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger aircraft_maintenance_pipeline
```

The initial state may appear as:

```text
queued
```

## 7. Confirm the DAG Run Succeeded

Wait approximately 30–60 seconds, then run:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs \
  --dag-id aircraft_maintenance_pipeline
```

The newest manual run should reach:

```text
success
```

In the Airflow UI, open the DAG and confirm all six task instances are green.

Recommended evidence file:

```text
docs/images/prometheus/airflow-dag-success.png
```

## 8. Review Task Logs

Use the Airflow UI to open each task log, or inspect scheduler output when troubleshooting.

Important success messages include:

```text
Produced 100 confirmed events
Wrote <count> events to s3://<bucket>/<raw-key>
Validation complete.
Bronze data written to s3://<bucket>/<bronze-key>
Silver data written to s3://<bucket>/<silver-key>
Gold summary written to s3://<bucket>/<gold-key>
```

The consumed count may differ from exactly 100 when consumer-group offsets, prior messages, or reruns affect what is available to the consumer. Validate the actual run from task logs and downstream object counts rather than assuming every rerun writes exactly 100 raw rows.

## 9. Validate Kafka Production

Because the Kafka container uses `KAFKA_OPTS` for the JMX Exporter, clear it for Kafka CLI commands.

List the topic:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics --bootstrap-server localhost:9092 --list
```

Expected topic:

```text
aircraft_maintenance_events
```

Check the end offset:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic aircraft_maintenance_events \
  --time -1
```

Expected format:

```text
aircraft_maintenance_events:0:<offset>
```

A successful producer run should increase the topic end offset by 100.

## 10. Validate S3 Outputs

Confirm the run produced or updated the expected objects:

```text
raw/aircraft_events/events_<timestamp>.jsonl
metadata/aircraft_events/validated_raw_files.json
analytics/aircraft_events/validation_report.txt
bronze/aircraft_events/maintenance_events_bronze.csv
metadata/aircraft_events/bronze_processed_raw_files.json
silver/aircraft_events/maintenance_events_silver.csv
gold/aircraft_events/daily_summary.csv
```

Validation should confirm:

- a new timestamped raw object exists when messages were consumed;
- the validation report describes newly validated raw files;
- the bronze row count increases only for previously unprocessed raw files;
- the silver dataset contains unique `event_id` values with valid timestamps;
- the gold dataset contains grouped `event_count` values.

## 11. Idempotency and Rerun Behavior

The pipeline is partially idempotent through S3 manifests and silver-layer deduplication:

- validation skips raw files already listed in its manifest;
- bronze processing skips raw files already listed in its manifest;
- silver removes duplicate `event_id` values;
- gold is rebuilt from the current silver dataset.

The Kafka producer is not idempotent at the business-event level. Every producer run creates 100 new UUID-based events.

The consumer uses automatic offset commits. Rerun behavior depends on the committed offsets for:

```text
aircraft-maintenance-consumer-group
```

Do not delete consumer-group state or S3 manifest objects during routine validation unless intentionally testing recovery behavior.

## 12. Troubleshooting

### DAG does not appear

Check the scheduler:

```bash
docker compose ps airflow-scheduler
```

Check for import errors:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-import-errors
```

Confirm the DAG file is mounted:

```bash
docker compose exec airflow-scheduler \
  ls -l /opt/airflow/dags
```

### Producer cannot connect to Kafka

Check Kafka:

```bash
docker compose ps kafka
```

Check Kafka logs:

```bash
docker compose logs --tail=150 kafka
```

The producer must connect to the internal listener:

```text
kafka:29092
```

### Consumer writes no S3 object

The consumer intentionally skips the S3 write when no messages are consumed.

Check:

- whether the producer task succeeded;
- whether the consumer group already committed offsets past the available messages;
- whether the consumer log says `No events consumed during this run`;
- whether AWS credentials, region, and bucket name are present in the Airflow container.

Show environment-variable names without printing secret values:

```bash
docker compose exec airflow-scheduler sh -c \
  'env | cut -d= -f1 | grep -E "^(AWS_|S3_)" | sort'
```

### S3 access fails

Check the failing Airflow task log for the AWS error code.

Confirm these variables are passed to the Airflow containers:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
S3_BUCKET_NAME
```

Do not paste credential values into tickets, screenshots, commits, or runbooks.

### Validation reports no new raw files

This is expected when every raw object is already present in:

```text
metadata/aircraft_events/validated_raw_files.json
```

The task exits successfully in this condition.

### Bronze reports no new files

This is expected when every raw object is already present in:

```text
metadata/aircraft_events/bronze_processed_raw_files.json
```

The task exits successfully without modifying bronze.

### Silver or gold task fails with an empty input

Confirm the preceding S3 object exists and contains rows:

```text
bronze/aircraft_events/maintenance_events_bronze.csv
silver/aircraft_events/maintenance_events_silver.csv
```

Then review the preceding Airflow task logs.

## 13. Validation Checklist

The Airflow pipeline is validated when all of the following are true:

- Airflow webserver and scheduler are running.
- `aircraft_maintenance_pipeline` appears in `airflow dags list`.
- All six tasks appear in the expected dependency order.
- The newest DAG run reaches `success`.
- The producer confirms 100 Kafka events.
- The Kafka end offset increases by 100 for the producer run.
- The consumer either writes a new raw S3 object or clearly reports why no new messages were consumed.
- Validation and bronze manifests prevent repeat processing of the same raw files.
- The silver dataset is cleaned and deduplicated by `event_id`.
- The gold summary is written successfully.
- The Airflow UI provides screenshot evidence of the successful run.

## 14. Safe Shutdown

When taking a break, stop the services without deleting containers or volumes:

```bash
docker compose stop
```

Avoid:

```bash
docker compose down -v
```

The `-v` option removes named volumes, including Kafka KRaft data and Prometheus history.

## 15. Related Runbooks

```text
docs/runbooks/kafka_jmx_exporter.md
docs/runbooks/prometheus_kafka_validation_runbook.md
docs/runbooks/kafka_kraft_migration_runbook.md
docs/runbooks/safe_shutdown_restart.md
docs/runbooks/docker_wsl_recovery.md
```
