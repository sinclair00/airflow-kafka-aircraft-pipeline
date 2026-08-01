# Airflow 3 Migration Runbook

**Project:** `airflow-kafka-aircraft-pipeline`
**Project root:** `~/projects/airflow-kafka-aircraft-pipeline`
**Migration branch:** `upgrade-airflow-3`
**Rollback commit before migration:** `dade8fa`
**Migration date:** 2026-08-01
**Source:** Apache Airflow 2.9.3
**Target:** Apache Airflow 3.3.0
**Executor:** LocalExecutor
**Metadata database:** PostgreSQL 15
**Authentication:** Flask AppBuilder (FAB) Auth Manager
**Status:** Migration completed and validated

---

## 1. Purpose

This runbook records the controlled migration from Airflow 2.9.3 to Airflow 3.3.0, including database backup, secrets, authentication, Compose services, DAG changes, database migration, validation, encountered failures, and recovery steps.

## 2. Safety Rules

Never run:

```bash
docker compose down -v
```

Do not paste secrets into ChatGPT, Git, documentation, shell history, or Docker Compose.

The following must remain ignored by Git:

```gitignore
.env
/backups/
```

The `.env` file contains secrets. The `backups/` directory contains PostgreSQL dumps, including FAB authentication tables and password hashes.

## 3. Pre-Migration State

Original Airflow services:

```text
airflow-init
airflow-webserver
airflow-scheduler
```

Original image:

```dockerfile
FROM apache/airflow:2.9.3
```

Original DAG imports:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
```

Original schedule argument:

```python
schedule_interval=None
```

Original hardcoded secrets:

```yaml
AIRFLOW__WEBSERVER__SECRET_KEY: "my_super_secret_key_123"
```

```bash
airflow users create --username airflow --password airflow --firstname Don --lastname Sinclair --role Admin --email sinclair00@gmail.com
```

Existing FAB account:

```text
username: airflow
email: sinclair00@gmail.com
role: Admin
```

## 4. Airflow 3 Architecture

The migration retained `LocalExecutor` and used:

```text
airflow-init
airflow-api-server
airflow-scheduler
airflow-dag-processor
```

The API server replaces the old webserver. The standalone DAG processor is required in Airflow 3.

Redis and a Celery worker were not added because this project uses `LocalExecutor`. A triggerer was not added because the current DAG does not use deferrable operators.

## 5. Selected Versions

```text
apache-airflow==3.3.0
apache-airflow-providers-standard==1.16.0
apache-airflow-providers-fab==3.7.3
```

The Standard provider supplies `BashOperator`. The FAB provider preserves database-backed users, password hashes, roles, and permissions.

## 6. Branch and Rollback Point

The migration branch was created from clean and synchronized `main`:

```bash
git switch main
git pull
git switch -c upgrade-airflow-3
```

Rollback commit before migration:

```text
dade8fa
```

## 7. Back Up the Metadata Database

Start PostgreSQL if necessary:

```bash
docker compose up -d postgres
```

Create the backup directory:

```bash
mkdir -p backups
```

Create a compressed custom-format dump:

```bash
docker compose exec -T postgres pg_dump -U airflow -d airflow -Fc > backups/airflow_metadata_pre_airflow3_2026-08-01.dump
```

Verify the dump:

```bash
docker compose exec -T postgres pg_restore -l < backups/airflow_metadata_pre_airflow3_2026-08-01.dump | head -20
```

Validated output included:

```text
Format: CUSTOM
Dumped from database version: 15.17
```

Add to `.gitignore`:

```gitignore
# Local database backups
/backups/
```

Verify:

```bash
git check-ignore -v backups/airflow_metadata_pre_airflow3_2026-08-01.dump
```

## 8. Add Secrets to `.env`

Existing AWS variables remained unchanged.

Add three separate locally generated values:

```dotenv
AIRFLOW_API_SECRET_KEY=...
AIRFLOW_JWT_SECRET=...
AIRFLOW_ADMIN_PASSWORD=...
```

Responsibilities:

- `AIRFLOW_API_SECRET_KEY`: Airflow 3 API application secret.
- `AIRFLOW_JWT_SECRET`: shared internal JWT secret for API server and scheduler.
- `AIRFLOW_ADMIN_PASSWORD`: used only for the controlled FAB password reset.

The PostgreSQL password and Airflow administrator password are separate credentials.

Verify without displaying values:

```bash
awk -F= '/^(AIRFLOW_API_SECRET_KEY|AIRFLOW_JWT_SECRET|AIRFLOW_ADMIN_PASSWORD)=/ {print $1 ": " (length(substr($0,index($0,"=")+1)) > 0 ? "set" : "EMPTY")}' .env
```

Expected:

```text
AIRFLOW_API_SECRET_KEY: set
AIRFLOW_JWT_SECRET: set
AIRFLOW_ADMIN_PASSWORD: set
```

## 9. Update `requirements.txt`

Final contents:

```text
pandas
kafka-python
psycopg2-binary
boto3
apache-airflow-providers-standard==1.16.0
apache-airflow-providers-fab==3.7.3
```

## 10. Update `dockerfile`

The project file is lowercase `dockerfile`.

```dockerfile
FROM apache/airflow:3.3.0

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
```

Explicitly reinstalling the same Airflow version prevents dependency resolution from silently changing it.

## 11. Update the DAG

File:

```text
dags/aircraft_maintenance_pipeline.py
```

Use:

```python
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
```

Replace:

```python
schedule_interval=None
```

with:

```python
schedule=None
```

No task logic changed.

## 12. Update `docker-compose.yml`

### 12.1 FAB authentication

Add to all Airflow services:

```yaml
AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

FAB stands for Flask AppBuilder and preserves the PostgreSQL-backed users, password hashes, roles, and permissions.

### 12.2 `airflow-init`

Remove:

```yaml
AIRFLOW__WEBSERVER__SECRET_KEY: "my_super_secret_key_123"
```

Replace the old user creation command with:

```yaml
command: >
  bash -c "airflow db migrate &&
  airflow fab-db migrate"
```

Do not use `airflow users create` to update an existing user password.

### 12.3 API server

Rename:

```yaml
airflow-webserver:
```

to:

```yaml
airflow-api-server:
```

Change:

```yaml
command: webserver
```

to:

```yaml
command: api-server
```

Use:

```yaml
AIRFLOW__API__SECRET_KEY: ${AIRFLOW_API_SECRET_KEY}
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
```

Keep:

```yaml
ports:
  - "8080:8080"
```

### 12.4 Scheduler

Use:

```yaml
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
```

The Execution API URL is required for task execution under Airflow 3 with this Compose topology.

### 12.5 DAG processor

Add:

```yaml
  airflow-dag-processor:
    build: .
    depends_on:
      - postgres
      - airflow-init
    restart: unless-stopped
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    command: dag-processor
    volumes:
      - ./dags:/opt/airflow/dags
```

Validate Compose:

```bash
docker compose config --quiet
```

List services:

```bash
docker compose config --services
```

Expected Airflow services:

```text
airflow-init
airflow-api-server
airflow-scheduler
airflow-dag-processor
```

## 13. Build Airflow 3 Images

```bash
docker compose build airflow-init airflow-api-server airflow-scheduler airflow-dag-processor
```

Verify version:

```bash
docker run --rm --entrypoint airflow airflow-kafka-aircraft-pipeline-airflow-scheduler version
```

Expected:

```text
3.3.0
```

Verify providers and imports:

```bash
docker run --rm --entrypoint python airflow-kafka-aircraft-pipeline-airflow-scheduler -c "from importlib.metadata import version; from airflow.sdk import DAG; from airflow.providers.standard.operators.bash import BashOperator; from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager; print('standard provider:', version('apache-airflow-providers-standard')); print('FAB provider:', version('apache-airflow-providers-fab')); print('Airflow 3 imports: OK')"
```

Expected:

```text
standard provider: 1.16.0
FAB provider: 3.7.3
Airflow 3 imports: OK
```

## 14. Remove the Obsolete Airflow 2 Webserver Container

Verify:

```bash
docker ps -a --filter "name=airflow-kafka-aircraft-pipeline-airflow-webserver-1"
```

Remove only the exited orphan:

```bash
docker rm airflow-kafka-aircraft-pipeline-airflow-webserver-1
```

This does not remove volumes, images, PostgreSQL data, or project files.

## 15. Run Database Migrations

Verify FAB migration command:

```bash
docker compose run --rm --no-deps airflow-init airflow fab-db --help
```

Run:

```bash
docker compose run --rm airflow-init
```

This executes:

```bash
airflow db migrate
airflow fab-db migrate
```

Successful output included:

```text
Database migration done!
FABDBManager tables already exist without an Alembic version
Running upgrade ... Fix fab db inconsistencies.
Database migration done!
```

## 16. Verify Existing Administrator

```bash
docker compose run --rm airflow-init airflow users list
```

Validated account:

```text
1 | airflow | sinclair00@gmail.com | Don | Sinclair | Admin
```

## 17. Reset the Existing Administrator Password

Run interactively:

```bash
docker compose run --rm airflow-init airflow users reset-password --username airflow
```

Use `Ctrl+Shift+V` to paste from NordPass. Nothing appears on screen while pasting. Do not use `--password` because it can expose the value in shell history or process listings.

### Stale session failure encountered

The first reset failed with:

```text
msgspec.DecodeError: MessagePack data is malformed: trailing characters
```

Old Airflow 2 session rows could not be decoded by the Airflow 3 FAB session serializer.

Count sessions:

```bash
docker compose exec -T postgres psql -U airflow -d airflow -c "SELECT COUNT(*) AS session_count FROM session;"
```

Result:

```text
4
```

Delete only stale web sessions:

```bash
docker compose exec -T postgres psql -U airflow -d airflow -c "DELETE FROM session;"
```

Result:

```text
DELETE 4
```

This logs out old browser sessions only. It does not delete users, roles, DAGs, runs, task history, connections, or variables.

Retry the reset. Expected:

```text
User "airflow" password reset successfully
```

## 18. Start Airflow 3

```bash
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
```

Verify:

```bash
docker compose ps
```

Expected running services:

```text
airflow-api-server
airflow-scheduler
airflow-dag-processor
postgres
```

## 19. Review Startup Logs

```bash
docker compose logs --tail=80 airflow-api-server airflow-scheduler airflow-dag-processor
```

Healthy output included:

```text
Application startup complete.
Uvicorn running on http://0.0.0.0:8080
Loaded executor: LocalExecutor
Starting the scheduler
Starting the Dag Processor Job
Found 1 files for bundle dags-folder
# DAGs: 1
# Errors: 0
```

Warnings observed but not treated as failures:

```text
starlette.middleware.wsgi is deprecated
HTTP_422_UNPROCESSABLE_ENTITY is deprecated
appbuilder.app is deprecated
Using the in-memory storage for tracking rate limits
```

The in-memory rate-limit warning is acceptable for this local portfolio deployment, not for production.

## 20. Verify DAG Parsing

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected:

```text
No data found
```

```bash
docker compose exec airflow-scheduler airflow dags list | grep aircraft_maintenance_pipeline
```

Expected DAG:

```text
aircraft_maintenance_pipeline
```

## 21. Verify GUI and Login

Open:

```text
http://localhost:8080
```

Login with username `airflow` and the new password stored in NordPass.

Validated:

- Airflow 3 GUI loaded.
- FAB login succeeded.
- Dark mode was available.
- The DAG appeared in the UI.

Suggested screenshot path:

```text
docs/images/airflow/airflow-3-aircraft-maintenance-success.png
```

## 22. Kafka Validation

Start Kafka:

```bash
docker compose up -d kafka
```

Verify:

```bash
docker compose ps kafka
```

### Kafka CLI JMX issue

Kafka CLI commands inherited `KAFKA_OPTS` and attempted to start a second JMX exporter on port 7071, causing:

```text
java.net.BindException: Address already in use
Prometheus JMX Exporter exiting
```

The broker itself remained healthy.

Clear `KAFKA_OPTS` only for CLI commands:

```bash
docker compose exec -e KAFKA_OPTS= kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Expected:

```text
__consumer_offsets
aircraft_maintenance_events
```

## 23. Prometheus Validation

Start Prometheus:

```bash
docker compose up -d prometheus
```

Verify:

```bash
docker compose ps prometheus
```

Check Kafka target:

```bash
curl -sG --data-urlencode 'query=up{job="kafka"}' http://localhost:9090/api/v1/query | python3 -m json.tool
```

Validated:

```text
instance: kafka:7071
job: kafka
value: 1
```

## 24. Record Pre-Run Kafka Offset

```bash
docker compose exec -e KAFKA_OPTS= kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic aircraft_maintenance_events
```

Pre-run result:

```text
aircraft_maintenance_events:0:100
```

## 25. First DAG Run Failure and Fix

Trigger:

```bash
docker compose exec airflow-scheduler airflow dags trigger aircraft_maintenance_pipeline
```

First run ID:

```text
manual__2026-08-01T22:41:04.506551+00:00
```

`run_producer` failed before the script ran. Scheduler logs showed:

```text
httpx.ConnectError: [Errno 111] Connection refused
```

Cause:

The Airflow 3 task worker attempted to use:

```text
http://localhost:8080/execution/
```

Inside the scheduler container, `localhost` referred to the scheduler container, not the API server.

Fix the scheduler environment:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
```

Validate:

```bash
docker compose config --quiet
```

Recreate only the scheduler:

```bash
docker compose up -d --force-recreate airflow-scheduler
```

Verify:

```bash
docker compose exec airflow-scheduler airflow config get-value core execution_api_server_url
```

Expected:

```text
http://airflow-api-server:8080/execution/
```

Connectivity test:

```bash
docker compose exec airflow-scheduler python -c "import socket; socket.create_connection(('airflow-api-server', 8080), timeout=5).close(); print('scheduler-to-api-server: connected')"
```

Expected:

```text
scheduler-to-api-server: connected
```

## 26. Successful DAG Run

Trigger a new run:

```bash
docker compose exec airflow-scheduler airflow dags trigger aircraft_maintenance_pipeline
```

Successful run ID:

```text
manual__2026-08-01T22:53:22.373628+00:00
```

Check DAG state:

```bash
docker compose exec airflow-scheduler airflow dags state aircraft_maintenance_pipeline 'manual__2026-08-01T22:53:22.373628+00:00'
```

Result:

```text
success
```

Check all tasks:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run aircraft_maintenance_pipeline 'manual__2026-08-01T22:53:22.373628+00:00'
```

Validated:

```text
run_producer               success
run_consumer               success
validate_raw_events        success
transform_raw_to_bronze    success
transform_bronze_to_silver success
build_summary_report       success
```

## 27. Kafka Message Validation

```bash
docker compose exec -e KAFKA_OPTS= kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic aircraft_maintenance_events
```

Post-run result:

```text
aircraft_maintenance_events:0:200
```

Validation:

```text
Before run: 100
After run:  200
Increase:   100 messages
```

## 28. Prometheus Message Validation

The 10-minute `increase()` query initially returned `0` because Prometheus did not have a suitable sample before the message burst.

Raw topic counter query:

```bash
curl -sG --data-urlencode 'query=kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}' http://localhost:9090/api/v1/query | python3 -m json.tool
```

Validated value:

```text
100
```

Final evidence:

```text
Kafka topic offset increased by 100.
Prometheus Kafka target was UP = 1.
Prometheus exposed the topic message counter at 100.
```

## 29. Final Validation Checklist

- [x] Airflow 3.3.0 image verified
- [x] Standard provider 1.16.0 verified
- [x] FAB provider 3.7.3 verified
- [x] Airflow 3 DAG imports verified
- [x] PostgreSQL backup created and validated
- [x] Backup directory ignored by Git
- [x] Hardcoded webserver secret removed
- [x] Hardcoded admin password removed
- [x] FAB authentication configured
- [x] Existing admin account preserved
- [x] Existing admin password reset safely
- [x] Core database migrated
- [x] FAB database migrated
- [x] Old Airflow 2 webserver container removed
- [x] API server running
- [x] Scheduler running
- [x] DAG processor running
- [x] DAG import errors: none
- [x] Airflow 3 GUI and FAB login verified
- [x] Dark mode verified
- [x] Kafka broker and topic verified
- [x] Prometheus Kafka target UP
- [x] Successful DAG run completed
- [x] All six tasks succeeded
- [x] Kafka offset increased by 100
- [x] Prometheus message counter exposed

## 30. Git Review

```bash
git status --short --branch
```

Expected tracked modifications:

```text
.gitignore
dags/aircraft_maintenance_pipeline.py
docker-compose.yml
dockerfile
requirements.txt
```

The `.env` file and `backups/` directory must not appear.

Validate formatting:

```bash
git diff --check
```

Review:

```bash
git --no-pager diff
```

Search for old hardcoded values:

```bash
grep -RIn --exclude-dir=.git --exclude='.env' --exclude='*.dump' -E 'my_super_secret_key_123|--password airflow|AIRFLOW__WEBSERVER__SECRET_KEY' .
```

Expected: no matches.

## 31. Suggested Documentation Updates

Update:

```text
README.md
docs/runbooks/airflow_pipeline_run.md
docs/runbooks/safe_shutdown_restart.md
docs/runbooks/docker_wsl_recovery.md
```

Document:

- Airflow 3.3.0
- API server replacing webserver
- Standalone DAG processor
- FAB provider and database-backed authentication
- Execution API URL requirement
- Password reset procedure
- Stale session cleanup
- Kafka CLI `KAFKA_OPTS` workaround
- Current service names
- Airflow 3 dark-mode screenshot

## 32. Suggested Commit

After copying this runbook into the repository:

```bash
git add .gitignore dockerfile requirements.txt docker-compose.yml dags/aircraft_maintenance_pipeline.py docs/runbooks/airflow_3_migration_runbook.md
```

```bash
git commit -m "Upgrade Airflow to 3.3.0"
```

```bash
git push -u origin upgrade-airflow-3
```

Never add:

```text
.env
backups/
```

## 33. Rollback Guidance

Pre-migration Git commit:

```text
dade8fa
```

Pre-migration database dump:

```text
backups/airflow_metadata_pre_airflow3_2026-08-01.dump
```

Do not run Airflow 2.9.3 against the already migrated Airflow 3 metadata database.

A full rollback requires both:

1. Restoring the source files to the Airflow 2 state.
2. Restoring PostgreSQL from the pre-migration dump.

Stop Airflow services before rollback work:

```bash
docker compose stop airflow-api-server airflow-scheduler airflow-dag-processor airflow-init
```

Never use:

```bash
docker compose down -v
```

The database restore procedure should be documented and rehearsed separately before use.

## 34. Useful Terminal Shortcuts

Clear a recalled or partially typed command:

```text
Ctrl+C
```

Clear from the cursor to the beginning of the line:

```text
Ctrl+U
```

Paste into the VS Code terminal:

```text
Ctrl+Shift+V
```

Show or hide the existing terminal panel:

```text
Ctrl+`
```

## 35. Final Result

Validated architecture:

```text
Kafka KRaft
Airflow 3.3.0 API Server
Airflow Scheduler
Airflow DAG Processor
LocalExecutor
FAB Auth Manager
PostgreSQL 15
Python
S3 raw/bronze/silver/gold
Prometheus
Kafka JMX Exporter
Docker Compose
```

The successful validation run completed all six tasks, added 100 Kafka messages, and remained observable through Prometheus.
