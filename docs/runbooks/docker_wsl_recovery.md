# Docker Desktop and WSL Recovery Runbook

**Last updated:** 2026-07-31

## Purpose

This runbook documents how to recover the aircraft maintenance project when Docker commands fail from WSL, Docker Desktop loses WSL integration, Docker Compose becomes unavailable, or the Docker runtime stops responding.

Use this runbook when you see symptoms such as:

```text
Cannot connect to the Docker daemon
docker: command not found
/usr/bin/docker: Input/output error
/mnt/c/Program Files/Docker/Docker/resources/bin/docker: Input/output error
```

It also applies when Docker Desktop was updated, Windows was restarted, WSL was shut down, or VS Code lost its WSL connection.

## Environment

The project runs through:

```text
Windows
  → Docker Desktop
  → WSL 2 integration
  → Ubuntu
  → Docker Compose
  → PostgreSQL, Kafka KRaft, Airflow, Prometheus
```

Docker Desktop is not only a graphical client. On this workstation it provides the Docker runtime, Compose integration, and WSL connection used by the project.

## Recovery Principle

Recover the environment from the outside inward:

```text
Windows
  → Docker Desktop
  → WSL
  → Docker CLI
  → Docker Compose
  → project containers
```

Do not begin by deleting containers or volumes.

## 1. Preserve Project State

Do not run:

```bash
docker compose down -v
```

Do not manually delete project volumes.

The project contains persistent state including:

```text
kafka-kraft-data
prometheus-data
```

A WSL or Docker Desktop failure normally does not require data deletion.

## 2. Check Whether Docker Desktop Is Running

In Windows, open Docker Desktop.

Wait until Docker Desktop reports that the Docker engine is running.

After an update, Docker Desktop may close and require manual reopening.

Do not continue until Docker Desktop finishes starting.

## 3. Test Docker from WSL

Open the Ubuntu WSL terminal or reconnect VS Code to WSL.

From WSL, run:

```bash
docker version
```

A working connection shows both:

```text
Client:
Server:
```

If the `Server:` section is missing, Docker Desktop is not fully connected to WSL.

Also check Compose:

```bash
docker compose version
```

## 4. Check Docker Desktop WSL Integration

In Docker Desktop:

```text
Settings
  → Resources
  → WSL Integration
```

Confirm that integration is enabled for the Ubuntu distribution used by this project.

Apply and restart Docker Desktop if the setting changes.

Then return to WSL and rerun:

```bash
docker version
```

## 5. Restart the WSL Environment

When Docker commands return an input/output error, WSL may have a stale or damaged runtime connection.

Close active WSL terminals and VS Code WSL windows.

Open Windows PowerShell and run:

```powershell
wsl --shutdown
```

This stops all WSL distributions and the WSL virtual machine.

Then:

1. Reopen Docker Desktop.
2. Wait for the Docker engine to start.
3. Reopen Ubuntu or reconnect VS Code to WSL.
4. Return to the project directory.

```bash
cd ~/projects/airflow-kafka-aircraft-pipeline
```

5. Verify Docker again.

```bash
docker version
```

## 6. Reconnect VS Code to WSL

If VS Code was open when WSL shut down, its integrated terminal may no longer be valid.

Close the stale terminal.

Reconnect VS Code to the Ubuntu WSL environment.

Open a new terminal and verify:

```bash
pwd
```

Expected project path:

```text
/home/<user>/projects/airflow-kafka-aircraft-pipeline
```

Then run:

```bash
docker version
```

## 7. Validate Docker Compose

Confirm the Compose plugin responds:

```bash
docker compose version
```

Validate the project configuration:

```bash
docker compose config --services
```

Expected services:

```text
postgres
airflow-init
airflow-scheduler
airflow-webserver
kafka
prometheus
```

There should be no ZooKeeper service in the current KRaft architecture.

## 8. Recover After a Docker Desktop Update

Docker Desktop updates can change Docker Engine, Compose, WSL integration, networking, and startup behavior.

After an update:

```bash
docker version
```

```bash
docker compose version
```

```bash
docker compose config --services
```

If Docker Desktop closed during the update, reopen it manually and wait for the engine to start.

A previous project issue involved Docker Compose failing during configuration parsing. Updating Docker Desktop changed Compose from version `5.3.0` to `5.3.1` and resolved that failure.

Treat component versions as part of the local runtime state.

## 9. Inspect Existing Containers

After Docker and Compose recover, inspect the project containers before starting anything:

```bash
docker compose ps -a
```

A safely stopped stack may show:

```text
airflow-init        Exited (0)
airflow-scheduler   Exited (0)
airflow-webserver   Exited (0)
postgres            Exited (0)
prometheus          Exited (0)
kafka               Exited (143)
```

Kafka exit code `143` normally indicates a graceful `SIGTERM` stop.

## 10. Restart the Project

When the existing containers still match the current Compose file:

```bash
docker compose start
```

When the Compose configuration or images changed, or containers are missing:

```bash
docker compose up -d
```

Wait approximately 20 to 30 seconds, then check:

```bash
docker compose ps -a
```

Expected steady state:

```text
airflow-init        Exited (0)
airflow-scheduler   Up
airflow-webserver   Up
kafka               Up
postgres            Up
prometheus          Up
```

## 11. Validate the Recovered Stack

### Prometheus

```bash
curl -s http://localhost:9090/-/ready
```

Expected:

```text
Prometheus Server is Ready.
```

### Airflow

```bash
curl -s http://localhost:8080/health
```

Expected relevant fields:

```text
metadatabase: healthy
scheduler: healthy
```

### Kafka KRaft

```bash
docker compose exec kafka env KAFKA_OPTS=   kafka-metadata-quorum   --bootstrap-server localhost:9092   describe --status
```

Expected:

```text
ClusterId:        hC_o8C4lQDmc3XzPtBiX2w
LeaderId:         1
CurrentVoters:    [1]
MaxFollowerLag:   0
```

### Kafka topic persistence

```bash
docker compose exec kafka env KAFKA_OPTS=   kafka-topics   --bootstrap-server localhost:9092   --list
```

Expected topics include:

```text
__consumer_offsets
aircraft_maintenance_events
```

## 12. Troubleshooting by Symptom

### `docker: command not found`

First verify that the terminal is running inside the intended Ubuntu WSL distribution.

Then check Docker Desktop WSL integration.

Restart WSL with:

```powershell
wsl --shutdown
```

Reopen Docker Desktop and reconnect the terminal.

### Docker path reports `Input/output error`

Example:

```text
/mnt/c/Program Files/Docker/Docker/resources/bin/docker: Input/output error
```

This usually indicates a broken WSL-to-Docker Desktop connection rather than a project-code failure.

Use this sequence:

1. Close WSL terminals.
2. Run `wsl --shutdown` in Windows PowerShell.
3. Reopen Docker Desktop.
4. Wait for the engine.
5. Reopen WSL.
6. Run `docker version`.

### Docker client works but server is unavailable

Symptom:

```text
Client:
```

appears, but there is no working `Server:` section.

Docker Desktop is not ready or WSL integration is unavailable.

Check Docker Desktop status and WSL Integration settings.

### `docker compose config` crashes or fails unexpectedly

Check the Compose version:

```bash
docker compose version
```

Restart Docker Desktop.

If the failure started after a Desktop or Compose change, review whether a newer Docker Desktop patch is available.

After updating, verify:

```bash
docker compose config --services
```

Do not edit the project configuration merely to work around a confirmed Compose runtime defect unless the configuration itself is invalid.

### Containers do not restart

Inspect all states:

```bash
docker compose ps -a
```

Inspect the failing service:

```bash
docker compose logs --tail=150 <service>
```

Examples:

```bash
docker compose logs --tail=150 kafka
```

```bash
docker compose logs --tail=150 airflow-scheduler
```

Restart only the affected service when appropriate:

```bash
docker compose restart <service>
```

## 13. Recovery Checklist

Recovery is complete when:

- Docker Desktop is open.
- Docker Desktop reports the engine is running.
- WSL integration is enabled for Ubuntu.
- VS Code or the terminal is connected to the correct WSL distribution.
- `docker version` shows both client and server.
- `docker compose version` succeeds.
- `docker compose config --services` lists the expected KRaft stack.
- The project containers start.
- Prometheus is ready.
- Airflow metadata and scheduler health are healthy.
- Kafka returns the expected KRaft cluster ID.
- `aircraft_maintenance_events` still exists.

## 14. Safe Shutdown After Recovery

At the end of the work session:

```bash
docker compose stop
```

Do not use:

```bash
docker compose down -v
```

## 15. Related Runbooks

```text
docs/runbooks/safe_shutdown_restart.md
docs/runbooks/kafka_kraft_migration_runbook.md
docs/runbooks/kafka_jmx_exporter.md
docs/runbooks/prometheus_kafka_validation_runbook.md
docs/runbooks/airflow_pipeline_run.md
```
