# Safe Shutdown and Restart Runbook

**Last updated:** 2026-07-31

## Purpose

This runbook documents how to stop and restart the aircraft maintenance pipeline safely while preserving Kafka KRaft data, PostgreSQL data, Prometheus history, Airflow metadata, and existing container configuration.

Use this runbook when:

- ending a work session;
- restarting after Docker Desktop or Windows has been closed;
- confirming that the stack recovered correctly;
- distinguishing a safe stop from destructive Docker Compose commands.

## Current Services

The current Docker Compose configuration defines:

```text
postgres
airflow-init
airflow-scheduler
airflow-webserver
kafka
prometheus
```

The current architecture uses Kafka KRaft mode. ZooKeeper is not part of the stack.

## Persistent Data

The project uses named volumes including:

```text
kafka-kraft-data
prometheus-data
```

These volumes preserve Kafka KRaft data and Prometheus history across normal container stops and restarts.

PostgreSQL data must also be treated as persistent project state according to the current Compose configuration.

## 1. Check the Current State

From the project root:

```bash
cd ~/projects/airflow-kafka-aircraft-pipeline
```

Check all project containers:

```bash
docker compose ps -a
```

A running stack normally shows:

- `airflow-scheduler` — Up
- `airflow-webserver` — Up
- `kafka` — Up
- `postgres` — Up
- `prometheus` — Up
- `airflow-init` — Exited (0)

`airflow-init` is a one-time initialization service. `Exited (0)` is expected after it completes successfully.

## 2. Safe Shutdown

Stop the existing containers without removing them:

```bash
docker compose stop
```

This is the preferred command when pausing work.

It:

- sends a graceful stop signal to running containers;
- preserves the containers;
- preserves named volumes;
- preserves Kafka KRaft data;
- preserves Prometheus history;
- preserves the current Compose-created container configuration.

Check the stopped state:

```bash
docker compose ps -a
```

Expected results include:

```text
airflow-init        Exited (0)
airflow-scheduler   Exited (0)
airflow-webserver   Exited (0)
postgres            Exited (0)
prometheus          Exited (0)
kafka               Exited (143)
```

Kafka exit code `143` normally means the process received `SIGTERM` during a graceful Docker stop. In this context, it is expected.

## 3. Commands to Avoid

Do not use:

```bash
docker compose down -v
```

The `-v` option removes named volumes. In this project, that can remove:

- Kafka KRaft data;
- Prometheus history;
- other persisted service state.

Also avoid manually deleting project volumes unless performing a deliberate reset with verified backups.

A plain:

```bash
docker compose down
```

removes containers and the Compose network but normally preserves named volumes. It is still unnecessary for a routine work-session shutdown.

For ordinary daily use, prefer:

```bash
docker compose stop
```

## 4. Restart Docker Desktop if Required

After Windows, WSL, or Docker Desktop has been closed, start Docker Desktop first.

Wait until Docker Desktop reports that the engine is running.

From WSL, confirm both the Docker client and server are available:

```bash
docker version
```

The output must include both:

```text
Client:
Server:
```

If only the client appears, or the command reports an engine or WSL integration error, Docker Desktop is not ready.

## 5. Restart Existing Containers

When the containers already exist and the Compose configuration has not changed, restart them with:

```bash
docker compose start
```

This starts the existing containers without recreating them.

Docker may report all six services as started, including `airflow-init`. The initialization container may run briefly and return to `Exited (0)`.

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

## 6. Reconcile the Current Compose Definition

Use this instead of `docker compose start` when:

- `docker-compose.yml` changed;
- an image definition changed;
- a service does not exist;
- a container must be recreated;
- you are unsure whether the existing containers match the current Compose file.

Run:

```bash
docker compose up -d
```

This creates, starts, or reconciles services against the current Compose definition while preserving named volumes.

If a custom image changed, rebuild the affected service first or use an appropriate build command.

Example for Kafka:

```bash
docker compose build kafka
docker compose up -d kafka
```

## 7. Verify Prometheus Readiness

Run:

```bash
curl -s http://localhost:9090/-/ready
```

Expected:

```text
Prometheus Server is Ready.
```

You can also open:

```text
http://localhost:9090/targets
```

Expected targets:

```text
prometheus  UP
kafka       UP
```

## 8. Verify Airflow Health

Run:

```bash
curl -s http://localhost:8080/health
```

The relevant expected fields are:

```json
{
  "metadatabase": {
    "status": "healthy"
  },
  "scheduler": {
    "status": "healthy"
  }
}
```

The current stack does not define separate triggerer or DAG-processor services, so their health fields may be `null`.

You can also open:

```text
http://localhost:8080
```

## 9. Verify the Kafka KRaft Quorum

Because Kafka CLI tools inherit the broker's JMX `KAFKA_OPTS`, clear that variable for CLI commands.

Run:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  describe --status
```

Expected single-node KRaft state includes:

```text
ClusterId:        hC_o8C4lQDmc3XzPtBiX2w
LeaderId:         1
CurrentVoters:    [1]
MaxFollowerLag:   0
```

The cluster ID should remain unchanged across a normal stop and restart.

## 10. Verify Kafka Topic Persistence

Run:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

Expected topics include:

```text
__consumer_offsets
aircraft_maintenance_events
```

The presence of `aircraft_maintenance_events` after restart confirms that Kafka topic metadata persisted in the KRaft data volume.

## 11. Optional Offset Check

To confirm that topic offsets remain available:

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

A nonzero offset confirms that previously produced topic data remains available.

## 12. Restart Validation Checklist

The restart is validated when:

- Docker Desktop is running.
- `docker version` shows both client and server sections.
- `airflow-init` is `Exited (0)`.
- Airflow scheduler and webserver are running.
- PostgreSQL is running.
- Kafka is running.
- Prometheus is running.
- Prometheus readiness succeeds.
- Airflow metadata database is healthy.
- Airflow scheduler is healthy.
- Kafka reports the expected KRaft cluster ID.
- Kafka node `1` is the leader and current voter.
- `aircraft_maintenance_events` still exists.

## 13. Common Problems

### Docker command fails from WSL

Symptoms may include:

```text
Cannot connect to the Docker daemon
Input/output error
command not found
```

First confirm Docker Desktop is open and fully started.

Then run:

```bash
docker version
```

If WSL integration remains unavailable, follow:

```text
docs/runbooks/docker_wsl_recovery.md
```

### A service remains stopped

Check:

```bash
docker compose ps -a
```

Then inspect the specific service logs:

```bash
docker compose logs --tail=150 <service>
```

Example:

```bash
docker compose logs --tail=150 kafka
```

Restart only the affected service when appropriate:

```bash
docker compose restart <service>
```

### Kafka does not recover

Check Kafka logs:

```bash
docker compose logs --tail=150 kafka
```

Verify the KRaft quorum:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  describe --status
```

Do not attempt to restart or troubleshoot ZooKeeper. The current architecture uses KRaft.

### `airflow-init` exits

`Exited (0)` is expected.

Investigate only when it exits with a nonzero code:

```bash
docker compose logs --tail=150 airflow-init
```

## 14. Related Runbooks

```text
docs/runbooks/airflow_pipeline_run.md
docs/runbooks/kafka_jmx_exporter.md
docs/runbooks/prometheus_kafka_validation_runbook.md
docs/runbooks/kafka_kraft_migration_runbook.md
docs/runbooks/docker_wsl_recovery.md
```
