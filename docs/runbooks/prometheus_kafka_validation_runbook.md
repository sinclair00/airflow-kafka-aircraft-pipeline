# Prometheus Kafka KRaft Validation Runbook

**Last updated:** 2026-07-31

## Purpose

This runbook validates that the aircraft maintenance pipeline can produce messages through the Kafka KRaft broker and that Prometheus can observe those messages through the Kafka JMX Exporter.

Use this runbook when you need to prove the following path is working:

```text
Airflow DAG
  → Kafka KRaft broker
  → Kafka topic: aircraft_maintenance_events
  → Kafka JMX Exporter
  → Prometheus
  → README screenshot evidence
```

## Expected Evidence

By the end of this runbook, you should have evidence that:

- The Docker Compose stack is running without ZooKeeper.
- Kafka is operating in KRaft mode.
- Prometheus can scrape both itself and Kafka.
- The `aircraft_maintenance_pipeline` DAG completes successfully.
- The Kafka topic `aircraft_maintenance_events` exists.
- Kafka has received messages on that topic.
- Prometheus can query Kafka message activity.
- Screenshots are saved for portfolio documentation.

## Prerequisites

Docker Desktop must be running and connected to WSL.

From WSL, confirm Docker is ready:

```bash
docker version
```

The output must include both sections:

```text
Client:
Server:
```

Start from the project root:

```bash
cd ~/projects/airflow-kafka-aircraft-pipeline
```

## 1. Start or Reconcile the Stack

Use Docker Compose to create, start, or reconcile the current KRaft-based services:

```bash
docker compose up -d
```

This is preferred over `docker compose start` after configuration changes because it applies the current Compose definition while preserving named volumes.

## 2. Verify Services Are Running

Run:

```bash
docker compose ps
```

Expected running services:

```text
postgres
kafka
prometheus
airflow-webserver
airflow-scheduler
```

Notes:

- There should be no ZooKeeper service in the current stack.
- `airflow-init` may be exited. That is normal.
- Prometheus should expose the UI on `localhost:9090`.
- Airflow should expose the UI on `localhost:8080`.


## 3. Verify Kafka Is Running in KRaft Mode

Because the Kafka container has `KAFKA_OPTS` configured for the JMX Exporter Java agent, clear `KAFKA_OPTS` when running Kafka command-line tools.

Run:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  describe --status
```

Expected output includes KRaft quorum information such as:

```text
ClusterId:
LeaderId:
CurrentVoters:
```

For this single-node project, the leader should be broker/controller node `1`.

You can also confirm that Kafka started successfully:

```bash
docker compose logs --tail=100 kafka
```

Look for:

```text
Kafka Server started
```

Do not continue until Kafka is running and the metadata quorum responds.

## 4. Verify Prometheus Targets

Open:

```text
http://localhost:9090/targets
```

Expected target state:

```text
prometheus  UP
kafka       UP
```

Save a screenshot as:

```text
docs/images/prometheus/prometheus-targets-up.png
```

## 5. Verify the Airflow DAG Exists

Run:

```bash
docker compose exec airflow-scheduler \
  airflow dags list
```

Look for:

```text
aircraft_maintenance_pipeline
```

If the DAG is paused, unpause it:

```bash
docker compose exec airflow-scheduler \
  airflow dags unpause aircraft_maintenance_pipeline
```

Expected result:

```text
aircraft_maintenance_pipeline | False
```

## 6. Trigger the Airflow DAG

Run:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger aircraft_maintenance_pipeline
```

Expected immediate state:

```text
queued
```

## 7. Confirm the DAG Succeeded

Wait 30–60 seconds, then run:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs \
  --dag-id aircraft_maintenance_pipeline
```

Look for the newest manual run.

Expected state:

```text
success
```

Save the Airflow UI screenshot as:

```text
docs/images/prometheus/airflow-dag-success.png
```

Preferred screenshot: use the Airflow web UI instead of a terminal screenshot because it is clearer for portfolio review.

## 8. Verify the Kafka Topic Exists

Because the Kafka container has `KAFKA_OPTS` configured for the JMX Exporter Java agent, clear `KAFKA_OPTS` when running Kafka command-line tools. Otherwise the CLI tool may try to start another JMX Exporter instance and fail with a port conflict on `7071`.

Run:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics --bootstrap-server localhost:9092 --list
```

Expected topic:

```text
aircraft_maintenance_events
```

You may also see:

```text
__consumer_offsets
```

## 9. Verify Kafka Topic Offsets

Run:

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

Example:

```text
aircraft_maintenance_events:0:200
```

The final number is the current end offset. A value greater than `0` confirms that Kafka has received messages on the topic.

## 10. Verify the JMX Exporter Topic Metric Directly

Run:

```bash
docker compose exec kafka sh -c \
  "curl -s http://localhost:7071/metrics | grep 'kafka_server_brokertopicmetrics_messagesin_total.*aircraft_maintenance_events'"
```

Expected format:

```text
kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"} 100.0
```

A value greater than `0` confirms that the Kafka JMX Exporter exposes the project topic message counter.

## 11. Query Prometheus for the Topic Counter

Open Prometheus:

```text
http://localhost:9090
```

Run this query:

```promql
kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}
```

Expected result after a successful 100-event validation run:

```text
100
```

A later run may produce a larger cumulative value. Any value greater than `0` proves Prometheus can see the Kafka topic counter.

Recommended screenshot filename:

```text
docs/images/prometheus/prometheus-query-aircraft-topic-total.png
```

## 12. Query Prometheus for 10-Minute Message Activity

Run this query:

```promql
increase(kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}[10m])
```

Expected result:

```text
greater than 0
```

This proves Prometheus can calculate recent Kafka message activity for the project topic.

Recommended screenshot filename:

```text
docs/images/prometheus/prometheus-query-aircraft-topic-increase.png
```

## 13. Optional Broader Context Query

For README evidence, this broader query can be useful because it shows aggregate Kafka activity, internal Kafka activity, and project topic activity together:

```promql
increase(kafka_server_brokertopicmetrics_messagesin_total[10m])
```

The number of returned series can vary because Kafka may expose internal-topic activity in addition to the project topic.

The result must include a row for:

```text
topic="aircraft_maintenance_events"
```

That project-topic series should show a value greater than `0` when the Airflow run occurred inside the selected time window.

Recommended screenshot filename:

```text
docs/images/prometheus/prometheus-query-message-increase.png
```

This is often the best portfolio screenshot because it shows context and confirms the project topic is active.

## 14. Why the 10-Minute Increase Can Vary

The `increase()` value may change slightly each time the query runs. That is normal.

Prometheus calculates `increase()` from scraped samples inside a moving time window. As new 15-second scrapes enter the window and old samples leave the window, the value can shift slightly.

For screenshots, the important requirement is:

```text
aircraft_maintenance_events > 0
```

## 15. Troubleshooting

### Prometheus query returns 0

First, widen the window:

```promql
increase(kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}[30m])
```

Then check the current counter:

```promql
kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}
```

If the current counter is greater than `0`, Prometheus sees the topic but no new messages were observed inside the selected time window.

Trigger the Airflow DAG again and rerun the 10-minute query after the DAG succeeds.

### Kafka CLI command fails with JMX Exporter port conflict

Symptom:

```text
java.net.BindException: Address already in use
Prometheus JMX Exporter exiting
```

Cause:

The Kafka CLI inherited `KAFKA_OPTS` and tried to start a second JMX Exporter on port `7071`.

Fix:

Clear `KAFKA_OPTS` for Kafka CLI commands:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics --bootstrap-server localhost:9092 --list
```

### Kafka target is DOWN in Prometheus

Check the Kafka container:

```bash
docker compose ps kafka
```

Check Kafka logs:

```bash
docker compose logs --tail=150 kafka
```

Verify the KRaft metadata quorum:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  describe --status
```

If Kafka is not healthy, restart only the Kafka service:

```bash
docker compose restart kafka
```

Then recheck the container and Prometheus target:

```bash
docker compose ps kafka
```

```text
http://localhost:9090/targets
```

Do not restart or troubleshoot ZooKeeper. The current architecture uses KRaft and has no ZooKeeper service.

### Prometheus is not reachable

Check the Prometheus container:

```bash
docker compose ps prometheus
```

Check readiness:

```bash
curl -v http://localhost:9090/-/ready
```

Expected:

```text
Prometheus Server is Ready.
```

## 16. README Section Template

Use this section in `monitoring/README.md` or the root `README.md`, adjusting image paths as needed.

```markdown
### Kafka KRaft and Airflow Validation

The Kafka broker runs in KRaft mode without ZooKeeper. This confirms that the `aircraft_maintenance_pipeline` DAG completed successfully before validating Kafka message activity in Prometheus.

![Airflow DAG run success](../docs/images/prometheus/airflow-dag-success.png)

### Kafka Message Activity Over 10 Minutes

This confirms that Prometheus can query Kafka message activity from the JMX Exporter target using a 10-minute increase window after the Airflow pipeline runs.

![Prometheus query for Kafka message increase](../docs/images/prometheus/prometheus-query-message-increase.png)
```

Path note:

- From `monitoring/README.md`, use paths such as `../docs/images/prometheus/<file>.png`.
- From root `README.md`, use paths such as `docs/images/prometheus/<file>.png`.

## 17. Commit the Evidence

Check Git status:

```bash
git status --short
```

Stage all README and screenshot changes:

```bash
git add -A
```

Review the staged summary:

```bash
git diff --cached --stat
```

Commit:

```bash
git commit -m "document Prometheus Kafka message activity"
```

Push:

```bash
git push origin main
```

## 18. Safe Shutdown

Use `stop`, not `down`, when taking a break:

```bash
docker compose stop
```

Avoid:

```bash
docker compose down -v
```

Reason:

- `docker compose stop` preserves containers and named-volume data.
- `docker compose down` removes containers and the Compose network but normally preserves named volumes.
- `docker compose down -v` removes named volumes, including Kafka KRaft data and Prometheus history.
