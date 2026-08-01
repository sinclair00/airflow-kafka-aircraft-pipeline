# Kafka JMX Exporter Runbook

**Last updated:** 2026-07-31

## Purpose

This runbook documents how the Kafka JMX Exporter is built, attached to the Kafka KRaft broker, scraped by Prometheus, validated, and troubleshot in the aircraft maintenance pipeline.

Use this runbook when you need to:

- confirm that the Kafka broker exposes JMX metrics;
- verify that Prometheus can scrape those metrics;
- rebuild the custom Kafka image after exporter changes;
- diagnose missing Kafka metrics or exporter startup failures.

## Monitoring Path

```text
Kafka KRaft broker JVM
  → Prometheus JMX Exporter Java agent
  → HTTP metrics endpoint on kafka:7071
  → Prometheus scrape job: kafka
  → PromQL queries and Grafana panels
```

The current architecture uses Kafka KRaft mode. ZooKeeper is not part of this monitoring path.

## Repository Files

The JMX Exporter implementation is defined by these files:

```text
docker-compose.yml
monitoring/jmx-exporter/Dockerfile
monitoring/jmx-exporter/kafka.yml
monitoring/prometheus.yml
```

### `monitoring/jmx-exporter/Dockerfile`

The custom Kafka image:

- starts from `confluentinc/cp-kafka:7.5.0`;
- installs Prometheus JMX Exporter `1.6.0`;
- stores the Java agent at `/opt/jmx-exporter/jmx_prometheus_javaagent.jar`;
- copies the exporter rules file to `/opt/jmx-exporter/kafka.yml`;
- returns execution to the Kafka image user.

### `monitoring/jmx-exporter/kafka.yml`

The rules file converts Kafka JMX MBeans into Prometheus metrics.

The configuration:

```yaml
lowercaseOutputName: true
```

normalizes exported metric names to lowercase.

The rules also convert Kafka per-second counters into Prometheus counters ending in `_total` and preserve useful labels such as:

```text
topic
partition
clientId
```

### `docker-compose.yml`

Kafka loads the JMX Exporter as a Java agent through:

```yaml
KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/kafka.yml"
```

This means:

- the exporter runs inside the Kafka JVM;
- the exporter listens on port `7071`;
- the exporter uses `/opt/jmx-exporter/kafka.yml`;
- Kafka data remains separate in the `kafka-kraft-data` named volume.

### `monitoring/prometheus.yml`

Prometheus scrapes Kafka every 15 seconds through:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: kafka
    static_configs:
      - targets:
          - kafka:7071
```

The hostname `kafka` is the Docker Compose service name. Prometheus reaches the exporter through the Compose network, so port `7071` does not need to be published to the Windows host.

## 1. Start or Reconcile the Stack

From the project root:

```bash
cd ~/projects/airflow-kafka-aircraft-pipeline
```

Start or reconcile the current Compose definition:

```bash
docker compose up -d
```

## 2. Verify Kafka and Prometheus

Run:

```bash
docker compose ps kafka prometheus
```

Both services should be running.

Check Kafka startup:

```bash
docker compose logs --tail=100 kafka
```

Look for:

```text
Kafka Server started
```

## 3. Verify the JMX Exporter Endpoint Directly

Run from inside the Kafka container:

```bash
docker compose exec kafka sh -c \
  "curl -s http://localhost:7071/metrics | head"
```

Expected output contains Prometheus-formatted metrics.

Check exporter health:

```bash
docker compose exec kafka sh -c \
  "curl -s http://localhost:7071/metrics | grep '^jmx_scrape_error'"
```

Expected:

```text
jmx_scrape_error 0.0
```

A value of `0.0` means the exporter completed the JMX scrape without an exporter-level error.

## 4. Verify the Project Topic Metric

Run:

```bash
docker compose exec kafka sh -c \
  "curl -s http://localhost:7071/metrics | grep 'kafka_server_brokertopicmetrics_messagesin_total.*aircraft_maintenance_events'"
```

Expected format:

```text
kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"} 100.0
```

The value may be larger after additional pipeline runs. Any value greater than `0` confirms that the exporter exposes the project topic message counter.

## 5. Verify the Prometheus Target

Open:

```text
http://localhost:9090/targets
```

Expected target:

```text
kafka  UP
```

You can also query:

```promql
up{job="kafka"}
```

Expected result:

```text
1
```

## 6. Query the Kafka Topic Counter in Prometheus

Open:

```text
http://localhost:9090
```

Run:

```promql
kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}
```

A value greater than `0` proves that:

```text
Kafka JMX
  → JMX Exporter
  → Prometheus
```

is working for the project topic.

To measure recent activity, run:

```promql
increase(kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}[10m])
```

Trigger the Airflow DAG before this query when fresh activity is required.

## 7. Rebuild After Exporter Changes

Rebuild the Kafka image when either of these files changes:

```text
monitoring/jmx-exporter/Dockerfile
monitoring/jmx-exporter/kafka.yml
```

Run:

```bash
docker compose build kafka
```

Then recreate the Kafka service:

```bash
docker compose up -d kafka
```

Recheck:

```bash
docker compose ps kafka
```

and:

```bash
docker compose logs --tail=100 kafka
```

Do not use `docker compose down -v`. The `-v` option would remove named volumes, including Kafka KRaft data and Prometheus history.

## 8. Kafka CLI Commands and `KAFKA_OPTS`

Kafka command-line tools launched inside the Kafka container inherit `KAFKA_OPTS`.

Because `KAFKA_OPTS` starts the JMX Exporter on port `7071`, a Kafka CLI command can attempt to start a second exporter and fail with:

```text
java.net.BindException: Address already in use
Prometheus JMX Exporter exiting
```

Clear `KAFKA_OPTS` for Kafka CLI commands.

Example:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics --bootstrap-server localhost:9092 --list
```

This does not change the running Kafka broker. It clears the variable only for that CLI process.

## 9. Troubleshooting

### Prometheus Kafka target is DOWN

Check the services:

```bash
docker compose ps kafka prometheus
```

Check Kafka logs:

```bash
docker compose logs --tail=150 kafka
```

Check Prometheus logs:

```bash
docker compose logs --tail=150 prometheus
```

Verify the exporter endpoint from inside Kafka:

```bash
docker compose exec kafka sh -c \
  "curl -s http://localhost:7071/metrics | head"
```

If the endpoint works inside Kafka but the Prometheus target remains down, verify that `monitoring/prometheus.yml` still targets:

```text
kafka:7071
```

### Exporter endpoint does not respond

Confirm that `docker-compose.yml` contains the Java agent setting:

```text
-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/kafka.yml
```

Confirm that the files exist inside the container:

```bash
docker compose exec kafka ls -l /opt/jmx-exporter
```

Expected files:

```text
jmx_prometheus_javaagent.jar
kafka.yml
```

If either file is missing, rebuild and recreate Kafka:

```bash
docker compose build kafka
docker compose up -d kafka
```

### `jmx_scrape_error` is `1.0`

Check Kafka logs and inspect the exporter rules file:

```bash
docker compose logs --tail=150 kafka
```

```bash
sed -n '1,80p' monitoring/jmx-exporter/kafka.yml
```

Rebuild Kafka after correcting the rules file.

### Topic metric is missing

First confirm the topic exists:

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics --bootstrap-server localhost:9092 --list
```

Then confirm that the Airflow DAG has produced messages.

Query all broker topic message counters:

```bash
docker compose exec kafka sh -c \
  "curl -s http://localhost:7071/metrics | grep 'kafka_server_brokertopicmetrics_messagesin_total'"
```

If Kafka has not received project messages since the broker was recreated, trigger the Airflow DAG and check again.

## 10. Validation Checklist

The JMX Exporter configuration is validated when all of the following are true:

- Kafka is running in KRaft mode.
- The Kafka container contains the exporter JAR and `kafka.yml`.
- `http://localhost:7071/metrics` responds inside the Kafka container.
- `jmx_scrape_error` equals `0.0`.
- The project topic counter is present and greater than `0`.
- Prometheus shows the `kafka` target as `UP`.
- `up{job="kafka"}` returns `1`.
- Prometheus returns the project topic message counter.

## 11. Related Runbooks

```text
docs/runbooks/prometheus_kafka_validation_runbook.md
docs/runbooks/kafka_kraft_migration_runbook.md
docs/runbooks/safe_shutdown_restart.md
docs/runbooks/docker_wsl_recovery.md
```
