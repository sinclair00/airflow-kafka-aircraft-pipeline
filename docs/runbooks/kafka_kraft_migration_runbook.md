# Kafka ZooKeeper to KRaft Migration Runbook

## Purpose

Document the completed migration of the aircraft maintenance pipeline from ZooKeeper-managed Kafka to Kafka running in KRaft mode.

The migration replaced ZooKeeper with a single Kafka node operating as both broker and controller while preserving Airflow connectivity, JMX metrics, and Prometheus monitoring.

## Migration Status

- Status: Completed
- Migration date: 2026-07-22
- Migration commit: `ea44aa0`
- Kafka image: `aircraft-kafka-jmx:7.5.0`
- JMX Exporter version: `1.6.0`
- KRaft cluster ID: `hC_o8C4lQDmc3XzPtBiX2w`

This implementation starts a fresh KRaft cluster with a dedicated Docker volume. It does not perform an in-place conversion of the former ZooKeeper-managed Kafka metadata.

## Current Architecture

The Docker Compose stack contains:

- Kafka in combined KRaft broker and controller mode
- Kafka JMX Exporter
- Prometheus
- Grafana
- PostgreSQL
- Airflow API server
- Airflow scheduler
- Airflow DAG processor
- Airflow initialization service

ZooKeeper is no longer present in `docker-compose.yml`.

## Kafka Configuration

The Kafka service uses these KRaft settings:

```yaml
KAFKA_NODE_ID: 1
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
CLUSTER_ID: hC_o8C4lQDmc3XzPtBiX2w
````

Listener purposes:

* `kafka:29092`: internal Docker-network client access
* `localhost:9092`: host client access
* `kafka:29093`: internal KRaft controller communication

Controller port `29093` is not published to the host.

Kafka data is stored in:

```text
airflow-kafka-aircraft-pipeline_kafka-kraft-data
```

## Migration Procedure

The completed migration used the following sequence:

1. Confirmed the existing ZooKeeper-based stack was stopped.
2. Identified the Kafka and ZooKeeper storage volumes.
3. Created the Git branch `feature/kafka-kraft-migration`.
4. Created rollback copies of the original storage.
5. Removed ZooKeeper from Docker Compose.
6. Configured Kafka as a combined KRaft broker and controller.
7. Added the KRaft controller listener.
8. Assigned a new KRaft cluster ID.
9. Added the dedicated `kafka-kraft-data` volume.
10. Validated the Docker Compose configuration.
11. Started Kafka independently.
12. Confirmed KRaft startup through Kafka logs.
13. Created the `aircraft_maintenance_events` topic.
14. Confirmed JMX Exporter availability.
15. Started Prometheus and confirmed Kafka scraping.
16. Started Airflow and PostgreSQL.
17. Triggered and validated the Airflow DAG.
18. Confirmed Kafka, JMX, and Prometheus message counts.
19. Committed, pushed, and fast-forwarded the migration into `main`.
20. Removed the obsolete stopped ZooKeeper container after validating backups.

## Validation Commands

### Validate Docker Compose

```bash
docker compose config --quiet
```

A zero exit code confirms the Compose configuration is valid.

### Confirm Kafka Is Running

```bash
docker compose ps kafka
```

### Confirm KRaft Startup

```bash
docker compose logs kafka | grep -E \
  'KafkaRaftServer|broker has been unfenced|Transition from STARTING to STARTED'
```

Expected evidence includes:

```text
[KafkaRaftServer nodeId=1] Kafka Server started
```

### Confirm Active KRaft Environment

```bash
docker compose exec kafka sh -c '
echo "PROCESS_ROLES=$KAFKA_PROCESS_ROLES"
echo "CONTROLLER_QUORUM=$KAFKA_CONTROLLER_QUORUM_VOTERS"
echo "CONTROLLER_LISTENER=$KAFKA_CONTROLLER_LISTENER_NAMES"
echo "ZOOKEEPER_CONNECT=${KAFKA_ZOOKEEPER_CONNECT:-<not set>}"
'
```

Expected characteristics:

* Process roles are `broker,controller`.
* A controller quorum is configured.
* The controller listener is configured.
* ZooKeeper connectivity is not configured.

### List Kafka Topics

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

Expected topic:

```text
aircraft_maintenance_events
```

### Confirm JMX Exporter

```bash
docker compose exec kafka sh -c \
  'curl -fsS http://localhost:7071/metrics |
   grep "^jmx_scrape_error"'
```

Expected result:

```text
jmx_scrape_error 0.0
```

### Confirm Prometheus Can Scrape Kafka

```bash
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=up{job="kafka"}'
```

Expected metric value:

```text
1
```

### Confirm Airflow Recognizes the DAG

```bash
docker compose exec airflow-scheduler \
  airflow dags list | grep aircraft_maintenance_pipeline
```

### Trigger the DAG

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger aircraft_maintenance_pipeline
```

### Check the Kafka Topic Offset

```bash
docker compose exec kafka env KAFKA_OPTS= \
  kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic aircraft_maintenance_events
```

Validated result from the migration test:

```text
aircraft_maintenance_events:0:100
```

### Check the Kafka JMX Message Metric

```bash
docker compose exec kafka sh -c \
  'curl -fsS http://localhost:7071/metrics |
   grep '\''kafka_server_brokertopicmetrics_messagesin_total.*topic="aircraft_maintenance_events"'\'''
```

Validated result:

```text
kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"} 100.0
```

### Check the Prometheus Message Metric

```bash
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode \
  'query=kafka_server_brokertopicmetrics_messagesin_total{topic="aircraft_maintenance_events"}'
```

Validated result:

```text
100
```

## Validation Summary

The completed functional validation chain was:

```text
Airflow DAG
    -> 100 Kafka messages
    -> Kafka topic end offset 100
    -> JMX message counter 100
    -> Prometheus message metric 100
```

The following success criteria were confirmed:

* Kafka started in KRaft mode.
* Kafka did not require ZooKeeper.
* Host Kafka connectivity worked through `localhost:9092`.
* Docker services reached Kafka through `kafka:29092`.
* The Airflow DAG completed successfully.
* The aircraft topic received 100 messages.
* JMX Exporter remained available on port `7071`.
* Prometheus reported the Kafka target as UP.
* Prometheus stored the aircraft topic message metric.

## Rollback Backups

The original ZooKeeper-era storage was copied into the following named Docker volumes:

```text
aircraft-kafka-zookeeper-backup-20260722
aircraft-kafka-secrets-backup-20260722
aircraft-zookeeper-data-backup-20260722
aircraft-zookeeper-log-backup-20260722
aircraft-zookeeper-secrets-backup-20260722
```

Original source volumes:

```text
Kafka data:
518840337e30768bce3069eb508ac79ecd7e9ea83436ed3927734ef4b9c834ca

Kafka secrets:
4ae30d5c3441626c47509ee276c7b9e571b0a5d1ef41f2e6313704262b690466

ZooKeeper data:
e01fb10261efcc54137cafc99c531f3a1b547510590e733eecf66f6757a77548

ZooKeeper transaction log:
76c7338403a455e9ce899fd804a89942d8f5f34b28a98b934167d5359c98e81b

ZooKeeper secrets:
d62562e27d86544cb10a3d092c36c63eef9fe37a88f82d7f70941acf26567bc8
```

The backups were validated using recursive `diff -qr` comparisons. The final result was:

```text
All rollback backup contents match
```

## Rollback Considerations

The KRaft cluster uses a new cluster ID and a new data volume. The ZooKeeper-era Kafka data must not be attached directly to the KRaft broker.

A rollback requires all of the following:

1. Stop the active stack without deleting volumes.
2. Restore the pre-migration Docker Compose configuration from Git.
3. Recreate the ZooKeeper-era Kafka and ZooKeeper services.
4. Restore the matching Kafka and ZooKeeper volumes together.
5. Validate the restored Compose configuration.
6. Start ZooKeeper before Kafka.
7. Confirm the original topic and offsets.
8. Revalidate Airflow and Prometheus connectivity.

Do not use:

```bash
docker compose down -v
```

unless permanent volume deletion is explicitly intended.

Do not delete the named rollback volumes until the KRaft deployment has remained stable and the backups are no longer required.

## Operational Notes

* `docker compose ps` identifies the service as `kafka`; it does not display Kafka's metadata mode.
* KRaft mode is verified through Kafka configuration and startup logs.
* The old ZooKeeper container was removed only after the rollback volumes were validated.
* Removing the container without `-v` did not delete its original anonymous volumes.
* The underscore warning for `aircraft_maintenance_events` is informational because the topic name does not also contain periods.
* Architecture diagrams updated for the KRaft-based design and observability stack.

## Follow-Up Work Completed

1. Captured updated KRaft and Airflow screenshots.
2. Updated architecture diagrams for the KRaft-based design and observability stack.
3. Added an architecture decision record for the KRaft migration.
4. Upgraded the project to Airflow 3.
5. Revalidated all DAG commands after the Airflow upgrade.
6. Added Grafana dashboards.
