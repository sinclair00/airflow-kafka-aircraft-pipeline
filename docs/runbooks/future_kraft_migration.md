# Kafka ZooKeeper to KRaft Migration Plan

## Purpose

Document the planned migration from ZooKeeper-managed Kafka to Kafka running in KRaft mode.

This runbook is currently a planning document. The Docker Compose configuration must not be changed until the existing architecture is documented and committed.

## Current State

The current Docker Compose stack contains:

- ZooKeeper
- One Kafka broker
- Kafka JMX Exporter
- Prometheus
- PostgreSQL
- Airflow scheduler
- Airflow webserver

Current Kafka characteristics:

- Kafka image: aircraft-kafka-jmx:7.5.0
- ZooKeeper image: confluentinc/cp-zookeeper:7.5.0
- Internal Kafka listener: kafka:29092
- Host Kafka listener: localhost:9092
- JMX Exporter port: 7071
- Kafka broker ID: 1

## Migration Objective

Replace ZooKeeper with a single-node KRaft configuration while preserving:

- Airflow producer connectivity
- Docker-network Kafka access
- Host Kafka access
- The aircraft_maintenance_events topic
- Kafka JMX Exporter metrics
- Prometheus Kafka monitoring

## Planned Work

1. Record the current Compose configuration.
2. Identify existing Kafka and ZooKeeper volumes.
3. Confirm whether existing Kafka messages must be preserved.
4. Create a dedicated Git migration branch.
5. Remove the ZooKeeper service.
6. Configure Kafka broker and controller roles.
7. Add the KRaft controller listener.
8. Configure the node ID and controller quorum.
9. Configure a KRaft cluster ID.
10. Preserve the existing client listeners.
11. Preserve the JMX Exporter Java agent.
12. Validate the Compose configuration.
13. Start Kafka and inspect its logs.
14. Validate topics and message production.
15. Validate Prometheus and JMX metrics.
16. Trigger and validate the Airflow DAG.
17. Update documentation and screenshots.
18. Commit and push the completed migration.

## Safety Rules

- Begin with a clean Git working tree.
- Commit this plan before editing Docker Compose.
- Use a dedicated migration branch.
- Do not delete volumes without reviewing them.
- Do not run docker compose down -v unless volume deletion is intentional.
- Preserve the current ZooKeeper configuration in Git for rollback.
- Validate Docker Compose before starting containers.

## Success Criteria

The migration is complete when:

- ZooKeeper is removed from Docker Compose.
- Kafka starts successfully in KRaft mode.
- Kafka is reachable at localhost:9092.
- Docker services reach Kafka at kafka:29092.
- The Airflow DAG completes successfully.
- Kafka receives aircraft maintenance messages.
- The JMX Exporter remains available on port 7071.
- Prometheus reports Kafka as UP.
- Kafka message activity appears in Prometheus.
- Documentation and screenshots are updated.
- All changes are committed and pushed.

## Rollback Strategy

If KRaft validation fails:

1. Stop the stack without deleting volumes.
2. Restore the ZooKeeper-based Docker Compose configuration from Git.
3. Validate the restored Compose configuration.
4. Restart the original stack.
5. Confirm ZooKeeper, Kafka, Airflow, and Prometheus are healthy.

Do not delete either ZooKeeper or KRaft storage during rollback until the contents have been reviewed.

## Follow-Up Work

After KRaft is stable:

1. Upgrade to Airflow 3.
2. Revalidate Airflow commands and screenshots.
3. Add Grafana dashboards.
4. Add architecture decision records.
