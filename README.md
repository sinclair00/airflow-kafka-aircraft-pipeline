````markdown
# Airflow Kafka Aircraft Maintenance Pipeline

End-to-end data pipeline using:

- Apache Airflow
- Apache Kafka
- Docker Compose
- Python

## Pipeline

1. Producer → sends aircraft events to Kafka
2. Consumer → reads events and stores raw data
3. Validator → validates raw events
4. Transformer → builds curated dataset
5. Summary → generates report

## Run

```bash
docker compose up -d
````

Open Airflow UI:
[http://localhost:8080](http://localhost:8080)

## Notes

* Kafka runs with internal/external listeners
* Data stored in `/opt/airflow/data`

````
