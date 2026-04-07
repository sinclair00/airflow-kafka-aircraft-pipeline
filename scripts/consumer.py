import json
import os
import boto3
from datetime import datetime
from kafka import KafkaConsumer


def main():

    s3 = boto3.client("s3")

    bucket = os.getenv("S3_BUCKET")
    raw_prefix = os.getenv("S3_RAW_PREFIX", "raw")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{raw_prefix}/events_{timestamp}.jsonl"

    consumer = KafkaConsumer(
        "aircraft_maintenance_events",
        bootstrap_servers="kafka:29092",
        auto_offset_reset="earliest",
        group_id="aircraft-maintenance-consumer-group",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    count = 0

    try:
        lines = []

        for message in consumer:
            event = message.value
            lines.append(json.dumps(event))
            count += 1

        if lines:
            payload = "\n".join(lines) + "\n"

            s3.put_object(Bucket=bucket, Key=s3_key, Body=payload.encode("utf-8"))

            print(f"Wrote {count} events to s3://{bucket}/{s3_key}")

        else:
            print("No events consumed; nothing written to S3.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
