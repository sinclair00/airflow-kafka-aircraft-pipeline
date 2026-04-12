import json
import os
import boto3
from datetime import datetime
from kafka import KafkaConsumer
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting consumer process")

    s3 = boto3.client("s3")

    S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
    RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/aircraft_events/")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{RAW_PREFIX}events_{timestamp}.jsonl"

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
    lines = []

    try:
        logger.info("Target raw file: s3://%s/%s", S3_BUCKET_NAME, s3_key)

        for message in consumer:
            event = message.value
            lines.append(json.dumps(event))
            count += 1

        if lines:
            payload = "\n".join(lines) + "\n"

            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=payload.encode("utf-8"),
                ContentType="application/json",
            )

            logger.info("Wrote %d events to s3://%s/%s", count, S3_BUCKET_NAME, s3_key)
        else:
            logger.info("No events consumed during this run; skipping S3 write.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
