import json
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logger.info("Producer processing started")

    try:
        components = ["hydraulics", "engine", "avionics", "landing_gear", "fuel_system"]
        event_types = ["inspection", "alert", "repair", "replacement"]
        severities = ["low", "medium", "high"]
        statuses = ["open", "in_progress", "closed"]
        locations = ["San Diego", "Seattle", "Everett", "Mesa"]

        for _ in range(100):
            event = {
                "event_id": str(uuid.uuid4()),
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "aircraft_id": f"AC-{random.randint(1000, 9999)}",
                "tail_number": f"N{random.randint(100, 999)}BA",
                "component": random.choice(components),
                "event_type": random.choice(event_types),
                "severity": random.choice(severities),
                "status": random.choice(statuses),
                "location": random.choice(locations),
                "technician_id": f"TECH-{random.randint(100, 999)}",
                "hours_since_last_service": random.randint(1, 500),
                "notes": "simulated maintenance event",
            }

        producer.send("aircraft_maintenance_events", event)
        producer.flush()
        logger.info("Produced 100 events")
        pass

    finally:
        producer.close()


if __name__ == "__main__":
    main()
