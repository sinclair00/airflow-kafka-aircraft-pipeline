import json
import os
from datetime import datetime
from kafka import KafkaConsumer

def main():

    output_dir = "/opt/airflow/data/raw"
    os.makedirs(output_dir, exist_ok=True)
    outfile = os.path.join(output_dir, f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")

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
        with open(outfile, "w", encoding="utf-8") as f:
            for msg in consumer:
                f.write(json.dumps(msg.value) + "\n")
                count += 1
                print(f"Wrote {count} events to {outfile}")
           
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
 




