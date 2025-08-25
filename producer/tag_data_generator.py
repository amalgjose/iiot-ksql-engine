import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"   # internal container network
TOPIC = "sensor-data"

producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

TAG_COUNT = 20
INTERVAL = 10  # seconds

def generate_data():
    ts = datetime.utcnow().isoformat()
    for tag_id in range(1, TAG_COUNT + 1):
        yield {
            "tag_id": f"tag-{tag_id}",
            "timestamp": ts,
            "value": round(random.uniform(10, 100), 2)
        }

def main():
    while True:
        for record in generate_data():
            producer.produce(
                topic=TOPIC,
                key=record["tag_id"],
                value=json.dumps(record),
                callback=delivery_report
            )
        producer.flush()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
