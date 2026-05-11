from kafka import KafkaProducer
import os
import json
import time
import random

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic_name = os.getenv("KAFKA_TOPIC", "tourism_events")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "booking_id": f"BK{random.randint(100,999)}",
        "destination_id": random.choice(["D01", "D02", "D03"]),
        "price": random.randint(1000, 10000)
    }

    producer.send(topic_name, value=data)
    print("Sent:", data)

    time.sleep(2)
