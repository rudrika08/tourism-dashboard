from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "booking_id": f"BK{random.randint(100,999)}",
        "destination_id": random.choice(["D01", "D02", "D03"]),
        "price": random.randint(1000, 10000)
    }

    producer.send("tourism_events", value=data)
    print("Sent:", data)

    time.sleep(2)
