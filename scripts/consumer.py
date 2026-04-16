from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="tourism_db",
    user="postgres",
    password="postgres",   # 🔥 replace this
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

consumer = KafkaConsumer(
    'tourism_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    data = message.value
    print("Received:", data)

    try:
        cursor.execute("""
            INSERT INTO fact_bookings_stream (booking_id, destination_id, price)
            VALUES (%s, %s, %s)
        """, (
            data["booking_id"],
            data["destination_id"],
            data["price"]
        ))

        conn.commit()

    except Exception as e:
        print("DB Error:", e)
