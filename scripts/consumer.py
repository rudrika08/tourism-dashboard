from kafka import KafkaConsumer
import json
import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tourism_events")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL, sslmode=DB_SSLMODE)


def get_table_columns(cursor, table_name):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    return [row[0] for row in cursor.fetchall()]


conn = get_connection()
cursor = conn.cursor()

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

columns = get_table_columns(cursor, "fact_bookings_stream")
if not columns:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_bookings_stream (
            booking_id TEXT,
            destination_id TEXT,
            price BIGINT,
            total_price_inr BIGINT
        )
        """
    )
    conn.commit()
    columns = get_table_columns(cursor, "fact_bookings_stream")

has_price = "price" in columns
has_total_price = "total_price_inr" in columns

for message in consumer:
    data = message.value
    print("Received:", data)

    try:
        insert_columns = ["booking_id", "destination_id"]
        insert_values = [data["booking_id"], data["destination_id"]]

        if has_price:
            insert_columns.append("price")
            insert_values.append(data["price"])

        if has_total_price:
            insert_columns.append("total_price_inr")
            insert_values.append(data["price"])

        column_sql = ", ".join(insert_columns)
        placeholder_sql = ", ".join(["%s"] * len(insert_values))

        cursor.execute(
            f"INSERT INTO fact_bookings_stream ({column_sql}) VALUES ({placeholder_sql})",
            tuple(insert_values),
        )

        conn.commit()

    except Exception as e:
        print("DB Error:", e)
