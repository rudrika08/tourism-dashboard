from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema of Kafka message
schema = StructType() \
    .add("booking_id", StringType()) \
    .add("destination_id", StringType()) \
    .add("price", IntegerType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tourism_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (binary → string → JSON)
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# 🔥 Example processing
processed_df = parsed_df.withColumn(
    "price_category",
    col("price") > 5000
)

# Output to console
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
