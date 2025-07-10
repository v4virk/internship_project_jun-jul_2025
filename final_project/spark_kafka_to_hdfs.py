from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, ArrayType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToHDFSConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.2") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Define schema for Kafka messages
schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", DoubleType()) \
    .add("products", ArrayType(
        StructType() \
            .add("product_id", StringType()) \
            .add("product_category", StringType()) \
            .add("price", StringType()) \
            .add("product_type", StringType(), nullable=True) \
            .add("brand", StringType(), nullable=True)
    ), nullable=True)

# Read from Kafka
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test-events") \
        .option("startingOffsets", "earliest") \
        .load()
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    spark.stop()
    exit(1)

# Parse JSON data
try:
    parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
except Exception as e:
    print(f"Error parsing JSON: {e}")
    spark.stop()
    exit(1)

# Write to HDFS
try:
    query = parsed.writeStream \
        .format("parquet") \
        .option("path", "hdfs://localhost:9000/user/harvijaysingh/events/") \
        .option("checkpointLocation", "hdfs://localhost:9000/user/harvijaysingh/checkpoints/") \
        .partitionBy("event_type") \
        .outputMode("append") \
        .start()
    query.awaitTermination()
except Exception as e:
    print(f"Error writing to HDFS: {e}")
    spark.stop()
    exit(1)
finally:
    spark.stop()
    