from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Define schema for events_log.json
schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", DoubleType()) \
    .add("ip", StringType()) \
    .add("device", StringType()) \
    .add("user_type", StringType()) \
    .add("products", ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("product_category", StringType()),
        StructField("price", StringType()),
        StructField("product_type", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True)
    ])))

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadEventsLog") \
    .getOrCreate()

# Read JSON file from HDFS
df = spark.read.schema(schema).json("hdfs://localhost:9000/user/harvijaysingh/events_log/events_log.json")

# Show the data
df.show(truncate=False)

# Stop the Spark session
spark.stop()
