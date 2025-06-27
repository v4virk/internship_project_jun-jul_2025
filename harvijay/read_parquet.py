from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadParquet") \
    .getOrCreate()

# Read Parquet files from HDFS
df = spark.read.parquet("hdfs://localhost:9000/user/harvijaysingh/events")

# Show the data
df.show()

# Stop the Spark session
spark.stop()