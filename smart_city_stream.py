from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    when,
    avg,
    sum,
    count
)

# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# DATA SCHEMA
# ============================================================

schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("service", StringType(), True),
    StructField("location", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("speed", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("event_time", StringType(), True)
])

# ============================================================
# READ DATA FROM KAFKA
# ============================================================

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "smart_city") \
    .option("startingOffsets", "latest") \
    .load()

# ============================================================
# CONVERT KAFKA VALUE
# ============================================================

json_stream = raw_stream.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# ============================================================
# CLEAN DATA
# ============================================================

clean_stream = json_stream \
    .filter(col("location").isNotNull()) \
    .filter(col("service").isNotNull()) \
    .filter(col("vehicle_count") >= 0) \
    .filter(col("speed") >= 0) \
    .filter(col("passenger_count") >= 0)

# ============================================================
# CONVERT EVENT TIME
# ============================================================

clean_stream = clean_stream.withColumn(
    "event_timestamp",
    to_timestamp(col("event_time"))
)

# ============================================================
# TRANSFORM DATA
# ============================================================

transformed_stream = clean_stream.withColumn(
    "traffic_status",
    when(col("vehicle_count") >= 70, "HIGH")
    .when(col("vehicle_count") >= 40, "MEDIUM")
    .otherwise("LOW")
)

# ============================================================
# DISPLAY PROCESSED STREAM
# ============================================================

query = transformed_stream.select(
    "event_id",
    "service",
    "location",
    "vehicle_type",
    "vehicle_count",
    "speed",
    "passenger_count",
    "temperature",
    "event_time",
    "traffic_status"
).writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/smart_city_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
