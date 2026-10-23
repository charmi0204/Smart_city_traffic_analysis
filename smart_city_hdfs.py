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
    when
)

# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("SmartCityHDFS") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# SCHEMA
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
# READ KAFKA
# ============================================================

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "smart_city") \
    .option("startingOffsets", "latest") \
    .load()

# ============================================================
# PARSE JSON
# ============================================================

data = raw_stream.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# ============================================================
# CLEAN DATA
# ============================================================

clean_data = data \
    .filter(col("location").isNotNull()) \
    .filter(col("service").isNotNull()) \
    .filter(col("vehicle_count") >= 0) \
    .filter(col("speed") >= 0) \
    .filter(col("passenger_count") >= 0)

# ============================================================
# TRANSFORM DATA
# ============================================================

processed_data = clean_data \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    ) \
    .withColumn(
        "traffic_status",
        when(col("vehicle_count") >= 70, "HIGH")
        .when(col("vehicle_count") >= 40, "MEDIUM")
        .otherwise("LOW")
    )

# ============================================================
# WRITE TO HDFS AS PARQUET
# ============================================================

query = processed_data.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://localhost:9000/user/charmi04/smart_city/parquet"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/user/charmi04/smart_city/checkpoint"
    ) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
