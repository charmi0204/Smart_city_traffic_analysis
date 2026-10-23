
from pyspark.sql import SparkSession
import os
import sys

spark = (
    SparkSession.builder
    .appName("SmartCityCreateParquet")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

csv_file = os.path.expanduser(
    "~/smart_city_practical/traffic_data.csv"
)

hdfs_output = (
    "hdfs://localhost:9000/user/charmi04/smart_city/parquet"
)

if not os.path.exists(csv_file):
    print("ERROR: CSV file not found:", csv_file)
    print("Check the filename using: ls -l")
    spark.stop()
    sys.exit(1)

try:
    # Read CSV data
    df = spark.read.option(
        "header", True
    ).option(
        "inferSchema", True
    ).csv("file://" + csv_file)

    print("Input data:")
    df.show(5, truncate=False)

    print("Columns:")
    df.printSchema()

    # Create HDFS output directory
    os.system(
        "hdfs dfs -mkdir -p /user/charmi04/smart_city"
    )

    # Write data in Parquet format to HDFS
    df.write.mode("overwrite").parquet(hdfs_output)

    print("SUCCESS: Parquet data written to HDFS.")
    print(hdfs_output)

except Exception as e:
    print("ERROR:", e)
    raise

finally:
    spark.stop()

