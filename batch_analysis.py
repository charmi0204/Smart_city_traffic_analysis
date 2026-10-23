
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, max, min, desc, upper
)
import subprocess
import sys

# ============================================================
# SMART CITY TRAFFIC ANALYTICS SYSTEM
# BATCH ANALYSIS USING PYSPARK AND HDFS
# ============================================================

# STEP 1: CREATE SPARK SESSION
spark = (
    SparkSession.builder
    .appName("SmartCityTrafficBatchAnalysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# STEP 2: DEFINE HDFS INPUT PATH
input_path = (
    "hdfs://localhost:9000/user/charmi04/smart_city/parquet"
)

print("\n" + "=" * 65)
print("       SMART CITY TRAFFIC ANALYTICS SYSTEM")
print("             BATCH DATA ANALYSIS")
print("=" * 65)

# STEP 3: CHECK WHETHER HDFS INPUT CONTAINS DATA
try:
    result = subprocess.run(
        ["hdfs", "dfs", "-ls",
         "/user/charmi04/smart_city/parquet"],
        capture_output=True,
        text=True,
        timeout=30
    )

    if result.returncode != 0 or "part-" not in result.stdout:
        print("\nERROR: Parquet data files were not found in HDFS.")
        print("Expected folder: /user/charmi04/smart_city/parquet")
        print("\nCheck the folder using:")
        print("hdfs dfs -ls -R /user/charmi04/smart_city")
        print("\nFirst create/write Parquet data to this folder.")
        spark.stop()
        sys.exit(1)

except Exception as error:
    print("\nCould not check the HDFS input folder.")
    print("Details:", error)
    spark.stop()
    sys.exit(1)

# STEP 4: READ PARQUET DATA FROM HDFS
try:
    df = spark.read.parquet(input_path)
except Exception as error:
    print("\nERROR: Unable to read Parquet data.")
    print("Check the HDFS path and the data files.")
    print("Details:", error)
    spark.stop()
    sys.exit(1)

# STEP 5: DISPLAY SCHEMA
print("\nSTEP 5: DATA SCHEMA")
df.printSchema()

# STEP 6: DISPLAY SAMPLE RECORDS
print("\nSTEP 6: SAMPLE TRAFFIC RECORDS")
df.show(10, truncate=False)

# STEP 7: DISPLAY TOTAL EVENTS
print("\nSTEP 7: TOTAL TRAFFIC EVENTS")
total_events = df.count()
print("Total events:", total_events)

# STEP 8: ANALYZE AVERAGE SPEED
if "speed" in df.columns:
    print("\nSTEP 8: AVERAGE VEHICLE SPEED")
    df.select(avg("speed").alias("average_speed")).show()

# STEP 9: ANALYZE TOTAL VEHICLES
if "vehicle_count" in df.columns:
    print("\nSTEP 9: TOTAL VEHICLES")
    df.select(
        sum("vehicle_count").alias("total_vehicles")
    ).show()

# STEP 10: ANALYZE TOTAL PASSENGERS
if "passenger_count" in df.columns:
    print("\nSTEP 10: TOTAL PASSENGERS")
    df.select(
        sum("passenger_count").alias("total_passengers")
    ).show()

# STEP 11: EVENTS BY LOCATION
if "location" in df.columns:
    print("\nSTEP 11: TRAFFIC EVENTS BY LOCATION")
    df.groupBy("location") \
        .agg(count("*").alias("total_events")) \
        .orderBy(desc("total_events")) \
        .show(truncate=False)

# STEP 12: EVENTS BY SERVICE
if "service" in df.columns:
    print("\nSTEP 12: EVENTS BY SERVICE")
    df.groupBy("service") \
        .agg(count("*").alias("total_events")) \
        .orderBy(desc("total_events")) \
        .show(truncate=False)

# STEP 13: TRAFFIC STATUS ANALYSIS
if "traffic_status" in df.columns:
    print("\nSTEP 13: TRAFFIC STATUS ANALYSIS")
    df.groupBy("traffic_status") \
        .agg(count("*").alias("total_events")) \
        .orderBy(desc("total_events")) \
        .show(truncate=False)

    # STEP 14: HIGH / HEAVY TRAFFIC RECORDS
    print("\nSTEP 14: HIGH OR HEAVY TRAFFIC RECORDS")
    high_traffic = df.filter(
        upper(col("traffic_status")).contains("HIGH")
        | upper(col("traffic_status")).contains("HEAVY")
    )
    high_traffic.show(20, truncate=False)

    # STEP 15: HIGH TRAFFIC LOCATIONS
    if "location" in df.columns:
        print("\nSTEP 15: HIGH TRAFFIC LOCATIONS")
        high_traffic.groupBy("location") \
            .agg(count("*").alias("high_traffic_events")) \
            .orderBy(desc("high_traffic_events")) \
            .show(truncate=False)

# STEP 16: TEMPERATURE ANALYSIS
if "temperature" in df.columns:
    print("\nSTEP 16: TEMPERATURE SUMMARY")
    df.select(
        max("temperature").alias("maximum_temperature"),
        min("temperature").alias("minimum_temperature"),
        avg("temperature").alias("average_temperature")
    ).show()

# STEP 17: FINAL SUMMARY
print("\n" + "=" * 65)
print("                 FINAL ANALYSIS SUMMARY")
print("=" * 65)
print("Total traffic events analyzed:", total_events)
print("Input source: HDFS Parquet")
print("Analysis type: Batch Processing")
print("Technology: Apache Spark + PySpark + HDFS")
print("=" * 65)

# STEP 18: STOP SPARK SESSION
spark.stop()
print("\nBatch analysis completed.")
