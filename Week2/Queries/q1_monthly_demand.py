from pyspark.sql import SparkSession

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql.functions import (
    col, date_format, avg, broadcast, when, min, max, count, desc,
    hour, dayofweek, stddev, lit, round as spark_round
)

# Resolve paths relative to the repo root so the script works regardless of CWD.
# This file lives at Week2/Queries/, so the repo root is two levels up.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir))
WEEK1_DELTA = os.path.join(REPO_ROOT, "Week1", "delta")
INTEGRATED_TAXI_TRIPS_PATH = os.path.join(WEEK1_DELTA, "integrated_taxi_trips")

spark = (
    SparkSession.builder
    .appName("test")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

taxiTrips = spark.read.format("delta").load(INTEGRATED_TAXI_TRIPS_PATH)


taxiTrips.createOrReplaceTempView("integrated_taxi_trips")


# SPARK SQL FORMAT: 
something = spark.sql(
    """
    select 
        date_format(pickup_date, 'yyyy-MM') as pickup_month,
        pickup_zone,
        count(*) as num_trips
    from 
        integrated_taxi_trips
    group by pickup_month, pickup_zone
    """)

something.show()


# # 1.  Monthly taxi demand for each taxi zone.
# By month and by taxi zone, show number of trips (or number of customers?) - would say number of trips instead... because 100 cabs being called with 1 person would be more demand than 10 cabs being called with 4 people
# "Monthly taxi demand", so show for each yyyy-mm OR just group by january, febuary,...
# Show how much demand each taxi zone gets monthly
# Count trips grouped by taxi zone
# Also, should dropoff_zone be considered as well? 



# taxiTrips.withColumn("pickup_month", date_format(col("pickup_date"), "yyyy-MM"))\
#     .groupBy("pickup_month", "pickup_zone")\
#     .agg(count("*").alias("num_trips"))\
#     .orderBy(desc("num_trips")).show(truncate=False)


spark.stop()