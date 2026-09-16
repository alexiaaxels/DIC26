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

# TODO: change to Spark SQL format like q1


# TODO: maybe add a dataset/frame/whatever for the mapping of coco to labels

# # 2. Average trip distance under different weather conditions.

taxiTrips.withColumn(
    "weather_condition",
    when(col("pickup_weather_condition_code") == 1,               lit("Clear"))
    .when(col("pickup_weather_condition_code") == 2,               lit("Fair"))
    .when(col("pickup_weather_condition_code") == 3,               lit("Cloudy"))
    .when(col("pickup_weather_condition_code") == 4,               lit("Overcast"))
    .when(col("pickup_weather_condition_code").isin(5, 6),         lit("Fog"))
    .when(col("pickup_weather_condition_code").isin(7, 8, 17, 18), lit("Rain"))
    .when(col("pickup_weather_condition_code") == 9,               lit("Heavy Rain"))
    .when(col("pickup_weather_condition_code").isin(10, 11),       lit("Freezing Rain"))
    .when(col("pickup_weather_condition_code").isin(12, 13, 19, 20), lit("Sleet"))
    .when(col("pickup_weather_condition_code").isin(14, 15, 16, 21, 22), lit("Snow"))
    .when(col("pickup_weather_condition_code").isin(23, 24, 25, 26, 27), lit("Storm"))
    .otherwise(lit("Unknown"))
    ).groupBy("weather_condition").agg(
        spark_round(avg("trip_distance"),3).alias("avg_miles")
    ).show(truncate=False)

spark.stop()
