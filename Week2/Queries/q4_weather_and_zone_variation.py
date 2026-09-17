from pyspark.sql import SparkSession

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

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

# # 4. Taxi zones with the largest variation in demand under different weather conditions.

zone_weather_variation = spark.sql(
    """
        SELECT
            pickup_zone,
            CASE
                WHEN pickup_weather_condition_code == 1 THEN 'Clear'
                WHEN pickup_weather_condition_code == 2 THEN 'Fair'
                WHEN pickup_weather_condition_code == 3 THEN 'Cloudy'
                WHEN pickup_weather_condition_code == 4 THEN 'Overcast'
                WHEN pickup_weather_condition_code IN (5, 6) THEN 'Fog'
                WHEN pickup_weather_condition_code IN (7,8, 17, 18) THEN 'Rain'
                WHEN pickup_weather_condition_code == 9 THEN 'Heavy Rain'
                WHEN pickup_weather_condition_code IN (10, 11) THEN 'Freezing Rain'
                WHEN pickup_weather_condition_code IN (12, 13, 19, 20) THEN 'Sleet'
                WHEN pickup_weather_condition_code IN (14, 15, 16, 21, 22) THEN 'Snow'
                WHEN pickup_weather_condition_code IN (23, 24, 25, 26, 27) THEN 'Storm'
                ELSE 'Unknown'
            END AS weather_condition,
            COUNT(*) AS num_trips
        FROM 
            integrated_taxi_trips
        GROUP BY
            pickup_zone, weather_condition
"""
)

zone_weather_variation.createOrReplaceTempView("zone_weather_demand")

largest_variation = spark.sql(
    """
        SELECT
            pickup_zone,
            ROUND(AVG(num_trips), 2) AS avg_trips_per_condition,
            ROUND(STDDEV(num_trips), 2) AS stddev_trips,
            ROUND((STDDEV(num_trips) / AVG(num_trips))*100, 3) AS coeff_of_variation
        FROM
            zone_weather_demand
        GROUP BY
            pickup_zone
        ORDER BY
            coeff_of_variation DESC
"""
)

largest_variation.show(truncate=False)

spark.stop()
