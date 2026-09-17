from pyspark.sql import SparkSession

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Resolve paths relative to the repo root so the script works regardless of CWD.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
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


demand_by_air_quality = spark.sql(
    """
    SELECT
        CASE
            WHEN pickup_air_quality_pm25 IS NULL THEN 'Unknown'
            WHEN pickup_air_quality_pm25 <= 9.0 THEN 'Good'
            WHEN pickup_air_quality_pm25 <= 35.4 THEN 'Moderate'
            WHEN pickup_air_quality_pm25 <= 55.4 THEN 'Unhealthy for Sensitive'
            WHEN pickup_air_quality_pm25 <= 125.4 THEN 'Unhealthy'
            WHEN pickup_air_quality_pm25 <= 225.4 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END AS air_quality_category,
        COUNT(*) AS num_trips,
        ROUND(AVG(pickup_air_quality_pm25), 2) AS avg_pm25
    FROM
        integrated_taxi_trips
    GROUP BY
        air_quality_category
    ORDER BY
        avg_pm25     
"""
)

demand_by_air_quality.show(truncate=False)