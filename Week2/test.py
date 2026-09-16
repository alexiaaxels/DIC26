from pyspark.sql import SparkSession

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql.functions import col, date_format, avg, broadcast, when, min, max, count, desc

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


# Implement all analytical queries using Spark SQL.
# The queries should execute directly on the integrated dataset and the underlying Delta tables produced in Week 1.

# ANALYTICAL QUERIES:
# # Monthly taxi demand for each taxi zone.
# By month and by taxi zone, show number of trips (or number of customers?) - would say number of trips instead... because 100 cabs being called with 1 person would be more demand than 10 cabs being called with 4 people
# "Monthly taxi demand", so show for each yyyy-mm OR just group by january, febuary,...
# Show how much demand each taxi zone gets monthly
# Count trips grouped by taxi zone
# Also, should dropoff_zone be considered as well? 


# SCHEMA:
#  |-- state_code: string (nullable = true)
#  |-- county_code: string (nullable = true)
#  |-- time_utc: timestamp_ntz (nullable = true)
#  |-- vendor_id: integer (nullable = true)
#  |-- pickup_time_local: timestamp_ntz (nullable = true)
#  |-- dropoff_time_local: timestamp_ntz (nullable = true)
#  |-- passenger_count: long (nullable = true)
#  |-- trip_distance: double (nullable = true)
#  |-- ratecode_id: long (nullable = true)
#  |-- store_and_fwd_flag: string (nullable = true)
#  |-- pu_location_id: integer (nullable = true)
#  |-- do_location_id: integer (nullable = true)
#  |-- payment_type: long (nullable = true)
#  |-- fare_amount: double (nullable = true)
#  |-- extra: double (nullable = true)
#  |-- mta_tax: double (nullable = true)
#  |-- tip_amount: double (nullable = true)
#  |-- tolls_amount: double (nullable = true)
#  |-- improvement_surcharge: double (nullable = true)
#  |-- total_amount: double (nullable = true)
#  |-- congestion_surcharge: double (nullable = true)
#  |-- airport_fee: double (nullable = true)
#  |-- pickup_time_utc: timestamp (nullable = true)
#  |-- dropoff_time_utc: timestamp (nullable = true)
#  |-- pickup_zone: string (nullable = true)
#  |-- pickup_borough: string (nullable = true)
#  |-- location_id: integer (nullable = true)
#  |-- borough: string (nullable = true)
#  |-- zone: string (nullable = true)
#  |-- service_zone: string (nullable = true)
#  |-- dropoff_zone: string (nullable = true)
#  |-- dropoff_borough: string (nullable = true)
#  |-- pickup_weather_temperature: double (nullable = true)
#  |-- pickup_weather_relative_humidity: integer (nullable = true)
#  |-- pickup_weather_prcp: double (nullable = true)
#  |-- pickup_weather_snow_depth: string (nullable = true)
#  |-- pickup_weather_wind_speed: double (nullable = true)
#  |-- pickup_weather_wind_direaction: integer (nullable = true)
#  |-- pickup_weather_wind_peak_gust: string (nullable = true)
#  |-- pickup_weather_air_pressure: double (nullable = true)
#  |-- pickup_cloud_cover: integer (nullable = true)
#  |-- pickup_weather_condition_code: integer (nullable = true)
#  |-- pickup_air_quality_pm25: double (nullable = true)
#  |-- pickup_date: string (nullable = true)


# taxiTrips.withColumn("pickup_month", date_format(col("pickup_date"), "yyyy-MM"))\
#     .groupBy("pickup_month", "pickup_zone")\
#     .agg(count("*").alias("num_trips"))\
#     .orderBy(desc("num_trips")).show(20, truncate=False)

cnt = taxiTrips.count()

print(cnt)


# # Average trip distance under different weather conditions.




# # Relationship between air quality and taxi demand.
# # Taxi zones with the largest variation in demand under different weather conditions.
# # Peak travel hours for each day of the week.
# # Monthly trends in taxi demand.


spark.stop()
