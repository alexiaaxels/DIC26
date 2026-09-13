import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, avg, broadcast

spark = (
    SparkSession.builder
    .appName("Task5 Integration Pipeline")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def build_integrated_dataset():
    trips = spark.read.format("delta").load("delta/taxi_trips")
    weather = spark.read.format("delta").load("delta/weather")
    air_quality = spark.read.format("delta").load("delta/air_quality")
    zones = spark.read.format("delta").load("delta/taxi_zone")

    # getting air quality for NYC - maybe check the State/County names instead?
    nyc_aq = air_quality.filter(
        (col("latitude").between(40.5, 40.9)) & 
        (col("longitude").between(-74.25, -73.7))
    ).groupBy("time_utc").agg(
        avg("sample_measurement").alias("pickup_air_quality_pm25")
    )

    # prepare Weather observations for join - need weather in the right location
    weather_hourly = weather.select(
        col("time_utc"),
        col("temp").alias("pickup_weather_temp"),
        col("prcp").alias("pickup_weather_prcp"),
        col("wspd").alias("pickup_weather_wind_speed")
    )

    # join Taxi Trips with Zone Lookups (Pickup & Dropoff)
    trips_with_zones = trips \
        .join(broadcast(zones.alias("pu")), col("pu_location_id") == col("pu.location_id"), "left") \
        .select(
            trips["*"],
            col("pu.zone").alias("pickup_zone"),
            col("pu.borough").alias("pickup_borough")
        ) \
        .join(broadcast(zones.alias("do")), col("do_location_id") == col("do.location_id"), "left") \
        .select(
            "*",
            col("do.zone").alias("dropoff_zone"),
            col("do.borough").alias("dropoff_borough")
        )

    # join with Weather and Air Quality using the rounded 'time_utc' column
    integrated_df = trips_with_zones \
        .join(weather_hourly, "time_utc", "left") \
        .join(nyc_aq, "time_utc", "left") \
        .withColumn("pickup_date", date_format(col("pickup_time_local"), "yyyy-MM-dd"))

    # write to target analytical Delta table partitioned by pickup date
    integrated_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("pickup_date") \
        .save("delta/integrated_taxi_trips")

    print("Integrated Delta table successfully created at 'delta/integrated_taxi_trips'.")

if __name__ == "__main__":
    build_integrated_dataset()