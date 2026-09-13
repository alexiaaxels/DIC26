import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, avg, broadcast, when

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
    # nyc_aq = air_quality.filter(
    #     (col("latitude").between(40.5, 40.9)) & 
    #     (col("longitude").between(-74.25, -73.7))
    # ).groupBy("time_utc").agg(
    #     avg("sample_measurement").alias("pickup_air_quality_pm25")
    # )

    # prepare Weather observations for join - need weather in the right location
    weather_hourly = weather.select(
        col("time_utc"),
        col("temp").alias("pickup_weather_temperature"),
        col("rhum").alias("pickup_weather_relative_humidity"),
        col("prcp").alias("pickup_weather_prcp"),
        col("snwd").alias("pickup_weather_snow_depth"),
        col("wspd").alias("pickup_weather_wind_speed"),
        col("wdir").alias("pickup_weather_wind_direaction"),
        col("wpgt").alias("pickup_weather_wind_peak_gust"),
        col("pres").alias("pickup_weather_air_pressure"),
        col("cldc").alias("pickup_cloud_cover"),
        col("coco").alias("pickup_weather_condition_code"),
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

    trips_with_zones_to_states = (
        trips_with_zones
        .withColumn(
            "state_code", \
            when(col("pickup_borough") == "Bronx", "36") \
            .when(col("pickup_borough") == "Brooklyn", "36") \
            .when(col("pickup_borough") == "Manhattan", "36") \
            .when(col("pickup_borough") == "Queens", "36") \
            .when(col("pickup_borough") == "Staten Island", "36") \
            .when(col("pickup_borough") == "EWR", "34")
        )
        .withColumn(
            "county_code", \
            when(col("pickup_borough") == "Bronx", "005") \
            .when(col("pickup_borough") == "Brooklyn", "047") \
            .when(col("pickup_borough") == "Manhattan", "061") \
            .when(col("pickup_borough") == "Queens", "081") \
            .when(col("pickup_borough") == "Staten Island", "085") \
            .when(col("pickup_borough") == "EWR", "013")
        )
    )

    trips_with_zones = trips_with_zones.drop(
        "location_id",
        "borough",
        "zone",
        "service_zone"
    )

    aq_hourly = (
        air_quality
        .select(
            col("state_code"),
            col("county_code"),
            col("time_utc"),
            col("sample_measurement")
        )
        .groupBy(
            "state_code",
            "county_code",
            "time_utc"
        )
        .agg(
            avg("sample_measurement").alias("pickup_air_quality_pm25")
        )
    )

    # join with Weather and Air Quality using the rounded 'time_utc' column
    integrated_df = trips_with_zones_to_states \
        .join(weather_hourly, "time_utc", "left") \
        .join(aq_hourly,["state_code", "county_code", "time_utc"], "left") \
        .withColumn("pickup_date", date_format(col("pickup_time_local"), "yyyy-MM-dd"))

    # write to target analytical Delta table partitioned by pickup date
    integrated_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("pickup_date") \
        .save("delta/integrated_taxi_trips")
    #    .option("overwriteSchema", "true")  # enable when changes in the schema


    print("Integrated Delta table successfully created at 'delta/integrated_taxi_trips'.")

if __name__ == "__main__":
    build_integrated_dataset()