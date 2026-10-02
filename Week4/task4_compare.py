import os
import time

from pyspark.sql import functions as F
from pyspark.sql.functions import col

from common import REPO_ROOT, get_spark
from task3_ml_pipeline import latest_delta_version, load_data, train_and_evaluate

RAW_DATA_DIR = os.path.join(REPO_ROOT, "Week1", "Data")

def build_from_raw(spark):
    trips = spark.read.parquet(*[os.path.join(RAW_DATA_DIR, f"yellow_tripdata_2024-0{m}.parquet") for m in (1, 2, 3)])
    zones = spark.read.option("header", True).csv(os.path.join(RAW_DATA_DIR, "taxi_zone_lookup.csv"))
    weather = spark.read.option("header", True).option("inferSchema", True).csv(os.path.join(RAW_DATA_DIR, "weather.csv"))
    air_quality = spark.read.option("header", True).csv(os.path.join(RAW_DATA_DIR, "hourly_88101_2024.csv"))

    trips = (
        trips.select(
            col("tpep_pickup_datetime").alias("pickup_time_local"),
            col("tpep_dropoff_datetime").alias("dropoff_time_local"),
            col("PULocationID").alias("pu_location_id"),
            col("DOLocationID").alias("do_location_id"),
            "trip_distance", "passenger_count", "fare_amount",
        )
        .dropDuplicates()
        .dropna(subset=["pickup_time_local", "pu_location_id", "do_location_id", "fare_amount"])
        .filter((col("fare_amount") >= 0) & (col("trip_distance") >= 0))
        .filter(col("dropoff_time_local") > col("pickup_time_local"))
        .filter(~col("pu_location_id").isin(264, 265) & ~col("do_location_id").isin(264, 265))
        .withColumn("time_utc", F.date_trunc("hour", F.to_utc_timestamp("pickup_time_local", "America/New_York")))
    )

    weather = (
        weather.withColumn("time_utc", F.expr("make_timestamp(year, month, day, hour, 0, 0)"))
        .dropDuplicates(["time_utc"])
        .select("time_utc", col("temp").alias("pickup_weather_temperature"))
    )

    air_quality = (
        air_quality.filter((col("State Code") == "36") & col("County Code").isin("005", "047", "061", "081", "085"))
        .withColumn("time_utc", F.to_timestamp(F.concat_ws(" ", col("Date GMT"), col("Time GMT")), "yyyy-MM-dd HH:mm"))
        .withColumn("pm25", col("Sample Measurement").cast("double"))
        .filter(col("pm25") >= 0)
        .groupBy("time_utc")
        .agg(F.avg("pm25").alias("pickup_air_quality_pm25"))
    )

    pickup_zones = zones.select(col("LocationID").cast("int").alias("pu_location_id"), col("Zone").alias("pickup_zone"))
    dropoff_zones = zones.select(col("LocationID").cast("int").alias("do_location_id"), col("Zone").alias("dropoff_zone"))

    return (
        trips.join(F.broadcast(pickup_zones), "pu_location_id", "left")
        .join(F.broadcast(dropoff_zones), "do_location_id", "left")
        .join(weather, "time_utc", "left")
        .join(air_quality, "time_utc", "left")
    )

def build_from_platform(spark):
    return load_data(spark, latest_delta_version(spark))

def run(spark, name, build):
    start = time.perf_counter()
    df = build(spark).cache()
    rows = df.count()
    prep_time = time.perf_counter() - start

    start = time.perf_counter()
    _, metrics = train_and_evaluate(df)
    train_time = time.perf_counter() - start

    print(f"{name}: rows={rows}, preprocessing={prep_time:.1f}s, training={train_time:.1f}s, metrics={metrics}")
    df.unpersist()

if __name__ == "__main__":
    spark = get_spark("task4_compare")
    run(spark, "Approach A - raw files", build_from_raw)
    run(spark, "Approach B - integrated platform", build_from_platform)
    spark.stop()