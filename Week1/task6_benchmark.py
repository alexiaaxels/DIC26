import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, date_format, unix_timestamp

spark = (
    SparkSession.builder
    .appName("Task6 Benchmarking")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def get_dir_size_and_files(path):
    total_size = 0
    total_files = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            if f.endswith(".parquet"):
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
                total_files += 1
    return round(total_size / (1024 * 1024), 2), total_files

def run_benchmarks():
    trips_df = spark.read.format("delta").load("delta/taxi_trips")

    # storage design strategy A: no partition
    t0 = time.time()
    trips_df.write.format("delta").mode("overwrite").save("delta/bench_unpartitioned")
    ingest_time_a = round(time.time() - t0, 2)
    size_a, files_a = get_dir_size_and_files("delta/bench_unpartitioned")

    # storage design strategy B: partitioned by pickup date (TODO: should we do month instead of day?)
    t0 = time.time()
    trips_df.withColumn("pickup_date", date_format(col("pickup_time_local"), "yyyy-MM-dd")) \
        .write.format("delta").mode("overwrite").partitionBy("pickup_date").save("delta/bench_partitioned")
    ingest_time_b = round(time.time() - t0, 2)
    size_b, files_b = get_dir_size_and_files("delta/bench_partitioned")

    def execute_queries(table_path):
        df = spark.read.format("delta").load(table_path)
        zones = spark.read.format("delta").load("delta/taxi_zone")

        t_start = time.time()
        
        # query 1: number of taxi trips per borough
        q1 = df.join(zones, df["pu_location_id"] == zones["location_id"]) \
               .groupBy("borough").agg(count("*")).collect()
        
        # query 2: average trip duration per day
        q2 = df.withColumn("duration_s", unix_timestamp("dropoff_time_local") - unix_timestamp("pickup_time_local")) \
               .groupBy(date_format(col("pickup_time_local"), "yyyy-MM-dd")).agg(avg("duration_s")).collect()
               
        # query 3: average fare per borough
        q3 = df.join(zones, df["pu_location_id"] == zones["location_id"]) \
               .groupBy("borough").agg(avg("fare_amount")).collect()
        
        return round(time.time() - t_start, 2)

    latency_a = execute_queries("delta/bench_unpartitioned")
    latency_b = execute_queries("delta/bench_partitioned")

    print("\n\nRESULTS FROM BENCHMARK TESTING: \n")
    print(f"- Strategy A (Unpartitioned):")
    print(f"  Ingestion Time: {ingest_time_a}s | Size: {size_a} MB | Files: {files_a} | Query Latency: {latency_a}s")
    print(f"- Strategy B (Date Partitioned):")
    print(f"  Ingestion Time: {ingest_time_b}s | Size: {size_b} MB | Files: {files_b} | Query Latency: {latency_b}s")
    print("\n")

if __name__ == "__main__":
    run_benchmarks()