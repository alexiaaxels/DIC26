from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Week1DataExploration")
    .master("local[*]")
    .getOrCreate()
)



taxiZones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Data/taxi_zone_lookup.csv")
)

weather = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Data/weather.csv")
)

airQuality = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Data/hourly_88101_2024.csv")
)

taxiTrips = spark.read.parquet(
    "Data/yellow_tripdata_2024-01.parquet",
    "Data/yellow_tripdata_2024-02.parquet",
    "Data/yellow_tripdata_2024-03.parquet"
)


### PRINT SCHEMA:
# x.printSchema()
taxiZones.printSchema()


### SHOW FIRST N ENTRIES:
# x.show(N, truncate=False)
taxiZones.show(10, truncate=False)


### SELECT:
taxiZones.select("service_zone").distinct().show(truncate=False)


spark.stop()