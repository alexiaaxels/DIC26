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
    .csv("taxi_zone_lookup.csv")
)

weather = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("weather.csv")
)

airQuality = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("hourly_88101_2024.csv")
)

taxiTrips = spark.read.parquet("*.parquet")


### PRINT SCHEMA:
# x.printSchema()
taxiZones.printSchema()


### SHOW FIRST N ENTRIES:
# x.show(N, truncate=False)
taxiZones.show(10, truncate=False)


### SELECT:
taxiZones.select("service_zone").distinct().show(truncate=False)


spark.stop()