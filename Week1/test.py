from pyspark.sql import SparkSession
import re

spark = (
    SparkSession.builder
    .appName("Week1DataExploration")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

taxiTrips = spark.read.parquet("Data/yellow_tripdata_2024-01.parquet", "Data/yellow_tripdata_2024-02.parquet", "Data/yellow_tripdata_2024-03.parquet")


### PRINT SCHEMA:
# x.printSchema()
taxiZones.printSchema()


### SHOW FIRST N ENTRIES:
# x.show(N, truncate=False)
taxiZones.show(10, truncate=False)


### SELECT:
taxiZones.select("service_zone").distinct().show(truncate=False)


# STANDARDIZE COLUMN NAMES

def to_snake_case(name: str) -> str:
 # insert _ between lowercase/digit and uppercase letters
 name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
 # replace spaces with a single underscord
 name = re.sub(r'\s+', '_', name)
 return name.lower()

def standardize_col_headers(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, to_snake_case(column))
    return df

taxiZones = standardize_col_headers(taxiZones)
taxiZones.printSchema()

# NORMALIZE TIMESTAMPS AND OTHER COMMON DATA TYPES

weather = weather
# dt.normalize()


# spark.sql("create database id2221")
# spark.sql("use id2221")

# taxiZones.write.mode("overwrite").format("delta").save("id2221/df_delta")

# df_delta = spark.read.format("delta").load("id2221/df_delta")

# df_delta.show(10)

spark.stop()