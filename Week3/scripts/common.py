
import os
import sys
from pyspark.sql import SparkSession
from datetime import datetime, timezone

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

RANDOM_SEED = 20260923

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WEEK3_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
REPO_ROOT = os.path.abspath(os.path.join(WEEK3_DIR, os.pardir))

WEEK1_DIR = os.path.join(REPO_ROOT, "Week1")
WEEK1_DELTA = os.path.join(WEEK1_DIR, "delta")
WEEK1_DATA = os.path.join(WEEK1_DIR, "Data")

WEEK2_DIR = os.path.join(REPO_ROOT, "Week2")
WEEK2_DELTA = os.path.join(REPO_ROOT, "Week2", "delta")

UPDATES_DIR = os.path.join(WEEK3_DIR, "Data", "updates")
UPDATE_MANIFEST_PATH = os.path.join(UPDATES_DIR, "manifest.json")

TAXI_TRIPS_RAW_PATHS = [
    os.path.join(WEEK1_DATA, "yellow_tripdata_2024-01.parquet"),
    os.path.join(WEEK1_DATA, "yellow_tripdata_2024-02.parquet"),
    os.path.join(WEEK1_DATA, "yellow_tripdata_2024-03.parquet"),
]
WEATHER_RAW_PATH = os.path.join(WEEK1_DATA, "weather.csv")
AIR_QUALITY_RAW_PATH = os.path.join(WEEK1_DATA, "hourly_88101_2024.csv")

TAXI_TRIPS_DELTA = os.path.join(WEEK1_DELTA, "taxi_trips")
WEATHER_DELTA = os.path.join(WEEK1_DELTA, "weather")
AIR_QUALITY_DELTA = os.path.join(WEEK1_DELTA, "air_quality")

TAXI_TRIPS_UPDATE_PATH = os.path.join(UPDATES_DIR, "taxi_trips_update.parquet")
WEATHER_UPDATE_PATH = os.path.join(UPDATES_DIR, "weather_update.csv")
AIR_QUALITY_UPDATE_PATH = os.path.join(UPDATES_DIR, "air_quality_update.csv")

INTEGRATED_TAXI_TRIPS_PATH = os.path.join(WEEK1_DELTA, "integrated_taxi_trips")

METADATA_PATH = os.path.join(WEEK2_DELTA, "data_products_metadata")

def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ensure_updates_dir() -> None:
    os.makedirs(UPDATES_DIR, exist_ok=True)

def register_integrated(spark: SparkSession, view_name: str = "integrated_taxi_trips"):
    df = spark.read.format("delta").load(INTEGRATED_TAXI_TRIPS_PATH)
    df.cache()
    df.createOrReplaceTempView(view_name)
    return df


AIR_QUALITY_CATEGORIES = """
    case
        when pickup_air_quality_pm25 is null then 'Unknown'
        when pickup_air_quality_pm25 <= 9.0 then 'Good'
        when pickup_air_quality_pm25 <= 35.4 then 'Moderate'
        when pickup_air_quality_pm25 <= 55.4 then 'Unhealthy for Sensitive'
        when pickup_air_quality_pm25 <= 125.4 then 'Unhealthy'
        when pickup_air_quality_pm25 <= 225.4 then 'Very Unhealthy'
        else 'Hazardous'
    end
"""

def data_product_metadata(df, source: str, schema_version: str = "1.0"):
    return df \
        .withColumn("data_source", lit(source)) \
    .withColumn("schema_version", lit(schema_version)) \
    .withColumn("generated_at", current_timestamp())

def existing_metadata(spark: SparkSession, table_name: str):
    try: metadata = spark.read.format("delta").load(METADATA_PATH)
    except AnalysisException:
        return None
    row = metadata.filter(metadata.table_name == table_name).select("created_at").collect()
    return row[0]["created_at"] if row else None

    
def data_product_metadata(spark: SparkSession, metadata_rows: list, table_name: str, source: str, schema_version: str = "1.0"):
    now = datetime.now(timezone.utc).isoformat()
    metadata_rows.append({
        "table_name": table_name,
        "data_source": source,
        "schema_version": schema_version,
        "created_at": existing_metadata(spark, table_name) or now,
        "refresh_at": now
    })

def create_metadata(spark: SparkSession, metadata_rows: list):
    spark.createDataFrame(metadata_rows).write.format("delta").mode("overwrite").save(METADATA_PATH)

def create_dt(df, table_name: str):
    return df.write.format("delta") \
        .mode("overwrite") \
        .save(os.path.join(WEEK2_DELTA, table_name))

