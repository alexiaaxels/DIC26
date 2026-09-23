
import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

RANDOM_SEED = 20260923

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WEEK3_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
REPO_ROOT = os.path.abspath(os.path.join(WEEK3_DIR, os.pardir))

WEEK1_DIR = os.path.join(REPO_ROOT, "Week1")
WEEK1_DELTA = os.path.join(WEEK1_DIR, "delta")
WEEK1_DATA = os.path.join(WEEK1_DIR, "Data")

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
