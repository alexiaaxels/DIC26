import os
import sys
from datetime import datetime, timezone

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
WEEK1_DELTA = os.path.join(REPO_ROOT, "Week1", "delta")
WEEK2_DELTA = os.path.join(REPO_ROOT, "Week2", "delta")

INTEGRATED_TAXI_TRIPS_PATH = os.path.join(WEEK1_DELTA, "integrated_taxi_trips")
TAXI_TRIPS_PATH = os.path.join(WEEK1_DELTA, "taxi_trips")
WEATHER_PATH = os.path.join(WEEK1_DELTA, "weather")
AIR_QUALITY_PATH = os.path.join(WEEK1_DELTA, "air_quality")
TAXI_ZONES_PATH = os.path.join(WEEK1_DELTA, "taxi_zone")

def register_integrated(spark: SparkSession, view_name: str = "integrated_taxi_trips"):
    df = spark.read.format("delta").load(INTEGRATED_TAXI_TRIPS_PATH)
    df.cache()
    df.createOrReplaceTempView(view_name)
    return df

def register_taxi_trips(spark: SparkSession, view_name: str = "taxi_trips"):
    df = spark.read.format("delta").load(TAXI_TRIPS_PATH)
    df.cache()
    df.createOrReplaceTempView(view_name)
    return df


def register_weather(spark: SparkSession, view_name: str = "weather"):
    df = spark.read.format("delta").load(WEATHER_PATH)
    df.cache()
    df.createOrReplaceTempView(view_name)
    return df


def register_air_quality(spark: SparkSession, view_name: str = "air_quality"):
    df = spark.read.format("delta").load(AIR_QUALITY_PATH)
    df.cache()
    df.createOrReplaceTempView(view_name)
    return df


def register_taxi_zones(spark: SparkSession,view_name: str = "taxi_zones"):
    df = spark.read.format("delta").load(TAXI_ZONES_PATH)
    df.cache()
    df.createOrReplaceTempView(view_name)
    return df

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



TARGET = "fare_amount"


