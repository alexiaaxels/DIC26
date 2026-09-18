
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir))
WEEK1_DELTA = os.path.join(REPO_ROOT, "Week1", "delta")

INTEGRATED_TAXI_TRIPS_PATH = os.path.join(WEEK1_DELTA, "integrated_taxi_trips")


RAW_COCO_LABELS_CSV = os.path.join(SCRIPT_DIR, "raw_coco_labels.csv")

COCO_BUCKETS_CSV    = os.path.join(SCRIPT_DIR, "coco_buckets.csv")


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


def register_integrated(spark: SparkSession, view_name: str = "integrated_taxi_trips"):
    df = spark.read.format("delta").load(INTEGRATED_TAXI_TRIPS_PATH)
    df.createOrReplaceTempView(view_name)
    return df


def register_coco_labels(spark: SparkSession):

    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_COCO_LABELS_CSV)
    )
    raw.createOrReplaceTempView("raw_coco_labels")

    buckets = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(COCO_BUCKETS_CSV)
    )
    buckets.createOrReplaceTempView("coco_buckets")

    return raw, buckets


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