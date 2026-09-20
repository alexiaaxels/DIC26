
import os
import sys
from datetime import datetime, timezone

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.errors.exceptions.captured import AnalysisException


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
WEEK1_DELTA = os.path.join(REPO_ROOT, "Week1", "delta")
WEEK2_DELTA = os.path.join(REPO_ROOT, "Week2", "delta")

INTEGRATED_TAXI_TRIPS_PATH = os.path.join(WEEK1_DELTA, "integrated_taxi_trips")


RAW_COCO_LABELS_CSV = os.path.join(SCRIPT_DIR, "raw_coco_labels.csv")

COCO_BUCKETS_CSV    = os.path.join(SCRIPT_DIR, "coco_buckets.csv")

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


def register_integrated(spark: SparkSession, view_name: str = "integrated_taxi_trips"):
    df = spark.read.format("delta").load(INTEGRATED_TAXI_TRIPS_PATH)
    df.cache()
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
