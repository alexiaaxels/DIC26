import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import col, when, trim
from datetime import datetime
from pyspark.sql.types import StringType
import time
from data_config import DATASET_CONFIGS 

spark = (
    SparkSession.builder
    .appName("Task 3 Generic Ingestion Framework")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# create a dataframe for the datasets
def load_datasets(config):
    reader = spark.read.option('header', True).option('inferSchema', True)
    if config['format'] == 'csv':
        return reader.csv(config['path'])
    if config['format'] == 'parquet':
        return spark.read.parquet(*config['path'] if isinstance(config['path'], list) else [config['path']])
    raise ValueError(f"Unsupported format: {config['format']}")

# validates that the dataframe has all expected cols
def validate_schema(df, config, name):
    missing = config['expected_cols'] - set(c.lower() for c in df.columns)
    if missing:
        raise ValueError(f"[{name}] missing expected columns: {missing}")

# normalize the timestamps to time_utc with the type timestamp_ntz
def normalize(df, config):
    if config['timestamp_builder']:
        df = config['timestamp_builder'](df)
    return df

# drops duplicated and null primary keys, filters out rows where the time_utc is null, filters out outliers and returns row count  
def data_quality_check(df, config):
    total = df.count()
    df = df.dropDuplicates(config['key_cols'])
    df = df.dropna(subset=config['key_cols'])
    df = df.filter(col('time_utc').isNotNull()) if 'time_utc' in df.columns else df
    for field, (lo, hi) in config['numeric_checks'].items():
        if lo is not None:
            df = df.filter((col(field) >= lo) | col(field).isNull())
        if hi is not None:
            df = df.filter((col(field) <= hi) | col(field).isNull())
    rejected = total - df.count()
    return df, total, rejected

def to_snake_case(name: str) -> str:
 name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
 name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
 name = re.sub(r'\s+', '_', name)
 return name.lower()

# goes through each column header and formats it to snake_case
def standardize_column_names(df):
    for name in df.columns:
        df = df.withColumnRenamed(name, to_snake_case(name))
    return df

def ingest(name, config):
    start = time.time()
    df = load_datasets(config)
    df = standardize_column_names(df)
    validate_schema(df, config, name)
    df = normalize(df, config)
    if config["transform"]:
        df = config["transform"](df)
    df, total, rejected = data_quality_check(df, config, name)
    df.write.format("delta").mode("overwrite").save(f"delta/{name}")
    return {
        'dataset': name,
        'records_processed': df.count(),
        'records_rejected': rejected,
        'records_loaded': total,
        'execution_time_s': round(time.time() - start, 2),
        'schema_version': config['schema_version'],
        'timestamp': datetime.now(datetime.timezone.utc).isoformat(),
    }

metadata_rows = [ingest(name, cfg) for name, cfg in DATASET_CONFIGS.items()]

spark.createDataFrame(metadata_rows).write.format("delta").mode("append").save("delta/_ingestion_metadata")