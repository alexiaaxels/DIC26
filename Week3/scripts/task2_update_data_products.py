import os
import sys
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
WEEK3_DIR = os.path.dirname(SCRIPT_DIR) 
REPORT_PATH = os.path.join( WEEK3_DIR, "Data", "updates", "incremental_run_report.json",)
METADATA_PATH = os.path.join( REPO_ROOT, "Week2", "delta", "data_products_metadata", )

sys.path.insert(0, REPO_ROOT)

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from common import (
    get_spark,
    register_integrated,
    data_product_metadata,
    create_metadata,
)

from Week2.task4_data_products import (
    create_taxi_zone_stats,
    create_air_quality_impact_summary,
    create_borough_mobility_summary,
    create_weekday_mobility_summary,
)


spark = get_spark("task4_data_products_refresh")

register_integrated(spark)

with open(REPORT_PATH, "r", encoding="utf-8") as f: 
    report = json.load(f)  

changed_sources = {
    dataset_name
    for dataset_name, dataset_info in report["datasets"].items()
    if dataset_info["inserted"] > 0
}
latest_source_time = max(
    datetime.fromisoformat(
        dataset_info["executed_at"].replace("Z", "+00:00")
    )
    for dataset_info in report["datasets"].values()
    if dataset_info["inserted"] > 0
)

# Read the existing product metadata
try:
    metadata = (
        spark.read
        .format("delta")
        .load(METADATA_PATH)
    )
except AnalysisException:
    metadata = None


PRODUCT_DEPENDENCIES = {
    "taxi_zone_stats": {"taxi_trips"},
    "air_quality_impact_summary": {"taxi_trips", "air_quality"},
    "borough_mobility_summary": {"taxi_trips"},
    "weekday_mobility_summary": {"taxi_trips"},
}


def needs_refresh(table_name, changed_sources, metadata, latest_source_time):
    dependencies = PRODUCT_DEPENDENCIES[table_name]

    if not dependencies.intersection(changed_sources):
        return False

    if metadata is None:
        return True

    rows = (
        metadata
        .filter(F.col("table_name") == table_name)
        .select("refresh_at")
        .collect()
    )

    if not rows:
        return True

    refresh_time = rows[0]["refresh_at"]

    if refresh_time is None:
        return True

    if isinstance(refresh_time, str):
        refresh_time = datetime.fromisoformat(
            refresh_time.replace("Z", "+00:00")
        )

    return latest_source_time > refresh_time


products = {
    "taxi_zone_stats": create_taxi_zone_stats,
    "air_quality_impact_summary": create_air_quality_impact_summary,
    "borough_mobility_summary": create_borough_mobility_summary,
    "weekday_mobility_summary": create_weekday_mobility_summary,
}


if metadata is not None: 
    metadata_rows = [row.asDict() for row in metadata.collect()] 
else: 
    metadata_rows = []

refreshed_products = []

for table_name, create_function in products.items():

    if needs_refresh(table_name, changed_sources, metadata, latest_source_time):

        print(f"Refreshing {table_name}...")

        metadata_rows = [
            row for row in metadata_rows
            if row["table_name"] != table_name
        ]

        create_function(spark, metadata_rows)

        refreshed_products.append(table_name)
    else:
        print(
            f"Skipping {table_name} - no new source data."
        )


if refreshed_products:
    create_metadata(spark, metadata_rows)
    print(f"Metadata updated for " 
          f"{len(refreshed_products)} product(s)." 
    )
else:
    print("No data products required refreshing.")


print("Selective data-product refresh completed.")
