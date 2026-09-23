from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

_WEEK1_SCRIPTS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "Week1")
)
if _WEEK1_SCRIPTS not in sys.path:
    sys.path.insert(0, _WEEK1_SCRIPTS)
from data_config import DATASET_CONFIGS

from common import (  # noqa: E402
    AIR_QUALITY_DELTA,
    AIR_QUALITY_UPDATE_PATH,
    TAXI_TRIPS_DELTA,
    TAXI_TRIPS_UPDATE_PATH,
    UPDATES_DIR,
    WEATHER_DELTA,
    WEATHER_UPDATE_PATH,
    get_spark,
)


DATASETS = {
    "taxi_trips": {
        "update_path": TAXI_TRIPS_UPDATE_PATH,
        "delta_path": TAXI_TRIPS_DELTA,
        "format": "parquet",
    },
    "weather": {
        "update_path": WEATHER_UPDATE_PATH,
        "delta_path": WEATHER_DELTA,
        "format": "csv",
    },
    "air_quality": {
        "update_path": AIR_QUALITY_UPDATE_PATH,
        "delta_path": AIR_QUALITY_DELTA,
        "format": "csv",
    },
}


def _to_snake_case(name: str) -> str:
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"\s+", "_", name)
    return name.lower()


def _standardize_column_names(df: DataFrame) -> DataFrame:
    for c in df.columns:
        df = df.withColumnRenamed(c, _to_snake_case(c))
    return df


def _validate_schema(df: DataFrame, config: dict, dataset: str) -> None:
    missing = config["expected_cols"] - set(c.lower() for c in df.columns)
    if missing:
        raise ValueError(
            f"[{dataset}] update file is missing expected columns: {missing}"
        )


def _numeric_range_filter(df: DataFrame, config: dict) -> tuple[DataFrame, int]:
    before = df.count()
    for field, (lo, hi) in config["numeric_checks"].items():
        if field not in df.columns:
            continue
        if lo is not None:
            df = df.filter((col(field) >= lo) | col(field).isNull())
        if hi is not None:
            df = df.filter((col(field) <= hi) | col(field).isNull())
    rejected = before - df.count()
    return df, rejected


def _load_update(spark: SparkSession, dataset: str) -> DataFrame:
    info = DATASETS[dataset]
    path = info["update_path"]
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{dataset} file not found. Run task1_generate_updates.py first."
        )
    reader = spark.read.option("header", True).option("inferSchema", True)
    if info["format"] == "csv":
        return reader.csv(path)
    if info["format"] == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported update format: {info['format']}")


def _prepare(spark: SparkSession, dataset: str) -> tuple[DataFrame, int, int]:
    config = DATASET_CONFIGS[dataset]
    df = _load_update(spark, dataset)
    df = _standardize_column_names(df)
    _validate_schema(df, config, dataset)

    if config["timestamp_builder"]:
        df = config["timestamp_builder"](df)
    if config["transform"]:
        df = config["transform"](df)

    total_after_transform = df.count()

    key_cols = config["key_cols"]
    df_nonull = df.dropna(subset=key_cols)
    key_nulls = total_after_transform - df_nonull.count()

    df_dedup = df_nonull.dropDuplicates(key_cols)
    duplicates_in_file = df_nonull.count() - df_dedup.count()

    df_valid, range_rejected = _numeric_range_filter(df_dedup, config)
    rejected_invalid = key_nulls + range_rejected

    return df_valid, rejected_invalid, duplicates_in_file


def _merge_condition(key_cols: list[str]) -> str:
    return " and ".join(f"t.`{k}` <=> s.`{k}`" for k in key_cols)


def _merge(spark: SparkSession, dataset: str, updates: DataFrame) -> dict:
    info = DATASETS[dataset]
    config = DATASET_CONFIGS[dataset]
    delta_path = info["delta_path"]

    if not DeltaTable.isDeltaTable(spark, delta_path):
        raise RuntimeError(
            f"[{dataset}] target Delta table not found at {delta_path}, run Week 1 ingestion first. "
        )

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    target = DeltaTable.forPath(spark, delta_path)
    rows_before = target.toDF().count()
    updates_count = updates.count()

    (
        target.alias("t")
        .merge(updates.alias("s"), _merge_condition(config["key_cols"]))
        .whenNotMatchedInsertAll()
        .execute()
    )

    rows_after = spark.read.format("delta").load(delta_path).count()
    inserted = rows_after - rows_before
    duplicates_ignored = updates_count - inserted

    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "update_rows_considered": updates_count,
        "inserted": inserted,
        "duplicates_ignored_vs_target": duplicates_ignored,
    }

def _apply(spark: SparkSession, dataset: str) -> dict:
    started_at = time.time()
    updates, rejected_invalid, duplicates_in_file = _prepare(spark, dataset)

    delta_path = DATASETS[dataset]["delta_path"]
    target_schema_before = set(
        spark.read.format("delta").load(delta_path).columns
    )
    update_schema = set(updates.columns)
    added_columns = sorted(update_schema - target_schema_before)

    merge_stats = _merge(spark, dataset, updates)

    target_schema_after = set(
        spark.read.format("delta").load(delta_path).columns
    )
    schema_evolved = sorted(target_schema_after - target_schema_before)

    return {
        "dataset": dataset,
        "update_file": os.path.relpath(DATASETS[dataset]["update_path"]),
        "delta_table": os.path.relpath(delta_path),
        "rejected_invalid_records": rejected_invalid,
        "intra_file_duplicate_records": duplicates_in_file,
        "schema_columns_added_from_update": added_columns,
        "schema_evolved_on_target": schema_evolved,
        "execution_time_s": round(time.time() - started_at, 2),
        "executed_at": datetime.now(timezone.utc).isoformat(),
        **merge_stats,
    }


def _write_run_report(report: dict) -> str:
    os.makedirs(UPDATES_DIR, exist_ok=True)
    report_path = os.path.join(UPDATES_DIR, "incremental_run_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    return report_path


def main(argv: list[str]) -> int:
    selected = argv[1:] if len(argv) > 1 else list(DATASETS)
    unknown = [d for d in selected if d not in DATASETS]
    if unknown:
        print(f"Unknown datasets: {unknown}. Available: {list(DATASETS)}",
              file=sys.stderr)
        return 2

    spark = get_spark("Week3 Task1 - incremental update pipeline")

    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "datasets": {},
    }

    for name in selected:
        print(f"\n=== Applying incremental update for {name} ===")
        stats = _apply(spark, name)
        report["datasets"][name] = stats
        for k, v in stats.items():
            print(f"  {k}: {v}")

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report_path = _write_run_report(report)
    print(f"\nRun report written to {report_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
