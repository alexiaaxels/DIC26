from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from monitoring import log_run
from quarantine import (
    log_validation_metrics,
    rejects_path,
    write_rejects,
    write_validation_report,
)
from validation import (
    SchemaContractError,
    SchemaContractRule,
    Validator,
    rules_for_dataset,
)

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

_WEEK1_SCRIPTS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "Week1")
)
if _WEEK1_SCRIPTS not in sys.path:
    sys.path.insert(0, _WEEK1_SCRIPTS)
from data_config import DATASET_CONFIGS

from common import (  # noqa: E402
    AIR_QUALITY_DELTA,
    AIR_QUALITY_UPDATE_PATH,
    INCREMENTAL_HISTORY_PATH,
    INCREMENTAL_REPORT_PATH,
    MONITORING_ENABLED,
    TAXI_TRIPS_DELTA,
    TAXI_TRIPS_UPDATE_PATH,
    UPDATES_DIR,
    VALIDATION_ENABLED,
    WEATHER_DELTA,
    WEATHER_UPDATE_PATH,
    get_spark,
)
from validation_rules import EXTRA_RULES


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


def _prepare(
    spark: SparkSession, dataset: str
) -> tuple[DataFrame, DataFrame, list[dict], bool]:
    config = DATASET_CONFIGS[dataset]

    df = _load_update(spark, dataset)
    df = _standardize_column_names(df)

    if not VALIDATION_ENABLED:
        if config["timestamp_builder"]:
            df = config["timestamp_builder"](df)
        if config["transform"]:
            df = config["transform"](df)
        empty_rejected = (
            df.limit(0)
              .withColumn("_rule_id", lit(None).cast("string"))
              .withColumn("_reject_reason", lit(None).cast("string"))
        )
        return df, empty_rejected, [], False

    schema_rule = SchemaContractRule(
        required=config["expected_cols"],
        known_optional=config.get("optional_cols", set()),
    )
    contract_result = schema_rule.apply(df)
    contract_metric = contract_result.metrics
    if contract_metric.get("hard_failure"):
        empty_rejected = (
            df.limit(0)
              .withColumn("_rule_id", lit(schema_rule.rule_id))
              .withColumn("_reject_reason", lit(contract_metric.get("reason", "")))
        )
        return df.limit(0), empty_rejected, [contract_metric], True

    if config["timestamp_builder"]:
        df = config["timestamp_builder"](df)
    if config["transform"]:
        df = config["transform"](df)

    effective_config = dict(config)
    effective_config["extra_rules"] = EXTRA_RULES.get(dataset, [])
    all_rules = rules_for_dataset(effective_config)
    row_rules = [r for r in all_rules if r.rule_type != "schema_contract"]

    validator = Validator(row_rules)
    outcome = validator.run(df, dataset)

    per_rule_metrics = [contract_metric] + outcome.per_rule_metrics

    return outcome.passed, outcome.rejected, per_rule_metrics, False


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

def _summarise_rule_metrics(metrics: list[dict]) -> dict:
    """Roll per-rule counts up into the legacy metric names used by the
    monitoring table and the Task 2 refresh pipeline."""
    rejected_invalid = 0
    duplicates_in_file = 0
    for m in metrics:
        if m.get("hard_failure"):
            # Schema hard failure: treat the whole batch as invalid.
            return {
                "rejected_invalid_records": int(m.get("rows_in", 0)),
                "intra_file_duplicate_records": 0,
            }
        rtype = m.get("rule_type")
        failed = int(m.get("rows_failed", 0))
        if rtype == "duplicate":
            duplicates_in_file += failed
        elif rtype in ("schema_contract",):
            # Soft failure or "unexpected extras" — no row rejections.
            continue
        else:
            rejected_invalid += failed
    return {
        "rejected_invalid_records": rejected_invalid,
        "intra_file_duplicate_records": duplicates_in_file,
    }


def _apply(
    spark: SparkSession, dataset: str, run_id: str
) -> dict:
    started_at = time.time()

    try:
        passed, rejected, per_rule_metrics, hard_failure = _prepare(spark, dataset)
    except SchemaContractError as e:
        elapsed = round(time.time() - started_at, 2)
        return {
            "dataset": dataset,
            "update_file": os.path.relpath(DATASETS[dataset]["update_path"]),
            "delta_table": os.path.relpath(DATASETS[dataset]["delta_path"]),
            "hard_failure": True,
            "hard_failure_reason": str(e),
            "rejected_invalid_records": 0,
            "intra_file_duplicate_records": 0,
            "schema_columns_added_from_update": [],
            "schema_evolved_on_target": [],
            "per_rule_metrics": [],
            "execution_time_s": elapsed,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "processed_records": 0,
            "validation_failures": 0,
            "schema_version": DATASET_CONFIGS[dataset]["schema_version"],
            "rows_before": None,
            "rows_after": None,
            "update_rows_considered": 0,
            "inserted": 0,
            "duplicates_ignored_vs_target": 0,
            "run_id": run_id,
        }

    delta_path = DATASETS[dataset]["delta_path"]
    target_schema_before = set(
        spark.read.format("delta").load(delta_path).columns
    )
    update_schema = set(passed.columns)
    added_columns = sorted(update_schema - target_schema_before)

    quarantined = write_rejects(rejected, dataset, run_id)

    if hard_failure:
        elapsed = round(time.time() - started_at, 2)
        summary = _summarise_rule_metrics(per_rule_metrics)
        base_version = DATASET_CONFIGS[dataset]["schema_version"].split(".")[0]
        return {
            "dataset": dataset,
            "update_file": os.path.relpath(DATASETS[dataset]["update_path"]),
            "delta_table": os.path.relpath(delta_path),
            "hard_failure": True,
            "hard_failure_reason": next(
                (m.get("reason") for m in per_rule_metrics if m.get("hard_failure")),
                "unspecified schema-contract failure",
            ),
            "rejected_invalid_records": summary["rejected_invalid_records"],
            "intra_file_duplicate_records": summary["intra_file_duplicate_records"],
            "quarantined_records": quarantined,
            "schema_columns_added_from_update": [],
            "schema_evolved_on_target": [],
            "per_rule_metrics": per_rule_metrics,
            "execution_time_s": elapsed,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "processed_records": 0,
            "validation_failures": summary["rejected_invalid_records"] + summary["intra_file_duplicate_records"],
            "schema_version": f"{base_version}.0",
            "rows_before": None,
            "rows_after": None,
            "update_rows_considered": 0,
            "inserted": 0,
            "duplicates_ignored_vs_target": 0,
            "run_id": run_id,
        }

    merge_stats = _merge(spark, dataset, passed)

    target_schema_after = set(
        spark.read.format("delta").load(delta_path).columns
    )
    schema_evolved = sorted(target_schema_after - target_schema_before)

    original_columns = set(
        spark.read.format("delta").option("versionAsOf", 0).load(delta_path).columns
    )
    columns_added = sorted(target_schema_after - original_columns)
    base_version = DATASET_CONFIGS[dataset]["schema_version"].split(".")[0]
    schema_version = f"{base_version}.{len(columns_added)}"

    summary = _summarise_rule_metrics(per_rule_metrics)

    return {
        "dataset": dataset,
        "update_file": os.path.relpath(DATASETS[dataset]["update_path"]),
        "delta_table": os.path.relpath(delta_path),
        "hard_failure": False,
        "rejected_invalid_records": summary["rejected_invalid_records"],
        "intra_file_duplicate_records": summary["intra_file_duplicate_records"],
        "quarantined_records": quarantined,
        "schema_columns_added_from_update": added_columns,
        "schema_evolved_on_target": schema_evolved,
        "per_rule_metrics": per_rule_metrics,
        "execution_time_s": round(time.time() - started_at, 2),
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "processed_records": merge_stats["update_rows_considered"]
        + summary["rejected_invalid_records"]
        + summary["intra_file_duplicate_records"],
        "validation_failures": summary["rejected_invalid_records"]
        + summary["intra_file_duplicate_records"],
        "schema_version": schema_version,
        **merge_stats,
        "run_id": run_id,
    }


def _write_run_report(report: dict) -> tuple[str, str]:

    os.makedirs(UPDATES_DIR, exist_ok=True)

    with open(INCREMENTAL_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)

    with open(INCREMENTAL_HISTORY_PATH, "a") as f:
        f.write(json.dumps(report, default=str, ensure_ascii=False) + "\n")

    return INCREMENTAL_REPORT_PATH, INCREMENTAL_HISTORY_PATH


def main(argv: list[str]) -> int:
    selected = argv[1:] if len(argv) > 1 else list(DATASETS)
    unknown = [d for d in selected if d not in DATASETS]
    if unknown:
        print(f"Unknown datasets: {unknown}. Available: {list(DATASETS)}",
              file=sys.stderr)
        return 2

    spark = get_spark("Week3 Task1 - incremental update pipeline")

    run_id = str(uuid.uuid4())
    report = {
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "datasets_requested": selected,
        "datasets": {},
    }

    validation_report = {
        "run_id": run_id,
        "started_at": report["started_at"],
        "datasets": {},
    }

    for name in selected:
        print(f"\n=== Applying incremental update for {name} ===")
        stats = _apply(spark, name, run_id)
        if MONITORING_ENABLED:
            log_run(spark, "task1_incremental_update", stats, stats["schema_version"])
            log_validation_metrics(
                spark,
                run_id=run_id,
                dataset=name,
                per_rule_metrics=stats.get("per_rule_metrics", []),
                schema_version=stats["schema_version"],
            )

        validation_report["datasets"][name] = {
            "hard_failure": stats.get("hard_failure", False),
            "hard_failure_reason": stats.get("hard_failure_reason"),
            "rejected_invalid_records": stats["rejected_invalid_records"],
            "intra_file_duplicate_records": stats["intra_file_duplicate_records"],
            "quarantined_records": stats.get("quarantined_records", 0),
            "quarantine_path": os.path.relpath(rejects_path(name)),
            "per_rule_metrics": stats.get("per_rule_metrics", []),
        }
        stats_for_log = {k: v for k, v in stats.items() if k != "per_rule_metrics"}
        report["datasets"][name] = stats_for_log

        for k, v in stats_for_log.items():
            print(f"  {k}: {v}")
        if stats.get("per_rule_metrics"):
            print("  per_rule_metrics:")
            for m in stats["per_rule_metrics"]:
                print(
                    f"    - {m.get('rule_id')} "
                    f"({m.get('rule_type')}): "
                    f"in={m.get('rows_in')} "
                    f"failed={m.get('rows_failed')} "
                    f"in {m.get('elapsed_s')}s"
                )

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    validation_report["finished_at"] = report["finished_at"]

    latest_path, history_path = _write_run_report(report)
    validation_report_path = write_validation_report(run_id, validation_report)

    print(f"\nLatest run written to     {latest_path}")
    print(f"History appended to       {history_path}")
    print(f"Validation report at      {validation_report_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
