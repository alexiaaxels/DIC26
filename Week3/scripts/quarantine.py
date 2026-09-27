from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from common import WEEK3_DIR


REJECTS_ROOT = os.path.join(WEEK3_DIR, "delta", "rejects")
REPORTS_DIR = os.path.join(WEEK3_DIR, "Data", "reports")
VALIDATION_METRICS_PATH = os.path.join(WEEK3_DIR, "delta", "monitoring", "validation_metrics")


def rejects_path(dataset: str) -> str:
    return os.path.join(REJECTS_ROOT, dataset)


def write_rejects(rejected: DataFrame, dataset: str, run_id: str) -> int:
    if not rejected.take(1):
        return 0

    now = datetime.now(timezone.utc).isoformat()
    enriched = (
        rejected
        .withColumn("_run_id", lit(run_id))
        .withColumn("_rejected_at", lit(now))
    )

    target = rejects_path(dataset)
    os.makedirs(os.path.dirname(target), exist_ok=True)

    (
        enriched.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(target)
    )
    return enriched.count()


def log_validation_metrics(
    spark: SparkSession,
    run_id: str,
    dataset: str,
    per_rule_metrics: list[dict],
    schema_version: str,
) -> None:
    if not per_rule_metrics:
        return

    now = datetime.now(timezone.utc)
    rows = [
        (
            run_id,
            dataset,
            now,
            m.get("rule_id"),
            m.get("rule_type"),
            int(m.get("rows_in", 0)),
            int(m.get("rows_failed", 0)),
            int(m.get("rows_passed", 0)),
            float(m.get("elapsed_s", 0.0)),
            bool(m.get("hard_failure", False)),
            schema_version,
        )
        for m in per_rule_metrics
    ]
    columns = [
        "run_id", "dataset", "executed_at",
        "rule_id", "rule_type",
        "rows_in", "rows_failed", "rows_passed", "elapsed_s",
        "hard_failure", "schema_version",
    ]
    (
        spark.createDataFrame(rows, columns)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(VALIDATION_METRICS_PATH)
    )


def write_validation_report(run_id: str, report: dict) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, f"validation_{run_id}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    return path
