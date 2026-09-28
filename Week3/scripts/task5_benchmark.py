from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import statistics
from datetime import datetime, timezone

from delta.tables import DeltaTable

from common import (
    AIR_QUALITY_DELTA,
    TAXI_TRIPS_DELTA,
    WEATHER_DELTA,
    WEEK3_DIR,
    get_spark,
)


SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
INCREMENTAL_SCRIPT = os.path.join(SCRIPTS_DIR, "task1_incremental_update.py")
REFRESH_SCRIPT = os.path.join(SCRIPTS_DIR, "task2_update_data_products.py")
GENERATE_SCRIPT = os.path.join(SCRIPTS_DIR, "task1_generate_updates.py")
REPO_ROOT = os.path.dirname(WEEK3_DIR)
WEEK1_DELTA_DIR = os.path.join(REPO_ROOT, "Week1", "delta")
WEEK2_DIR = os.path.join(REPO_ROOT, "Week2")
WEEK2_PRODUCTS_SCRIPT = os.path.join(WEEK2_DIR, "task4_data_products.py")
UPDATES_DIR = os.path.join(WEEK3_DIR, "Data", "updates")

RESULTS_PATH = os.path.join(WEEK3_DIR, "Data", "reports", "benchmark_results.json")

REPEATS = 3

def _du_mb(path: str) -> float:
    if not os.path.exists(path):
        return 0.0
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return round(total / (1024 * 1024), 2)


def _current_delta_version(spark, path: str) -> int:
    return int(
        DeltaTable.forPath(spark, path)
        .history(1)
        .select("version")
        .collect()[0]["version"]
    )


def _restore(spark, path: str, version: int) -> None:
    spark.sql(
        f"RESTORE TABLE delta.`{path}` TO VERSION AS OF {version}"
    )


def _run_pipeline(validation: bool, monitoring: bool) -> float:
    env = os.environ.copy()
    env["DIC26_VALIDATION_ENABLED"] = "true" if validation else "false"
    env["DIC26_MONITORING_ENABLED"] = "true" if monitoring else "false"

    started = time.time()
    result = subprocess.run(
        [sys.executable, INCREMENTAL_SCRIPT],
        env=env,
        cwd=WEEK3_DIR,
        capture_output=True,
        text=True,
    )
    elapsed = round(time.time() - started, 2)
    if result.returncode != 0:
        print(result.stdout[-2000:])
        print(result.stderr[-2000:], file=sys.stderr)
        raise RuntimeError(f"pipeline failed under V={validation} M={monitoring}")
    return elapsed


def _run_refresh() -> float:
    started = time.time()
    result = subprocess.run(
        [sys.executable, REFRESH_SCRIPT],
        cwd=WEEK3_DIR,
        capture_output=True,
        text=True,
    )
    elapsed = round(time.time() - started, 2)
    if result.returncode != 0:
        print(result.stdout[-2000:])
        print(result.stderr[-2000:], file=sys.stderr)
        raise RuntimeError("task2 refresh failed")
    return elapsed

def _run_script(script: str, cwd: str) -> None:
    result = subprocess.run([sys.executable, script], cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout[-2000:])
        print(result.stderr[-2000:], file=sys.stderr)
        raise RuntimeError(f"{script} failed")

def _prerequisites() -> None:
    if not os.listdir(UPDATES_DIR) if os.path.exists(UPDATES_DIR) else True:
        _run_script(GENERATE_SCRIPT, WEEK3_DIR)
    if not os.path.exists(os.path.join(WEEK2_DIR, "delta", "data_products_metadata")):
        _run_script(WEEK2_PRODUCTS_SCRIPT, WEEK2_DIR)

def _storage() -> dict:
    week1 = _du_mb(WEEK1_DELTA_DIR)
    week2 = _du_mb(os.path.join(WEEK2_DIR, "delta"))
    added = {
        "rejects_mb": _du_mb(os.path.join(WEEK3_DIR, "delta", "rejects")),
        "monitoring_mb": _du_mb(os.path.join(WEEK3_DIR, "delta", "monitoring")),
        "reports_mb": _du_mb(os.path.join(WEEK3_DIR, "Data", "reports")),
        "updates_dir_mb": _du_mb(UPDATES_DIR),
    }
    added_total = round(sum(added.values()), 2)
    return {
        "week1_delta_mb": week1,
        "week2_products_mb": week2,
        **added,
        "week3_added_total_mb": added_total,
        "overhead_pct_of_week1_week2": round(100 * added_total / (week1 + week2), 2),
    }

def main() -> int:
    _prerequisites()
    spark = get_spark("Week3 Task5 - benchmark harness")
    spark.sparkContext.setLogLevel("WARN")

    baseline_versions = {
        path: _current_delta_version(spark, path)
        for path in (TAXI_TRIPS_DELTA, WEATHER_DELTA, AIR_QUALITY_DELTA)
    }
    print("Baseline Delta versions:")
    for path, v in baseline_versions.items():
        print(f"  {os.path.relpath(path)}: version {v}")

    storage_before = _storage()

    print("\n[warm-up] running pipeline once ...")
    _ = _run_pipeline(validation=True, monitoring=True)
    for path, v in baseline_versions.items():
        _restore(spark, path, v)

    configurations = [
        ("v_on__m_on",  True,  True),
        ("v_on__m_off", True,  False),
        ("v_off__m_on", False, True),
        ("v_off__m_off", False, False),
    ]

    samples: dict[str, list[float]] = {name: [] for name, _, _ in configurations}
    for i in range(REPEATS):
        for name, v, m in configurations:
            print(f"\n[bench {i + 1}/{REPEATS}] pipeline V={v} M={m} ...")
            samples[name].append(_run_pipeline(validation=v, monitoring=m))
            for path, ver in baseline_versions.items():
                _restore(spark, path, ver)

    pipeline_timings = {name: round(statistics.median(s), 2) for name, s in samples.items()}

    print("\n[bench] refresh after full incremental update ...")
    _ = _run_pipeline(validation=True, monitoring=True)
    refresh_after_change = _run_refresh()
    for path, v in baseline_versions.items():
        _restore(spark, path, v)

    storage_after = _storage()

    baseline = pipeline_timings["v_on__m_on"]
    minimum = pipeline_timings["v_off__m_off"]
    validation_overhead = round(baseline - pipeline_timings["v_off__m_on"], 2)
    monitoring_overhead = round(baseline - pipeline_timings["v_on__m_off"], 2)

    results = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "storage_before_mb": storage_before,
        "storage_adter_mb": storage_after,
        "repeats": REPEATS,
        "pipeline_samples_s": samples,
        "pipeline_wallclock_s": pipeline_timings,
        "overhead_s": {
            "validation_marginal": validation_overhead,
            "monitoring_marginal": monitoring_overhead,
            "baseline_total": baseline,
            "minimum_total": minimum,
        },
        "refresh_s": {
            "after_change": refresh_after_change,
        },
    }

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 62)
    print("BENCHMARK RESULTS")
    print("=" * 62)
    print(f"\n  {'Storage (MB)':<32s} {'before':>8}  {'after':>8}")
    for k in storage_before:
            print(f"  {k:<32s} {storage_before[k]:>8}  {storage_after[k]:>8}")

    print("\nIncremental pipeline wall-clock (seconds):")
    for k, v in pipeline_timings.items():
        print(f"  {k:<20s} {v:>8.2f}")

    print("\nMarginal overheads (seconds):")
    print(f"  validation           {validation_overhead:>8.2f}")
    print(f"  monitoring           {monitoring_overhead:>8.2f}")

    print("\nAnalytical refresh (seconds):")
    print(f"  after change         {refresh_after_change:>8.2f}")

    print(f"\nFull results JSON: {RESULTS_PATH}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
