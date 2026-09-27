from __future__ import annotations

import json
import os
import subprocess
import sys
import time
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

RESULTS_PATH = os.path.join(WEEK3_DIR, "Data", "reports", "benchmark_results.json")


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


def main() -> int:
    spark = get_spark("Week3 Task5 - benchmark harness")
    spark.sparkContext.setLogLevel("WARN")

    baseline_versions = {
        path: _current_delta_version(spark, path)
        for path in (TAXI_TRIPS_DELTA, WEATHER_DELTA, AIR_QUALITY_DELTA)
    }
    print("Baseline Delta versions:")
    for path, v in baseline_versions.items():
        print(f"  {os.path.relpath(path)}: version {v}")

    storage = {
        "taxi_trips_delta_mb": _du_mb(TAXI_TRIPS_DELTA),
        "weather_delta_mb": _du_mb(WEATHER_DELTA),
        "air_quality_delta_mb": _du_mb(AIR_QUALITY_DELTA),
        "rejects_delta_mb": _du_mb(os.path.join(WEEK3_DIR, "delta", "rejects")),
        "monitoring_delta_mb": _du_mb(os.path.join(WEEK3_DIR, "delta", "monitoring")),
        "updates_dir_mb": _du_mb(os.path.join(WEEK3_DIR, "Data", "updates")),
    }

    print("\n[warm-up] running pipeline once to warm the JVM cache...")
    _ = _run_pipeline(validation=True, monitoring=True)
    for path, v in baseline_versions.items():
        _restore(spark, path, v)

    configurations = [
        ("v_on__m_on",  True,  True),
        ("v_on__m_off", True,  False),
        ("v_off__m_on", False, True),
        ("v_off__m_off", False, False),
    ]

    pipeline_timings: dict[str, float] = {}
    for name, v, m in configurations:
        print(f"\n[bench] pipeline V={v} M={m} ...")
        pipeline_timings[name] = _run_pipeline(validation=v, monitoring=m)
        for path, ver in baseline_versions.items():
            _restore(spark, path, ver)

    print("\n[bench] refresh after full incremental update ...")
    _ = _run_pipeline(validation=True, monitoring=True)
    refresh_after_change = _run_refresh()
    for path, v in baseline_versions.items():
        _restore(spark, path, v)

    print("\n[bench] refresh after no-op (nothing changed) ...")
    _ = _run_pipeline(validation=True, monitoring=True)
    _ = _run_pipeline(validation=True, monitoring=True)
    refresh_after_noop = _run_refresh()
    for path, v in baseline_versions.items():
        _restore(spark, path, v)

    baseline = pipeline_timings["v_on__m_on"]
    minimum = pipeline_timings["v_off__m_off"]
    validation_overhead = round(baseline - pipeline_timings["v_off__m_on"], 2)
    monitoring_overhead = round(baseline - pipeline_timings["v_on__m_off"], 2)

    results = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "storage_mb": storage,
        "pipeline_wallclock_s": pipeline_timings,
        "overhead_s": {
            "validation_marginal": validation_overhead,
            "monitoring_marginal": monitoring_overhead,
            "baseline_total": baseline,
            "minimum_total": minimum,
        },
        "refresh_s": {
            "after_change": refresh_after_change,
            "after_noop": refresh_after_noop,
        },
    }

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 62)
    print("BENCHMARK RESULTS")
    print("=" * 62)
    print("\nStorage (MB):")
    for k, v in storage.items():
        print(f"  {k:<32s} {v:>8.2f}")

    print("\nIncremental pipeline wall-clock (seconds):")
    for k, v in pipeline_timings.items():
        print(f"  {k:<20s} {v:>8.2f}")

    print("\nMarginal overheads (seconds):")
    print(f"  validation           {validation_overhead:>8.2f}")
    print(f"  monitoring           {monitoring_overhead:>8.2f}")

    print("\nAnalytical refresh (seconds):")
    print(f"  after change         {refresh_after_change:>8.2f}")
    print(f"  after no-op          {refresh_after_noop:>8.2f}")

    print(f"\nFull results JSON: {RESULTS_PATH}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
