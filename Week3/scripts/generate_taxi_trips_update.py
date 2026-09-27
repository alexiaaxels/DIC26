from __future__ import annotations

import os
from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, rand
from pyspark.sql.types import DoubleType, LongType

from common import (
    RANDOM_SEED,
    TAXI_TRIPS_RAW_PATHS,
    TAXI_TRIPS_UPDATE_PATH,
    ensure_updates_dir,
    get_spark,
)

NEW_FRACTION = 0.07
DUPLICATE_FRACTION = 0.015

DENSITY_MATCH = True

FARE_JITTER_PCT = 0.05
DISTANCE_JITTER_PCT = 0.10
PASSENGER_JITTER_PROB = 0.20


def _load_raw(spark) -> DataFrame:
    missing = [p for p in TAXI_TRIPS_RAW_PATHS if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(
            "Missing raw taxi files (they are git-ignored). "
            f"Please place them at: {missing}"
        )
    return spark.read.parquet(*TAXI_TRIPS_RAW_PATHS)


def generate() -> dict:
    ensure_updates_dir()
    spark = get_spark("Week3 Task1 - taxi_trips update generator")

    raw = _load_raw(spark).cache()
    raw_count = raw.count()

    minmax = raw.selectExpr(
        "min(tpep_pickup_datetime) as lo",
        "max(tpep_pickup_datetime) as hi",
    ).collect()[0]
    min_pickup = minmax["lo"]
    max_pickup = minmax["hi"]
    if min_pickup is None or max_pickup is None:
        raise RuntimeError("Could not compute pickup-time bounds from raw taxi data.")

    original_span_s = int((max_pickup - min_pickup).total_seconds())

    if DENSITY_MATCH:
        window_seconds = int(original_span_s * NEW_FRACTION)
    else:
        window_seconds = 30 * 24 * 3600

    new_sample = raw.sample(withReplacement=False,
                            fraction=NEW_FRACTION,
                            seed=RANDOM_SEED)

    sample_min = new_sample.selectExpr("min(tpep_pickup_datetime) as m").collect()[0]["m"]
    if sample_min is None:
        sample_min = min_pickup

    target_start = max_pickup + timedelta(seconds=1)
    delta_seconds = int((target_start - sample_min).total_seconds())
    base_shift = f"make_interval(0,0,0,0,0,0,{delta_seconds})"

    new_trips = (
        new_sample
        .withColumn(
            "_jitter_s",
            (rand(seed=RANDOM_SEED + 1) * window_seconds).cast(LongType()),
        )
        .withColumn(
            "tpep_pickup_datetime",
            expr(
                f"tpep_pickup_datetime + {base_shift} "
                f"+ make_interval(0,0,0,0,0,0,_jitter_s)"
            ),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            expr(
                f"tpep_dropoff_datetime + {base_shift} "
                f"+ make_interval(0,0,0,0,0,0,_jitter_s)"
            ),
        )
        .drop("_jitter_s")
    )

    fj = FARE_JITTER_PCT
    dj = DISTANCE_JITTER_PCT
    pj = PASSENGER_JITTER_PROB

    new_trips = (
        new_trips
        .withColumn(
            "fare_amount",
            (expr(f"fare_amount * (1 + (rand({RANDOM_SEED + 3}) * 2 - 1) * {fj})"))
            .cast(DoubleType()),
        )
        .withColumn(
            "trip_distance",
            (expr(f"trip_distance * (1 + (rand({RANDOM_SEED + 4}) * 2 - 1) * {dj})"))
            .cast(DoubleType()),
        )
        .withColumn(
            "passenger_count",
            expr(
                f"greatest(1, passenger_count + "
                f"case when rand({RANDOM_SEED + 5}) < {pj} "
                f"then case when rand({RANDOM_SEED + 6}) < 0.5 then -1 else 1 end "
                f"else 0 end)"
            ).cast(LongType()),
        )
    )
    new_count = new_trips.count()

    duplicates = raw.sample(False, DUPLICATE_FRACTION, seed=RANDOM_SEED + 2)
    dup_count = duplicates.count()

    update = new_trips.unionByName(duplicates)

    tmp_out = TAXI_TRIPS_UPDATE_PATH + ".tmp"
    (
        update
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(tmp_out)
    )
    part_files = [f for f in os.listdir(tmp_out) if f.endswith(".parquet")]
    if len(part_files) != 1:
        raise RuntimeError(f"Expected one part file, found {part_files}")
    final_src = os.path.join(tmp_out, part_files[0])
    if os.path.exists(TAXI_TRIPS_UPDATE_PATH):
        os.remove(TAXI_TRIPS_UPDATE_PATH)
    os.replace(final_src, TAXI_TRIPS_UPDATE_PATH)
    for f in os.listdir(tmp_out):
        os.remove(os.path.join(tmp_out, f))
    os.rmdir(tmp_out)

    manifest = {
        "dataset": "taxi_trips",
        "format": "parquet",
        "output_path": os.path.relpath(TAXI_TRIPS_UPDATE_PATH),
        "original_row_count": raw_count,
        "original_min_pickup": min_pickup.isoformat(),
        "original_max_pickup": max_pickup.isoformat(),
        "original_span_days": round(original_span_s / 86400, 2),
        "new_window_days": round(window_seconds / 86400, 2),
        "new_records": new_count,
        "duplicate_records": dup_count,
        "total_records_in_file": new_count + dup_count,
        "new_fraction": NEW_FRACTION,
        "duplicate_fraction": DUPLICATE_FRACTION,
        "fare_jitter_pct": FARE_JITTER_PCT,
        "distance_jitter_pct": DISTANCE_JITTER_PCT,
        "passenger_jitter_prob": PASSENGER_JITTER_PROB,
        "schema_changes": [],
        "notes": (
            "New trips are sampled from the original data, time-shifted so "
            "the earliest new pickup falls one second after the original "
            "max pickup, and jittered across a window whose width equals "
            "NEW_FRACTION * original_span (density-preserving). Fare, "
            "distance and passenger_count are perturbed so trips are "
            "similar but distinct. Duplicates are verbatim rows from the "
            "original dataset."
        ),
    }

    spark.stop()
    return manifest


if __name__ == "__main__":
    m = generate()
    print("taxi_trips update generated:")
    for k, v in m.items():
        print(f"  {k}: {v}")

