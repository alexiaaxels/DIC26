from __future__ import annotations

import os
import random
from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, expr, lit, rand, row_number
)
from pyspark.sql.window import Window

from common import (
    RANDOM_SEED,
    TAXI_TRIPS_RAW_PATHS,
    TAXI_TRIPS_UPDATE_PATH,
    ensure_updates_dir,
    get_spark,
)

NEW_FRACTION = 0.07  # 7% new trips (project asks for 5-10% new taxi trips)
DUPLICATE_FRACTION = 0.015  # ~1.5% duplicates

# Create a Parquet file
# containing 5-10% new taxi trips
# with timestamps occurring after the latest trip in the og dataset
# with 1-2% duplicate trips copied from the og dataset
# should resemble the og data (similar PU and DO locations, distances and fare amounts) 

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

    max_pickup = raw.selectExpr("max(tpep_pickup_datetime) as m").collect()[0]["m"]
    if max_pickup is None:
        raise RuntimeError("Could not compute max pickup time from raw taxi data.")

    window_seconds = 30 * 24 * 3600

    seconds_from_original_min_to_max_offset = (
        max_pickup + timedelta(seconds=1)
    )

    new_sample = raw.sample(False, NEW_FRACTION, seed=RANDOM_SEED)

    sample_min = new_sample.selectExpr("min(tpep_pickup_datetime) as m").collect()[0]["m"]
    if sample_min is None:
        sample_min = raw.selectExpr("min(tpep_pickup_datetime) as m").collect()[0]["m"]
    delta_seconds = int(
        (seconds_from_original_min_to_max_offset - sample_min).total_seconds()
    )

    shift_expr = f"make_interval(0,0,0,0,0,0,{delta_seconds})" # make_interval(Y,M,W,D,H,M,S)
    new_trips = (
        new_sample
        .withColumn("_jitter_s", (rand(seed=RANDOM_SEED + 1) * window_seconds).cast("long"))
        .withColumn(
            "tpep_pickup_datetime",
            expr(f"tpep_pickup_datetime + {shift_expr} + make_interval(0,0,0,0,0,0,_jitter_s)"),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            expr(f"tpep_dropoff_datetime + {shift_expr} + make_interval(0,0,0,0,0,0,_jitter_s)"),
        )
        .drop("_jitter_s")
    )
    new_count = new_trips.count()

    # TODO: The shifting AND 0-30 day jitter causes the entries to be way sparser than the OG dataset.
    # Like we are selecting 7% of the OG entries that span 3 months, but we span that over 3-4 months
    # 7% of something that spans 3 months should not span 3-4 months, it should probably be scattered over less than a month
    # I think we have to mimic the density of the OG dataset

    # TODO: We currently just copy the entries and change their timestamps. 
    # I think we would have to actually make them new entries, like change the location and duration and amounts
    # but I am not entirely sure how it's best to do that


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
        "original_max_pickup": max_pickup.isoformat(),
        "new_records": new_count,
        "duplicate_records": dup_count,
        "total_records_in_file": new_count + dup_count,
        "new_fraction": NEW_FRACTION,
        "duplicate_fraction": DUPLICATE_FRACTION,
        "schema_changes": [],
        "notes": (
            "New trips are sampled from the original data and time-shifted to "
            "the month following the original max pickup time. Duplicates are "
            "verbatim rows from the original dataset."
        ),
    }

    spark.stop()
    return manifest


if __name__ == "__main__":
    m = generate()
    print("taxi_trips update generated:")
    for k, v in m.items():
        print(f"  {k}: {v}")
