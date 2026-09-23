from __future__ import annotations

import os
import random
from datetime import date, datetime, time, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    to_timestamp,
    max as spark_max,
)
from pyspark.sql.types import IntegerType, StructField, StructType

from common import (
    RANDOM_SEED,
    AIR_QUALITY_RAW_PATH,
    AIR_QUALITY_UPDATE_PATH,
    ensure_updates_dir,
    get_spark,
)

NEW_HOURS = 24

MAX_SITES = 200


def _load_raw(spark) -> DataFrame:
    if not os.path.exists(AIR_QUALITY_RAW_PATH):
        raise FileNotFoundError(
            f"Missing raw air-quality file at {AIR_QUALITY_RAW_PATH}"
        )
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(AIR_QUALITY_RAW_PATH)
    )


def _pm25_to_aqi(concentration: float) -> int:
    if concentration is None:
        return None
    bps = [
        (0.0, 9.0, 0, 50),
        (9.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 125.4, 151, 200),
        (125.5, 225.4, 201, 300),
        (225.5, 500.0, 301, 500),
    ]
    c = max(0.0, float(concentration))
    for c_lo, c_hi, i_lo, i_hi in bps:
        if c_lo <= c <= c_hi:
            return int(round((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo))
    return 500


def _to_datetime(date_value, time_value) -> datetime:
    """Combine a date value and a time value from a Spark Row into ``datetime``.

    Handles the various shapes Spark may return depending on schema inference:
      * ``date_value``  : ``datetime.date`` (usual), ``datetime.datetime``, or
                          ``str`` in ``YYYY-MM-DD``.
      * ``time_value``  : ``datetime.time``, ``datetime.datetime`` (Spark maps
                          bare ``HH:mm`` to a timestamp on today's date),
                          or ``str`` in ``HH:MM``.
    """
    # Normalise date_value -> datetime.date
    if isinstance(date_value, datetime):
        d = date_value.date()
    elif isinstance(date_value, date):
        d = date_value
    elif isinstance(date_value, str):
        d = datetime.strptime(date_value, "%Y-%m-%d").date()
    else:
        raise TypeError(f"Unsupported date value: {date_value!r}")

    # Normalise time_value -> datetime.time
    if isinstance(time_value, datetime):
        t = time_value.time()
    elif isinstance(time_value, time):
        t = time_value
    elif isinstance(time_value, str):
        t = datetime.strptime(time_value, "%H:%M").time()
    else:
        raise TypeError(f"Unsupported time value: {time_value!r}")

    return datetime.combine(d, t)


def _combine_time_only(dt: datetime):
    """Return a value shaped to match the raw file's ``time_*`` column type.

    Spark's ``inferSchema`` reads ``HH:mm`` as a full timestamp anchored to
    today's date. We reproduce that here so ``createDataFrame`` accepts the
    row against the raw schema.
    """
    return datetime.combine(date.today(), dt.time())


def generate() -> dict:
    ensure_updates_dir()
    spark = get_spark("Week3 Task1 - air_quality update generator")
    rng = random.Random(RANDOM_SEED)

    raw = _load_raw(spark)
    raw_columns = raw.columns
    raw_count = raw.count()

    def _c(name_lower: str) -> str:
        for c in raw_columns:
            if c.strip().lower().replace(" ", "_") == name_lower:
                return c
        raise KeyError(f"Column {name_lower!r} not present in raw file. "
                       f"Available: {raw_columns}")

    col_state = _c("state_code")
    col_county = _c("county_code")
    col_site = _c("site_num")
    col_poc = _c("poc")
    col_date_gmt = _c("date_gmt")
    col_time_gmt = _c("time_gmt")
    col_date_local = _c("date_local")
    col_time_local = _c("time_local")
    col_sample = _c("sample_measurement")

    # NOTE: ``date_gmt`` is inferred as a *timestamp* by Spark when the CSV is
    # read with ``inferSchema=true``. Casting it directly to string yields
    # ``"2024-03-28 00:00:00"``, which concatenated with ``time_gmt`` no
    # longer matches ``yyyy-MM-dd HH:mm``. We must format it explicitly.
    date_gmt_str = date_format(col(col_date_gmt), "yyyy-MM-dd")
    time_gmt_str = date_format(col(col_time_gmt), "HH:mm")
    with_ts = raw.withColumn(
        "_ts_gmt",
        to_timestamp(concat_ws(" ", date_gmt_str, time_gmt_str),
                     "yyyy-MM-dd HH:mm"),
    )

    latest_per_site = (
        with_ts
        .groupBy(col_state, col_county, col_site, col_poc)
        .agg(spark_max("_ts_gmt").alias("max_ts"))
    )

    if MAX_SITES is not None:
        latest_per_site = latest_per_site.orderBy(
            col_state, col_county, col_site, col_poc
        ).limit(MAX_SITES)

    site_rows = latest_per_site.collect()

    site_keys = [
        (r[col_state], r[col_county], r[col_site], r[col_poc]) for r in site_rows
    ]
    key_to_max_ts = {
        (r[col_state], r[col_county], r[col_site], r[col_poc]): r["max_ts"]
        for r in site_rows
    }

    template_rows = {}
    for key in site_keys:
        state, county, site, poc = key
        template = (
            with_ts
            .filter((col(col_state) == state) &
                    (col(col_county) == county) &
                    (col(col_site) == site) &
                    (col(col_poc) == poc))
            .orderBy(col("_ts_gmt").desc())
            .limit(1)
            .collect()
        )
        if template:
            template_rows[key] = {f: template[0][f] for f in template[0].__fields__
                                  if f != "_ts_gmt"}

    new_rows = []
    for key, template in template_rows.items():
        max_ts = key_to_max_ts[key]
        if max_ts is None:
            continue
        # Values coming back from Spark are already ``date`` / ``datetime``
        # objects (or strings, depending on schema inference). Handle both.
        template_gmt = _to_datetime(template[col_date_gmt], template[col_time_gmt])
        template_local = _to_datetime(template[col_date_local], template[col_time_local])
        local_offset = template_local - template_gmt

        for i in range(1, NEW_HOURS + 1):
            new_gmt = max_ts + timedelta(hours=i)
            new_local = new_gmt + local_offset

            row = dict(template)
            # Preserve the original column *types* (date / timestamp).
            # ``date_gmt`` is a date; ``time_gmt`` is inferred as a
            # timestamp-of-today with HH:mm populated. Match that.
            row[col_date_gmt] = new_gmt.date()
            row[col_time_gmt] = _combine_time_only(new_gmt)
            row[col_date_local] = new_local.date()
            row[col_time_local] = _combine_time_only(new_local)

            base = template.get(col_sample) or 0.0
            try:
                base_val = float(base)
            except (TypeError, ValueError):
                base_val = 0.0
            new_val = max(0.0, round(base_val + rng.uniform(-3.0, 3.0), 1))
            row[col_sample] = new_val

            row["aqi"] = _pm25_to_aqi(new_val)

            new_rows.append(row)

    output_columns = raw_columns + ["aqi"]
    # Explicit schema so ``createDataFrame`` doesn't try to infer types on
    # columns that are entirely null across all templates (e.g. ``qualifier``,
    # ``method_type``, ``uncertainty`` are almost always blank in EPA data).
    output_schema = StructType(
        list(raw.schema.fields) + [StructField("aqi", IntegerType(), True)]
    )
    update_df = spark.createDataFrame(
        [[r.get(c) for c in output_columns] for r in new_rows],
        schema=output_schema,
    )

    tmp_out = AIR_QUALITY_UPDATE_PATH + ".tmp"
    (
        update_df
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(tmp_out)
    )
    part = [f for f in os.listdir(tmp_out) if f.endswith(".csv")]
    if len(part) != 1:
        raise RuntimeError(f"Expected one part file, found {part}")
    if os.path.exists(AIR_QUALITY_UPDATE_PATH):
        os.remove(AIR_QUALITY_UPDATE_PATH)
    os.replace(os.path.join(tmp_out, part[0]), AIR_QUALITY_UPDATE_PATH)
    for f in os.listdir(tmp_out):
        os.remove(os.path.join(tmp_out, f))
    os.rmdir(tmp_out)

    manifest = {
        "dataset": "air_quality",
        "format": "csv",
        "output_path": os.path.relpath(AIR_QUALITY_UPDATE_PATH),
        "original_row_count": raw_count,
        "sites_covered": len(template_rows),
        "new_hours_per_site": NEW_HOURS,
        "new_records": len(new_rows),
        "duplicate_records": 0,
        "schema_changes": [
            {
                "column": "aqi",
                "type": "int",
                "change": "added",
                "value_range": [0, 500],
                "description": "Air Quality Index derived from the "
                               "sample_measurement (PM2.5) via EPA breakpoints.",
            }
        ],
        "notes": (
            "For each monitoring site we take the most recent original row as "
            "a template, shift its timestamps forward by 1..NEW_HOURS hours, "
            "and perturb the PM2.5 measurement. The new aqi column is derived "
            "from the perturbed measurement."
        ),
    }

    spark.stop()
    return manifest


if __name__ == "__main__":
    m = generate()
    print("air_quality update generated:")
    for k, v in m.items():
        print(f"  {k}: {v}")
