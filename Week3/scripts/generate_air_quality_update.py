from __future__ import annotations

import os

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    expr,
    row_number,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from common import (
    RANDOM_SEED,
    AIR_QUALITY_RAW_PATH,
    AIR_QUALITY_UPDATE_PATH,
    ensure_updates_dir,
    get_spark,
)

NEW_HOURS = 24 * 7


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


def _find_col(raw_columns: list[str], lower: str) -> str:
    for c in raw_columns:
        if c.strip().lower().replace(" ", "_") == lower:
            return c
    raise KeyError(f"Column {lower!r} not present in raw file. "
                   f"Available: {raw_columns}")


_AQI_SQL = """
    CASE
        WHEN sample_measurement IS NULL THEN NULL
        WHEN sample_measurement <= 9.0   THEN CAST(round(((50.0  -  0.0) / (9.0    -  0.0 ) * (sample_measurement -  0.0 )) +   0) AS INT)
        WHEN sample_measurement <= 35.4  THEN CAST(round(((100.0 - 51.0) / (35.4   -  9.1 ) * (sample_measurement -  9.1 )) +  51) AS INT)
        WHEN sample_measurement <= 55.4  THEN CAST(round(((150.0 -101.0) / (55.4   - 35.5 ) * (sample_measurement - 35.5 )) + 101) AS INT)
        WHEN sample_measurement <= 125.4 THEN CAST(round(((200.0 -151.0) / (125.4  - 55.5 ) * (sample_measurement - 55.5 )) + 151) AS INT)
        WHEN sample_measurement <= 225.4 THEN CAST(round(((300.0 -201.0) / (225.4  -125.5 ) * (sample_measurement -125.5 )) + 201) AS INT)
        WHEN sample_measurement <= 500.0 THEN CAST(round(((500.0 -301.0) / (500.0  -225.5 ) * (sample_measurement -225.5 )) + 301) AS INT)
        ELSE 500
    END
"""


def generate() -> dict:
    ensure_updates_dir()
    spark = get_spark("Week3 Task1 - air_quality update generator")

    raw = _load_raw(spark)
    raw_columns = raw.columns
    raw_count = raw.count()

    c_state = _find_col(raw_columns, "state_code")
    c_county = _find_col(raw_columns, "county_code")
    c_site = _find_col(raw_columns, "site_num")
    c_poc = _find_col(raw_columns, "poc")
    c_date_gmt = _find_col(raw_columns, "date_gmt")
    c_time_gmt = _find_col(raw_columns, "time_gmt")
    c_date_local = _find_col(raw_columns, "date_local")
    c_time_local = _find_col(raw_columns, "time_local")
    c_sample = _find_col(raw_columns, "sample_measurement")

    site_partition = [c_state, c_county, c_site, c_poc]

    def _rebuild_ts(date_col: str, time_col: str, alias: str) -> Column:
        return to_timestamp(
            concat_ws(
                " ",
                date_format(col(date_col), "yyyy-MM-dd"),
                date_format(col(time_col), "HH:mm"),
            ),
            "yyyy-MM-dd HH:mm",
        ).alias(alias)

    with_ts = (
        raw
        .withColumn("_ts_gmt", _rebuild_ts(c_date_gmt, c_time_gmt, "_ts_gmt"))
        .withColumn("_ts_local", _rebuild_ts(c_date_local, c_time_local, "_ts_local"))
    )

    latest_window = Window.partitionBy(*site_partition).orderBy(col("_ts_gmt").desc())
    templates = (
        with_ts
        .withColumn("_rn", row_number().over(latest_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    offsets = spark.range(1, NEW_HOURS + 1).withColumnRenamed("id", "_offset_h")

    expanded = templates.crossJoin(offsets)

    shifted = (
        expanded
        .withColumn(
            "_new_ts_gmt",
            expr("_ts_gmt + make_interval(0, 0, 0, 0, CAST(_offset_h AS INT), 0, 0)"),
        )
        .withColumn(
            "_new_ts_local",
            expr("_ts_local + make_interval(0, 0, 0, 0, CAST(_offset_h AS INT), 0, 0)"),
        )
    )

    date_type = raw.schema[c_date_gmt].dataType
    time_type = raw.schema[c_time_gmt].dataType

    projected = (
        shifted
        .withColumn("_new_date_gmt_s",  date_format(col("_new_ts_gmt"),  "yyyy-MM-dd"))
        .withColumn("_new_time_gmt_s",  date_format(col("_new_ts_gmt"),  "HH:mm"))
        .withColumn("_new_date_local_s", date_format(col("_new_ts_local"), "yyyy-MM-dd"))
        .withColumn("_new_time_local_s", date_format(col("_new_ts_local"), "HH:mm"))
    )

    perturbed = projected.withColumn(
        "_new_sample",
        expr(
            f"greatest(0.0, round(`{c_sample}` + (rand({RANDOM_SEED}) * 6.0 - 3.0), 1))"
        ),
    )

    def _to_time_col(str_col: str):
        return to_timestamp(
            concat_ws(" ",
                      date_format(expr("current_date()"), "yyyy-MM-dd"),
                      col(str_col)),
            "yyyy-MM-dd HH:mm",
        ).cast(time_type)

    replaced = (
        perturbed
        .withColumn(c_date_gmt,   to_date(col("_new_date_gmt_s"),   "yyyy-MM-dd").cast(date_type))
        .withColumn(c_time_gmt,   _to_time_col("_new_time_gmt_s"))
        .withColumn(c_date_local, to_date(col("_new_date_local_s"), "yyyy-MM-dd").cast(date_type))
        .withColumn(c_time_local, _to_time_col("_new_time_local_s"))
        .withColumn(c_sample, col("_new_sample"))
    )

    aqi_expr = _AQI_SQL.replace("sample_measurement", f"`{c_sample}`")
    with_aqi = replaced.withColumn("aqi", expr(aqi_expr).cast(IntegerType()))

    final_columns = raw_columns + ["aqi"]
    result = with_aqi.select(*[col(c) for c in final_columns])

    tmp_out = AIR_QUALITY_UPDATE_PATH + ".tmp"
    (
        result
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

    sites_covered = templates.select(*site_partition).distinct().count()
    new_rows_count = sites_covered * NEW_HOURS

    manifest = {
        "dataset": "air_quality",
        "format": "csv",
        "output_path": os.path.relpath(AIR_QUALITY_UPDATE_PATH),
        "original_row_count": raw_count,
        "sites_covered": sites_covered,
        "new_hours_per_site": NEW_HOURS,
        "new_records": new_rows_count,
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
            "For every monitoring site we take its most recent original row "
            "as a template, shift its GMT and local timestamps forward by "
            "1..NEW_HOURS hours, and perturb the PM2.5 measurement by a "
            "Uniform(-3, +3) noise term. The new aqi column is derived from "
            "the perturbed measurement via the EPA PM2.5 breakpoint table."
        ),
    }

    spark.stop()
    return manifest


if __name__ == "__main__":
    m = generate()
    print("air_quality update generated:")
    for k, v in m.items():
        print(f"  {k}: {v}")
