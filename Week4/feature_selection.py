import sys
import os

from pyspark.sql import functions as F

from common import TARGET, register_integrated, get_spark

CATEGORICAL_CANDIDATES = [
    "state_code",
    "county_code",

    "pickup_zone",
    "pickup_borough",
    "dropoff_zone",
    "dropoff_borough",

]

DATETIME_CANDIDATES = [
    "time_utc",
    "pickup_time_local",
    "dropoff_time_local",
    "pickup_time_utc",
    "dropoff_time_utc",
]

IGNORE = [
    "total_amount",
    "tip amount",
    "tolls_amount",
    "mta_tax",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "tip_amount",
    "extra",

    "pickup_date",
    "borough",
    "zone",
    "service_zone",
    "location_id",

    
    "vendor_id",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
]

def get_numeric_candidates(df):
    
    numeric_types = {
        "byte",
        "short",
        "int",
        "long",
        "float",
        "double",
        "decimal",
        "bigint"
    }

    return [
        column
        for column, dtype in df.dtypes
        if dtype.split("(")[0] in numeric_types
        and column not in CATEGORICAL_CANDIDATES
        and column not in DATETIME_CANDIDATES
        and column not in IGNORE
        and column != TARGET
    ]

def analyze_categorical_candidates(df,candidates):
    results = {}
    for cand in candidates:
        result = (
            df.groupBy(cand)
            .agg(
                F.count("*").alias("count"),
                F.avg(TARGET).alias("mean_fare"),
                F.stddev(TARGET).alias("std_fare")
            )
            .orderBy(
                F.desc("count")
            )
        )
        results[cand] = result

    return results


def analyze_datetime_features(df, candidates):
    results = {}

    for cand in candidates:

        temp_df = (
            df.withColumn(
                "hour",
                F.hour(F.col(cand))
            )
            .withColumn(
                "day_of_week",
                F.dayofweek(F.col(cand))
            )
            .withColumn(
                "month",
                F.month(F.col(cand))
            )
            .withColumn(
                "day",
                F.dayofmonth(F.col(cand))
            )
        )

        result = (
            temp_df.groupBy("hour")
            .agg(
                F.count("*").alias("count"),
                F.avg(TARGET).alias("mean_fare"),
                F.stddev(TARGET).alias("std_fare")
            )
            .orderBy("hour")
        )

        results[cand] = result

    return results


def analyze_numeric_candidates(df, candidates):
    results = []

    for cand in candidates:
        correlation = df.stat.corr(
            cand,
            TARGET
        )
        results.append(
            (cand, correlation)
        )

    return df.sparkSession.createDataFrame(
        results,
        ["candidate", "correlation"]
    ).orderBy(
        F.abs(F.col("correlation")).desc()
    )



if __name__ == "__main__":
    spark = get_spark("feature_selection")
    integrated_df = register_integrated(spark)

    NUMERIC_CANDIDATES = get_numeric_candidates(integrated_df)

    for column, dtype in integrated_df.dtypes:
        if column == "passenger_count":
            print("FOUND:", column, repr(dtype))

    cat_results = analyze_categorical_candidates(integrated_df, CATEGORICAL_CANDIDATES)
    for cand, result in cat_results.items():
        print(f"\n========== {cand} ==========")
        result.show(truncate=False)

    date_results = analyze_datetime_features(integrated_df, DATETIME_CANDIDATES)
    for cand, result in date_results.items():

        print(f"\n========== {cand} ==========")

        result.show(truncate=False)

    num_results = analyze_numeric_candidates(integrated_df,NUMERIC_CANDIDATES)
    num_results.show(truncate=False)