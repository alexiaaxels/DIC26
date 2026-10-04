import os
import time


from common import REPO_ROOT, build_from_raw, get_spark
from task3_ml_pipeline import latest_delta_version, load_data, train_and_evaluate

def build_from_platform(spark):
    return load_data(spark, latest_delta_version(spark))

def run(spark, name, build):
    start = time.perf_counter()
    df = build(spark).cache()
    rows = df.count()
    prep_time = time.perf_counter() - start

    start = time.perf_counter()
    _, metrics = train_and_evaluate(df)
    train_time = time.perf_counter() - start

    print(f"{name}: rows={rows}, preprocessing={prep_time:.1f}s, training={train_time:.1f}s, metrics={metrics}")
    df.unpersist()

if __name__ == "__main__":
    spark = get_spark("task4_compare")
    run(spark, "Approach A - raw files", build_from_raw)
    run(spark, "Approach B - integrated platform", build_from_platform)
    spark.stop()