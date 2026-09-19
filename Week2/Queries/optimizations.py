import time

from common import get_spark, register_integrated

from q1_monthly_zone_demand import run as query1
from q2_distance_by_weather import run as query2
from q3_demand_by_air_quality import run as query3
from q4_weather_and_zone_variation import run as query4
from q5_dow_peak_hours import run as query5
from q6_monthly_demand_trends import run as query6

queries = [
    ("Query 1", query1),
    ("Query 2", query2),
    ("Query 3", query3),
    ("Query 4", query4),
    ("Query 5", query5),
    ("Query 6", query6),
]

def run_all_queries(spark):
    results = []

    start = time.perf_counter()

    for name, query in queries:
        query_start = time.perf_counter()

        result = query(spark)

        # Force Spark to actually execute the query
        rows = result.collect()

        query_time = time.perf_counter() - query_start

        print(f"{name}: {query_time:.2f} seconds")

        results.append(rows)

    total_time = time.perf_counter() - start

    return results, total_time

if __name__ == "__main__":
    spark = get_spark("optimization_tests")
    register_integrated(spark)

    print("\n===OPTIMIZATION STRATEGY 1: CACHING===")

    ##RUN NON-CACHED FIRST
    spark.catalog.clearCache()
    uncached_results, uncached_time = run_all_queries(spark)


    ##RUN CACHED
    spark.catalog.cacheTable("integrated_taxi_trips")
    spark.sql("""
        SELECT COUNT(*)
        FROM integrated_taxi_trips
    """).collect()

    cached_results, cached_time = run_all_queries(spark)

    ##VERIFICATION

    if uncached_results == cached_results:
        print("Results are identical.")

    else:
        print("WARNING: Results differ!")

    
    print(f"\nTotal execution time without caching: " f"{uncached_time:.2f} seconds")
    print(f"\nTotal execution time with caching: " f"{cached_time:.2f} seconds")


    #print("\n===OPTIMIZATION STRATEGY 2: PARTITION PRUNING===")


    spark.stop()