import time

from common import get_spark, register_integrated

from q1_monthly_zone_demand import run as query1
from q2_distance_by_weather import run as query2
from q2_distance_by_weather import run_broadcast as query2_broadcast
from q3_demand_by_air_quality import run as query3
from q4_weather_and_zone_variation import run as query4
from q5_dow_peak_hours import run as query5
from q6_monthly_demand_trends import run as query6
from q6_monthly_demand_trends import run_simple as query6_simple

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
    print("\n")

    ##RUN CACHED
    spark.catalog.cacheTable("integrated_taxi_trips")
    spark.sql("""
        SELECT COUNT(*)
        FROM integrated_taxi_trips
    """).collect()

    cached_results, cached_time = run_all_queries(spark)

    ##VERIFICATION

    if uncached_results == cached_results:
        print("\nResults are identical.")

    else:
        print("\nWARNING: Results differ!")

    
    print(f"\nTotal execution time without caching: " f"{uncached_time:.2f} seconds")
    print(f"\nTotal execution time with caching: " f"{cached_time:.2f} seconds")


    print("\n===OPTIMIZATION STRATEGY 2: PARTITION PRUNING===")


    ##RUN NON-PRUNED

    #warmup
    spark.catalog.clearCache()
    query6_simple(spark).collect()
    query6(spark).collect()

    spark.catalog.clearCache()

    start = time.perf_counter()
    result_simple = query6_simple(spark).collect()
    total_time = time.perf_counter() - start
    query6_simple(spark).explain("formatted")
    print(f"WITHOUT partition pruning: {total_time:.2f} seconds")

    ##RUN PRUNED
    spark.catalog.clearCache()

    start = time.perf_counter()
    result_pruned = query6(spark).collect()
    total_time = time.perf_counter() - start
    query6(spark).explain("formatted")
    print(f"WITH partition pruning: {total_time:.2f} seconds")

    if sorted(result_simple, key=lambda r: tuple(r)) == sorted(result_pruned, key=lambda r: tuple(r)):
        print("\nResults are identical")
    else:
        print("\nWARNING: Results differ!")


    print("\n===OPTIMIZATION STRATEGY 3: BROADCAST JOINS===")

    #disable automatic broadcasting
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    #warmup
    query2(spark).collect()
    query2_broadcast(spark).collect()

    #WITHOUT broadcast
    start = time.perf_counter()
    result_normal = query2(spark).collect()
    time_normal = time.perf_counter() - start

    #WITH broadcast
    start = time.perf_counter()
    result_broadcast = query2_broadcast(spark).collect()
    time_broadcast = time.perf_counter() - start

    print(f"\nWITHOUT broadcast: {time_normal:.2f} seconds")
    query2(spark).explain("formatted")
    print(f"\nWITH broadcast: {time_broadcast:.2f} seconds")
    query2_broadcast(spark).explain("formatted")
    if result_normal == result_broadcast:
        print("\nResults are identical.")

    else:
        print("\nWARNING: Results differ!")


    print("\n=== OPTIMIZATION STRATEGY 4: ADAPTIVE QUERY EXECUTION ===")

    #warmup
    query1(spark).collect()

    ##NON-AQE
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    query1(spark).explain("formatted")

    start = time.perf_counter()
    result_off = query1(spark).collect()
    time_off = time.perf_counter() - start

    print(f"\nAQE OFF: {time_off:.2f} seconds")

    query1(spark).explain("formatted")

    #warmup just in case
    query1(spark).collect()

    ##AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    query1(spark).explain("formatted")

    start = time.perf_counter()
    result_on = query1(spark).collect()
    time_on = time.perf_counter() - start

    print(f"\nAQE ON:  {time_on:.2f} seconds")

    query1(spark).explain("formatted")

    if result_off == result_on:
        print("\nResults are identical.")

    else:
        print("\nWARNING: Results differ!")


    spark.stop()