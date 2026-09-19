from common import get_spark, register_integrated, AIR_QUALITY_CATEGORIES

def run(spark):
# # 3. Relationship between air quality and taxi demand.

    result = spark.sql(
        f"""
        select
            {AIR_QUALITY_CATEGORIES} as air_quality_category,
            count(*) as trips,
            count(distinct time_utc) as hours_observed,
            round(count(*) / count(distinct time_utc), 2) as avg_trips_per_hour
        from
            integrated_taxi_trips
        group by
            air_quality_category
        order by
            avg_trips_per_hour desc 
    """
    )

    return result

if __name__ == "__main__":
    spark = get_spark("q3_demand_by_air_quality")
    register_integrated(spark)

    run(spark).show(truncate=False)

    spark.stop()

