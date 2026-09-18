from common import get_spark, register_integrated, AIR_QUALITY_CATEGORIES

spark = get_spark("task4_data_products")
register_integrated(spark)

# Daily Mobility Summary


# Taxi Zone Statistics

taxi_zone_stats = spark.sql(
    """
        select
            pickup_zone,
            count(*) as trips,
            round(count(*) / count(distinct pickup_date), 2) as avg_trip_per_day,
            round(avg(trip_distance), 2) as avg_trip_distance,
            round(avg(timestampdiff(minute, pickup_time_local, dropoff_time_local)), 2) as avg_trip_duration,
            round(avg(fare_amount), 2) as avg_fare_amount,
            mode(dropoff_zone) as most_common_dropoff_zone,
            mode(hour(pickup_time_local)) as busiest_hour,
            mode(date_format(pickup_time_local, 'EEEE')) as busies_week_day
        from
            integrated_taxi_trips
        group by
            pickup_zone
        order by
            trips desc
"""
)

taxi_zone_stats.show(truncate=False)

taxi_zone_stats.write.format("delta") \
        .mode("overwrite") \
        .save("week2_delta/taxi_zone_stats")

# Weather Impact Summary


# Air Quality Impact Summary

air_quality_impact_summary = spark.sql(
    f"""
        select
            {AIR_QUALITY_CATEGORIES} as air_quality_category,
            count(*) as trips,
            count(distinct time_utc) as total_hours,
            round(count(*) / count(distinct time_utc), 2) as avg_trips_per_hour,
            round(avg(trip_distance), 2) as avg_trip_distance,
            mode(hour(pickup_time_local)) as most_common_hour
        from
            integrated_taxi_trips
        group by 
            air_quality_category
        order by
            avg_trips_per_hour desc
"""
)

air_quality_impact_summary.show(truncate=False)

air_quality_impact_summary.write.format("delta") \
        .mode("overwrite") \
        .save("week2_delta/air_quality_impact_summary")


# Borough Mobility Summary
