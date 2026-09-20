from common import get_spark, register_integrated, AIR_QUALITY_CATEGORIES, create_metadata, create_dt, data_product_metadata

spark = get_spark("task4_data_products")
register_integrated(spark)

metadata_rows = []

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

data_product_metadata(spark, metadata_rows, "taxi_zone_stats", "integrated_taxi_trips", "1.0")

create_dt(taxi_zone_stats, "taxi_zone_stats")

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

data_product_metadata(spark, metadata_rows, "air_quality_impact_summary", "integrated_taxi_trips", "1.0")

create_dt(air_quality_impact_summary, "air_quality_impact_summary")


# Borough Mobility Summary

borough_mobility_summary = spark.sql(
    """
        select
            pickup_borough,
            count(*) as trips,
            mode(dropoff_borough) as most_common_dropoff_borough,
            mode(pickup_zone) as most_common_pickup_borough,
            mode(dropoff_zone) as most_common_dropoff_zone,
            mode(hour(pickup_time_local)) as most_common_hour,
            round(avg(passenger_count), 2) as avg_passenger_count_per_trip,
            mode(date_format(pickup_time_local, 'EEEE')) as busies_week_day
        from
            integrated_taxi_trips
        group by
            pickup_borough
        order by
            trips desc
"""
)

data_product_metadata(spark, metadata_rows, "borough_mobility_summary", "integrated_taxi_trips", "1.0")

create_dt(borough_mobility_summary, "borough_mobility_summary")

# Weekday Mobility Summary
weekday_mobility_summary = spark.sql(
    """
        select
            date_format(pickup_time_local, 'EEEE') as week_day,
            count(*) as trips,
            mode(hour(pickup_time_local)) as busiest_hour,
            mode(pickup_zone) as most_common_pickup_zone,
            round(avg(fare_amount), 2) as avg_fare_amount,
            round(avg(timestampdiff(minute, pickup_time_local, dropoff_time_local)), 2) as avg_trip_duration
        from
            integrated_taxi_trips
        group by
            week_day
        order by
            trips desc
"""
)

data_product_metadata(spark, metadata_rows, "weekday_mobility_summary", "integrated_taxi_trips", "1.0")

create_metadata(spark, metadata_rows)
create_dt(weekday_mobility_summary, "weekday_mobility_summary")
