from common import get_spark, register_integrated, register_coco_labels

spark = get_spark("q2_distance_by_weather")
register_integrated(spark)
register_coco_labels(spark)

# TODO: have to decide whether we want to fully categorize the conditions or group into buckets
# So display separately different kinds of rain (light, heavy, shower, freezing) or just have them all be under rain


# OPTION 1 - bucket weather conditions 
result = spark.sql("""
    select 
        l.weather_bucket as weather,
        round(avg(t.trip_distance),2) as miles,
        count(*) as trips
    from integrated_taxi_trips t
        left join raw_coco_labels r on t.pickup_weather_condition_code = r.coco
        left join coco_buckets l on r.weather_condition = l.weather_condition
    group by l.weather_bucket
    order by miles desc

""")

# OPTION 2 - display exactly as the code states
result = spark.sql("""
    select 
        coalesce(r.weather_condition, 'Unknown') as weather,
        round(avg(t.trip_distance),2) as miles,
        count(*) as trips
    from integrated_taxi_trips t
        left join raw_coco_labels r on t.pickup_weather_condition_code = r.coco
    group by weather
    order by miles desc
""")


result.show(truncate=False)

spark.stop()
