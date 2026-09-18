from common import get_spark, register_integrated, register_coco_labels

spark = get_spark("q4_weather_and_zone_variation")
register_integrated(spark)
register_coco_labels(spark)

# # 4. Taxi zones with the largest variation in demand under different weather conditions.

zone_weather_variation = spark.sql(
    """
        select
            pickup_zone,
            l.weather_bucket as weather_condition,
            count(*) as trips
        from 
            integrated_taxi_trips t
            left join raw_coco_labels r on t.pickup_weather_condition_code = r.coco
            left join coco_buckets l on r.weather_condition = l.weather_condition
        group by
            pickup_zone, l.weather_bucket
"""
)

zone_weather_variation.createOrReplaceTempView("zone_weather_demand")

result = spark.sql(
    """
        select
            pickup_zone,
            round(avg(trips), 2) as avg_trips_per_condition,
            round(stddev(trips), 2) as stddev_trips,
            round((stddev(trips) / avg(trips))*100, 2) as coeff_of_variation
        from
            zone_weather_demand
        group by
            pickup_zone
        order by
            coeff_of_variation desc
"""
)

result.show(truncate=False)

spark.stop()
