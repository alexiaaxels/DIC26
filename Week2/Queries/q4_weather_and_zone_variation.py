from common import get_spark, register_integrated, register_coco_labels

spark = get_spark("q4_weather_and_zone_variation")
register_integrated(spark)
register_coco_labels(spark)

# # 4. Taxi zones with the largest variation in demand under different weather conditions.

zone_weather_variation = spark.sql(
    """
        SELECT
            pickup_zone,
            l.weather_bucket AS weather_condition,
            COUNT(*) AS trips
        FROM 
            integrated_taxi_trips t
            left join raw_coco_labels r on t.pickup_weather_condition_code = r.coco
            left join coco_buckets l on r.weather_condition = l.weather_condition
        GROUP BY
            pickup_zone, l.weather_bucket
"""
)

zone_weather_variation.createOrReplaceTempView("zone_weather_demand")

largest_variation = spark.sql(
    """
        SELECT
            pickup_zone,
            ROUND(AVG(trips), 2) AS avg_trips_per_condition,
            ROUND(STDDEV(trips), 2) AS stddev_trips,
            ROUND((STDDEV(trips) / AVG(trips))*100, 2) AS coeff_of_variation
        FROM
            zone_weather_demand
        GROUP BY
            pickup_zone
        ORDER BY
            coeff_of_variation DESC
"""
)

largest_variation.show(truncate=False)

spark.stop()
