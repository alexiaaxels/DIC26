from common import get_spark, register_integrated

spark = get_spark("q1_monthly_zone_demand")
register_integrated(spark)

result = spark.sql("""
    select
        date_format(pickup_date, 'yyyy-MM') as pickup_month,
        pickup_zone,
        count(*) as num_trips
    from integrated_taxi_trips
    group by pickup_month, pickup_zone
    order by num_trips desc
""") 

result.show(truncate=False)

spark.stop()
