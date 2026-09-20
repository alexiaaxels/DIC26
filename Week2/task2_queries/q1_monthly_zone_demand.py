import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import get_spark, register_integrated

def run(spark):
    result = spark.sql("""
        select
            date_format(pickup_date, 'yyyy-MM') as pickup_month,
            pickup_zone,
            count(*) as num_trips
        from integrated_taxi_trips
        group by pickup_month, pickup_zone
        order by num_trips desc, pickup_month, pickup_zone
    """)
    return result

if __name__ == "__main__":
    spark = get_spark("q1_monthly_zone_demand")
    register_integrated(spark)

    run(spark).show(truncate=False)

    spark.stop()
