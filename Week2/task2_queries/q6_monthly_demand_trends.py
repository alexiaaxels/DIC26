import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import get_spark, register_integrated

def run(spark):
    result = spark.sql("""
        SELECT
            DATE_FORMAT(pickup_date, 'yyyy-MM') AS pickup_month,
            COUNT(*) AS total_trips,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
            ROUND(AVG(passenger_count), 2) AS avg_passengers,
            ROUND(
                AVG(
                    TIMESTAMPDIFF(
                        MINUTE,
                        pickup_time_local,
                        dropoff_time_local
                    )
                ), 2
            ) AS avg_trip_duration_minutes,
            SUM(
                CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END
            ) AS airport_trips,
            ROUND(
                100.0 * SUM(
                    CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END
                ) / COUNT(*), 2
            ) AS airport_trip_percentage,
            ROUND(SUM(total_amount), 2) AS total_revenue

        FROM integrated_taxi_trips

        WHERE pickup_date >= '2024-01-01'
        AND pickup_date < '2024-04-01'

        GROUP BY DATE_FORMAT(pickup_date, 'yyyy-MM')

        ORDER BY pickup_month
    """)
    return result

def run_simple(spark):
    result = spark.sql("""
        SELECT
            DATE_FORMAT(pickup_date, 'yyyy-MM') AS pickup_month,
            COUNT(*) AS total_trips,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
            ROUND(AVG(passenger_count), 2) AS avg_passengers,
            ROUND(
                AVG(
                    TIMESTAMPDIFF(
                        MINUTE,
                        pickup_time_local,
                        dropoff_time_local
                    )
                ), 2
            ) AS avg_trip_duration_minutes,
            SUM(
                CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END
            ) AS airport_trips,
            ROUND(
                100.0 * SUM(
                    CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END
                ) / COUNT(*), 2
            ) AS airport_trip_percentage,
            ROUND(SUM(total_amount), 2) AS total_revenue

        FROM integrated_taxi_trips

        WHERE pickup_time_local >= '2024-01-01'
        AND pickup_time_local < '2024-04-01'

        GROUP BY DATE_FORMAT(pickup_date, 'yyyy-MM')

        ORDER BY pickup_month
    """)
    return result

if __name__ == "__main__":
    spark = get_spark("q6_monthly_demand_trends")
    register_integrated(spark)

    run(spark).show(truncate=False)
    
    spark.stop()
