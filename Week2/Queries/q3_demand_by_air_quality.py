from common import get_spark, register_integrated

spark = get_spark("q3_demand_by_air_quality")
register_integrated(spark)

# # 3. Relationship between air quality and taxi demand.

demand_by_air_quality = spark.sql(
    """
    SELECT
        CASE
            WHEN pickup_air_quality_pm25 IS NULL THEN 'Unknown'
            WHEN pickup_air_quality_pm25 <= 9.0 THEN 'Good'
            WHEN pickup_air_quality_pm25 <= 35.4 THEN 'Moderate'
            WHEN pickup_air_quality_pm25 <= 55.4 THEN 'Unhealthy for Sensitive'
            WHEN pickup_air_quality_pm25 <= 125.4 THEN 'Unhealthy'
            WHEN pickup_air_quality_pm25 <= 225.4 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END AS air_quality_category,
        COUNT(*) AS trips,
        COUNT(DISTINCT time_utc) AS hours_observed,
        ROUND(COUNT(*) / COUNT(DISTINCT time_utc), 2) AS avg_trips_per_hour
    FROM
        integrated_taxi_trips
    GROUP BY
        air_quality_category
    ORDER BY
        avg_trips_per_hour DESC 
"""
)

demand_by_air_quality.show(truncate=False)