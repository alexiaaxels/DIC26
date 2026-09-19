from common import get_spark, register_integrated

def run(spark):
# TODO: design decision, should display the busiest 3 hours, or 5, or sth else?

    result = spark.sql("""
        select * 
        from 
        (select grouped.dow, grouped.hour, grouped.trips, Rank()
            over (Partition BY grouped.dow order by grouped.trips desc) as rank
        from 
        (select 
            date_format(pickup_time_local, 'EEEE') as dow,
            hour(pickup_time_local) as hour,
            count(*) as trips
            
        from integrated_taxi_trips
        group by dow, hour
        ) as grouped
        ) rs where rank <= 3

    """)

    return result

if __name__ == "__main__":
    spark = get_spark("q5_dow_peak_hours")
    register_integrated(spark)

    run(spark).show(truncate=False)

    spark.stop()
