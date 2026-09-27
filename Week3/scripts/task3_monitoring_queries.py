from common import get_spark
from monitoring import MONITORING_PATH

spark = get_spark("Task3 monitoring queries")
spark.read.format("delta").load(MONITORING_PATH).createOrReplaceTempView("monitoring")

validation_failures = spark.sql(
    """
        select 
            dataset,
            count_if(validation_failures > 0) as runs_with_failures,
            sum(validation_failures) as total_validation_failures
        from 
            monitoring
        group by
            dataset
        order by
            runs_with_failures desc,
            total_validation_failures desc
"""
)

validation_failures.show(truncate=False)

processing_time = spark.sql(
    """
        select 
            dataset,
            round(avg(execution_time_s), 2) as avg_time_s,
            max(execution_time_s) as max_time_s
        from
            monitoring
        group by
            dataset
        order by
            avg_time_s desc
"""
)

processing_time.show(truncate=False)

processed_records = spark.sql(
    """
        select 
            executed_at,
            dataset,
            processed_records, 
            rejected_records
        from
            monitoring
        order by
            executed_at
"""
)

processed_records.show(truncate=False)


execution_change = spark.sql(
    """
        select 
            dataset,
            executed_at,
            execution_time_s,
            execution_time_s - lag(execution_time_s) over (partition by dataset order by executed_at) as change_s
        from
            monitoring
        order by
            dataset,
            executed_at
"""
)

execution_change.show(truncate=False)