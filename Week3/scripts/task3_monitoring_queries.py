from common import get_spark
from monitoring import MONITORING_PATH
from quarantine import VALIDATION_METRICS_PATH

spark = get_spark("Task3 monitoring queries")
spark.read.format("delta").load(MONITORING_PATH).createOrReplaceTempView("monitoring")

try:
    spark.read.format("delta").load(VALIDATION_METRICS_PATH) \
        .createOrReplaceTempView("validation_metrics")
    _has_validation = True
except Exception:
    _has_validation = False

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

if not _has_validation:
    print(
        "\n[info] validation_metrics table not found — run the incremental "
        "pipeline at least once with the Task 4 framework to populate it."
    )
else:
    print("\n--- Which rule fails most often (per dataset) ---")
    spark.sql(
        """
            select
                dataset,
                rule_id,
                rule_type,
                sum(rows_failed) as total_failed,
                sum(rows_in)     as total_checked,
                round(sum(rows_failed) / nullif(sum(rows_in), 0) * 100, 2)
                    as fail_pct
            from validation_metrics
            group by dataset, rule_id, rule_type
            having sum(rows_failed) > 0
            order by dataset, total_failed desc
        """
    ).show(50, truncate=False)

    print("\n--- Rule-type breakdown across all runs ---")
    spark.sql(
        """
            select
                rule_type,
                count(distinct rule_id) as distinct_rules,
                sum(rows_failed)        as total_failed,
                round(avg(elapsed_s), 3) as avg_elapsed_s
            from validation_metrics
            group by rule_type
            order by total_failed desc
        """
    ).show(truncate=False)

    print("\n--- Hard schema-contract failures ---")
    spark.sql(
        """
            select
                run_id,
                dataset,
                executed_at,
                rule_id
            from validation_metrics
            where hard_failure = true
            order by executed_at desc
        """
    ).show(truncate=False)