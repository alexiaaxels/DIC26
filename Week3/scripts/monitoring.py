import os
from datetime import datetime, timezone

from common import WEEK3_DIR

MONITORING_PATH = os.path.join(WEEK3_DIR, "delta", "monitoring")

def log_run(spark, pipeline_name, stats, schema_version):
    row = [(
        pipeline_name,
        stats["dataset"],
        datetime.now(timezone.utc),
        float(stats["execution_time_s"]),
        int(stats["processed_records"]),
        int(stats["inserted"]),
        int(stats["rejected_invalid_records"]),
        schema_version,
        int(stats["validation_failures"]),
    )]
    columns = ["pipeline_name", "dataset", "executed_at", "execution_time_s", "processed_records", "inserted_records", "rejected_records", "schema_version", "validation_failures"]
    spark.createDataFrame(row, columns).write.format("delta").mode("append").save(MONITORING_PATH)