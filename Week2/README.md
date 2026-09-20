A README describing how to run the platform.

# Requirements
To run the project, you will need:
- Java 17
- Python 3
- PySpark 4.0.4
- Delta Lake 4.0.0
- the Delta tables created in Week 1

We recommend using a Python virtual environment such as `.venv`.

# Run the platform
The platform consists of several python scripts located in `Week2/`. Before running the command below navigate to this folder.

Note: On Windows systems, you might have to use `python` instead of `python3` to invoke the installed Python version.

## The six analytical queries
Each query can be run independently:

`python3 task2_queries/q1_monthly_zone_demand.py`

`python3 task2_queries/q2_distance_by_weather.py`

...and similarly for q3-q6

Each script executes the query against the integrated Delta dataset and prints the resulting table to the console.

## The analytical data products
The analytical data products can be generated using the data-product generation script in `Week2/`:

`python3 task4_data_products.py`

This script generates four reusable analytical data products (`taxi_zone_stats`, `air_quality_impact_summary`, `borough_mobility_summary`, and `weekday_mobility_summary`) and stores them as Delta tables.
Metadata describing the data products is also generated. The outputs can be found in `Week2/delta`.

## The benchmark experiments
The four optimization experiments can be reproduced with:

`python3 task3_optimizations.py`

The script benchmarks caching, partition pruning, broadcast joins and Adaptive Query Execution (AQE).
Execution times, result comparisons, and relevant Spark execution plans are printed to the console.
