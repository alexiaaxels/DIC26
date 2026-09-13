A README describing how to run the platform.

Specify versions and such?
# Requirements
To run the project, you will need:
- Java 17
- Python 3
- PySpark 4.0.4
- Delta Lake 4.0.0

We recommend using a Python virtual environment such as `.venv`.

# Run the platform
The project consists of several Python scripts, with each script corresponding to a separate task. These should be ran in the order specified below. On Windows systems, you might have to use `python` instead of `python3` to invoke the installed Python version.

## Run ingestion:
`python3 task3.py`
Produces: 4 Delta tables
- air_quality (`delta/air_quality`)
- taxi_trips (`delta/taxi_trips`)
- taxi_zone (`delta/taxi_zone`)
- weather (`delta/weather`)
Also produces ingestion metadata (`delta/_ingestion_metadata`)

## Run integration pipeline:
`python3 task5_integration.py`
Produces: 1 Delta table: integrated_taxi_trips (`delta/integrated_taxi_trips`)

## Run benchmarks:
`python3 task6_benchmark.py`
