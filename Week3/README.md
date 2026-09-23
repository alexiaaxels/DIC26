
# Requirements

To run the project, you will need:

- Java 17
- Python 3
- PySpark 4.0.4
- Delta Lake 4.0.0
- the Delta tables created in Week 1

Use the same virtual environment created for Week 1.

## Task 1 

# Generate incremental update datasets

To run all generators:
`python3 scripts/task1_generate_updates.py`

To run a subset:
`python3 scripts/task1_generate_updates.py taxi_trips`
`python3 scripts/task1_generate_updates.py weather air_quality`



# Apply the update files to the Delta tables
After generating the update files, run the incremental pipeline.

To run the pipeline on all datasets:
`python3 scripts/task1_incremental_update.py`

To run a subset:
`python3 scripts/task1_incremental_update.py taxi_trips`
`python3 scripts/task1_incremental_update.py weather air_quality`

