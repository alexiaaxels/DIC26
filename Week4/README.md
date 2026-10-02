
# Requirements

To run the project, you will need:

- Java 17
- Python 3
- PySpark 4.0.4
- Delta Lake 4.0.0
- the Delta tables created in Week 1
- NumPy 2.x

Use the same virtual environment created for Week 1.

# Running the code
The platform consists of several python scripts located in `Week4/`. Navigate to this folder before running the commands below.

Note: On Windows systems, you might have to use `python` instead of `python3` to invoke the installed Python version.

## Feature Analysis and Training Dataset Design

`python3 task1_feature_selection.py`

The output provides statistical information about each candidate feature to help determine which features are relevant for the prediction problem.

### Understanding the output:
The output is an analysis of the candidate features, not an automatic feature-selection result. The statistics are used to make the final feature-selection decisions manually.
- categorical features: shows the number of records in each category and the corresponding mean and standard deviation of the target
- datetime features: converts each datetime into components such as hour, day of week, and month, then shows how the target varies across those values
- numerical features (ordered by correlation): calculates their correlation with fare_amount, allowing features with stronger relationships to be identified

## Generate training dataset
`python3 task2_training_dataset.py`

Generates training, validation and test datasets. The output prints the number of rows in each for verification.
