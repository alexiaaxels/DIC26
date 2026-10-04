from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, Imputer, SQLTransformer

import sys

from common import TARGET, build_from_raw, register_integrated, get_spark, register_weather, register_air_quality, register_taxi_trips, register_taxi_zones

FEATURES_CAT = [
    #"state_code",
    #"county_code",

    "pickup_zone",
    #"pickup_borough",
    "dropoff_zone",
    #"dropoff_borough",
]

FEATURES_NUM = [
    "trip_distance",
    "passenger_count",
    "pickup_air_quality_pm25", #surprisingly high correlation, should probably investigate further but we keep for now
    "pickup_weather_temperature"
]

FEATURES_DATE = [
    "pickup_time_local",
    #"dropoff_time_local" reason: more meaningful to predict knowing the pickup time only
]

def generate_training_data(df, target, features_cat, features_date, features_num, train_ratio=0.7, validation_ratio=0.15, seed=42):
    # Remove rows where the target is missing
    result = df.filter(F.col(target).isNotNull())

    # split
    train, validation, test = result.randomSplit(
        [
            train_ratio,
            validation_ratio,
            1.0 - train_ratio - validation_ratio
        ],
        seed=seed
    )
    stages = build_feature_pipeline_stages(features_cat, features_num, features_date)
    pipeline = Pipeline(stages = stages)

    # fit preprocessing only on training data
    pipeline_model = pipeline.fit(train)

    # apply identical transformations to all three sets
    train = pipeline_model.transform(train)
    validation = pipeline_model.transform(validation)
    test = pipeline_model.transform(test)

    train = train.select(target, "features")
    validation = validation.select(target, "features")
    test = test.select(target, "features")

    return train, validation, test

def build_feature_pipeline_stages(features_cat, features_num, features_date):
    date_exprs = ", ".join(
        f"hour({c}) AS {c}_hour, dayofweek({c}) AS {c}_day_of_week, month({c}) AS {c}_month"
        for c in features_date
    )
    date_stage = SQLTransformer(statement=f"SELECT *, {date_exprs} FROM __THIS__")

    date_features = [
        f"{c}_{suffix}"
        for c in features_date
        for suffix in ("hour", "day_of_week", "month")
    ]

    # handle missing numerical values
    imputed_num = [
        f"{feature}_imputed"
        for feature in features_num
    ]

    imputer = Imputer(
        inputCols=features_num,
        outputCols=imputed_num,
        strategy="median"
    )

    # scale numerical
    numeric_assembler = VectorAssembler(
        inputCols=imputed_num,
        outputCol="numeric_features",
        handleInvalid="keep"
    )

    scaler = StandardScaler(
        inputCol="numeric_features",
        outputCol="scaled_numeric_features",
        withMean=False,
        withStd=True
    )

    # encode categorical features
    indexers = [
        StringIndexer(
            inputCol=feature,
            outputCol=f"{feature}_index",
            handleInvalid="keep"
        )
        for feature in features_cat
    ]

    categorical_output = [
        f"{feature}_index"
        for feature in features_cat
    ]

    # combine all
    final_feature_columns = (
        ["scaled_numeric_features"]
        + categorical_output
        + date_features
    )

    final_assembler = VectorAssembler(
        inputCols=final_feature_columns,
        outputCol="features",
        handleInvalid="keep"
    )

    return [date_stage] + [imputer] + [numeric_assembler]  + [scaler]  + indexers + [final_assembler]


def generate_datasets(strategy):
    if strategy == "integrated":
        integrated_df = register_integrated(spark)
        train, validation, test = generate_training_data(integrated_df, TARGET, FEATURES_CAT, FEATURES_DATE, FEATURES_NUM)

    elif strategy == "raw":
        new_df = build_from_raw(spark)
        train, validation, test = generate_training_data(new_df, TARGET, FEATURES_CAT, FEATURES_DATE, FEATURES_NUM)
    else:
        print("Please specify \"raw\" or \"integrated\" as a strategy.")
        spark.stop()
        sys.exit()

    return train, validation, test  

if __name__=="__main__":
    spark = get_spark("training_dataset_generation")
    try:
        strategy = sys.argv[1]
    except:
        print("Please specify \"raw\" or \"integrated\" as a strategy.")
        spark.stop()
        sys.exit()

    train, validation, test = generate_datasets(strategy)
    
    print("Training dataset generation completed.")
    print(f"Train rows: {train.count()}")
    print(f"Validation rows: {validation.count()}")
    print(f"Test rows: {test.count()}")

    spark.stop()