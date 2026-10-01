from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, Imputer

from common import TARGET, register_integrated, get_spark

FEATURES_CAT = [
    "state_code",
    "county_code",

    "pickup_zone",
    "pickup_borough",
    "dropoff_zone",
    "dropoff_borough",
]

FEATURES_NUM = [
    "trip_distance",
    "passenger_count",
    "pickup_air_quality_pm25",
    "pickup_weather_temperature"
]

FEATURES_DATE = [
    "pickup_time_local",
    "dropoff_time_local"
]

def generate_training_data(df, target, features_cat, features_date, features_num, train_ratio=0.7, validation_ratio=0.15, seed=42):
    result = df

    # Remove rows where the target is missing
    result = result.filter(F.col(target).isNotNull())

    # prepare datetime features
    date_features = []

    for feature in features_date:
        result = (
            result
            .withColumn(f"{feature}_hour", F.hour(F.col(feature)))
            .withColumn(f"{feature}_day_of_week", F.dayofweek(F.col(feature)))
            .withColumn(f"{feature}_month", F.month(F.col(feature)))
        )

        date_features.extend([
            f"{feature}_hour",
            f"{feature}_day_of_week",
            f"{feature}_month"
        ])

    # split
    train, validation, test = result.randomSplit(
        [
            train_ratio,
            validation_ratio,
            1.0 - train_ratio - validation_ratio
        ],
        seed=seed
    )

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


    # build the pipeline
    pipeline = Pipeline(
        stages=(
            [imputer]
            + [numeric_assembler]
            + [scaler]
            + indexers
            + [final_assembler]
        )
    )

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


if __name__=="__main__":
    spark = get_spark("training_dataset")
    integrated_df = register_integrated(spark)

    train, validation, test = generate_training_data(integrated_df, TARGET, FEATURES_CAT, FEATURES_DATE, FEATURES_NUM)