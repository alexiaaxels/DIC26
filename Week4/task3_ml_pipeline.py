import sys
import json
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, Imputer, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from common import TARGET, INTEGRATED_TAXI_TRIPS_PATH, SCRIPT_DIR, get_spark
from task2_training_dataset import FEATURES_CAT, FEATURES_NUM, FEATURES_DATE

SEED = 42
MODELS_DIR = os.path.join(SCRIPT_DIR, "models")
GBT_PARAMS = {"maxIter": 20, "maxBins": 300, "seed": SEED}

def latest_delta_version(spark):
    history = spark.sql(f"DESCRIBE HISTORY delta.`{INTEGRATED_TAXI_TRIPS_PATH}`")
    return history.agg(F.max("version")).first()[0]

def load_data(spark, version):
    df = (spark.read.format("delta").option("versionAsOf", version).load(INTEGRATED_TAXI_TRIPS_PATH))
    return df.filter(F.col(TARGET).isNotNull())

def build_pipeline():
    date_exprs = ", ".join(
        f"hour({c}) AS {c}_hour, dayofweek({c}) AS {c}_day_of_week, month({c}) AS {c}_month" for c in FEATURES_DATE)

    date_cols = [f"{c}_{s}" for c in FEATURES_DATE for s in ("hour", "day_of_week", "month")]

    ## fills in the gaps with the median for rows that have missing data
    imputed = [f"{c}_imputed" for c in FEATURES_NUM]

    stages = [
        SQLTransformer(statement=f"SELECT *, {date_exprs} FROM __THIS__"),
        Imputer(inputCols=FEATURES_NUM, outputCols=imputed, strategy="median"),
        ## puts all the info into one vector/list instead of columns
        VectorAssembler(inputCols=imputed, outputCol="numeric_features", handleInvalid="keep"),
        StandardScaler(inputCol="numeric_features", outputCol="scaled_numeric_features"),
    ]

    stages += [
        StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in FEATURES_CAT
    ]

    stages += [
        VectorAssembler(
            inputCols=["scaled_numeric_features"] + [f"{c}_index" for c in FEATURES_CAT] + date_cols, 
            outputCol="features", handleInvalid="keep",
        ),
        GBTRegressor(labelCol=TARGET, featuresCol="features", **GBT_PARAMS),
    ]
    return Pipeline(stages=stages)

def train_and_evaluate(df):
    ## We train it on 80% of data and then predict for the 20% that the model hasn't seen
    train, test = df.randomSplit([0.8, 0.2], seed=SEED)

    model = build_pipeline().fit(train)

    predictions = model.transform(test)
    metrics = {}
    ## here we give the model a grade for the prediciton. MAE is used for typical errors, RMSE is used for the big mistakes, and R2 is an overall score.
    for metric in ("rmse", "mae", "r2"):
        metrics[metric] = round(RegressionEvaluator(labelCol=TARGET, metricName=metric).evaluate(predictions), 4)
        print(f"{metric}: {metrics[metric]:.4f}")

    return model, metrics

def save_model(model, version, metrics):
    path = os.path.join(MODELS_DIR, f"fare_model_v{version}")
    model.write().overwrite().save(path)

    metadata = {
        "delta_version": version,
        "target": TARGET,
        "features": {"categorical": FEATURES_CAT, "numeric": FEATURES_NUM, "date": FEATURES_DATE},
        "seed": SEED,
        "train_test_split": [0.8, 0.2],
        "model": GBT_PARAMS,
        "test_metrics": metrics,
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(os.path.join(path, "training_info.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    return path

if __name__ == "__main__":
    spark = get_spark("task3_ml_pipeline")

    version = int(sys.argv[1]) if len(sys.argv) > 1 else latest_delta_version(spark)
    print(f"Training on delta version {version}")
    
    df = load_data(spark, version)
    model, metrics = train_and_evaluate(df)

    path = save_model(model, version, metrics)
    print(f"Model saved to {path}")
    spark.stop()