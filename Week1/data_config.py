from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import col, to_timestamp, to_utc_timestamp, date_trunc, concat_ws, make_timestamp, lit, date_format

def air_quality_timestamp(df):
    return df.withColumn(
   'time_utc', 
   date_trunc(
      'hour', 
      to_timestamp(
         concat_ws(' ', col('date_gmt').cast('string'), date_format(col('time_gmt'), 'HH:mm')),
         'yyyy-MM-dd HH:mm'
      )
   ).cast('timestamp_ntz')
)

def weather_timestamp(df):
    return df.withColumn(
   'time_utc', 
   date_trunc(
      'hour', 
      make_timestamp(col('year'), col('month'), col('day'), col('hour'), lit(0), lit(0.0))
   ).cast('timestamp_ntz'))

def taxi_trips_timestamp(df):
    df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_time_local') \
           .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_time_local')
    return (
        df.withColumn('pickup_time_utc', to_utc_timestamp(col('pickup_time_local'), 'America/New_York'))
          .withColumn('dropoff_time_utc', to_utc_timestamp(col('dropoff_time_local'), 'America/New_York'))
          .withColumn('time_utc', date_trunc('hour', col('pickup_time_utc')).cast('timestamp_ntz'))
    )

def filter_unknown_locations(df):
    return df.filter(~col('pu_location_id').isin(264, 265) & ~col('do_location_id').isin(264, 265))

DATASET_CONFIGS = {
    'taxi_trips': {
        'path': ['data/yellow_tripdata_2024-01.parquet', 'data/yellow_tripdata_2024-02.parquet', 'data/yellow_tripdata_2024-03.parquet'],
        'format': 'parquet',
        'expected_cols': {
            'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pu_location_id', 'do_location_id', 'fare_amount'
        },
        'key_cols': ['vendor_id', 'pickup_time_local', 'do_location_id'],
        'timestamp_builder': taxi_trips_timestamp,
        'numeric_checks': {'trip_distance': (0, None), 'fare_amount': (0, None)},
        'transform': filter_unknown_locations,
        'schema_version': '1.0'
    },
    'weather': {
        'path': "data/weather.csv",
        'format': 'csv',
        'expected_cols': {'year', 'month', 'day', 'hour', 'temp', 'prcp'},
        'key_cols': ['year', 'month', 'day', 'hour'],
        'timestamp_builder': weather_timestamp,
        'numeric_checks': {'temp': (-90, 60)},
        'transform': None,
        'schema_version': '1.0'
    },
    'air_quality': {
        'path': "data/hourly_88101_2024.csv",
        'format': 'csv',
        'expected_cols': {'state_code', 'county_code', 'date_gmt', 'time_gmt', 'sample_measurement', 'parameter_code'},
        'key_cols': ['state_code', 'county_code', 'site_num', 'time_utc'],
        'timestamp_builder': air_quality_timestamp,
        'numeric_checks': {'sample_measurement': (0, None)},
        'transform': None,
        'schema_version': '1.0'
    },
    'taxi_zone': {
        'path': "data/taxi_zone_lookup.csv",
        'format': 'csv',
        'expected_cols': {'location_id', 'borough', 'zone'},
        'key_cols': ['location_id'],
        'timestamp_builder': None,
        'numeric_checks': {},
        'transform': None,
        'schema_version': '1.0'
    },
}
