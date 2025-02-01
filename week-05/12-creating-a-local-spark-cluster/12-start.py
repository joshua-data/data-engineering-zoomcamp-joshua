import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ======================================================
# Parse Arguments
# ======================================================
parser = argparse.ArgumentParser()

parser.add_argument('--green_path', required=True)
parser.add_argument('--yellow_path', required=True)
parser.add_argument('--save_path', required=True)

args = parser.parse_args()

green_path = args.green_path
yellow_path = args.yellow_path
save_path = args.save_path

# ======================================================
# Start Spark Session
# ======================================================
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

# ======================================================
# Load Input Data
# ======================================================

df_green = spark.read \
    .parquet(green_path) \
    .selectExpr(
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge"        
    )

df_yellow = spark.read \
    .parquet(yellow_path) \
    .selectExpr(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee"            
    )

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# ======================================================
# Union All Input Data
# ======================================================

common_cols = []

for item in set(df_yellow.columns) & set(df_green.columns):
    common_cols.append(item)

df_yellow_common = df_yellow \
    .select(common_cols) \
    .withColumn('service_type', F.lit('yellow'))

df_green_common = df_green \
    .select(common_cols) \
    .withColumn('service_type', F.lit('green'))

df_all = df_yellow_common.unionAll(df_green_common)
df_all.createOrReplaceTempView('trips_data')

# ======================================================
# Analyze Data
# ======================================================

query = """
    SELECT 
        PULocationID AS revenue_zone,
        DATE_TRUNC('MONTH', pickup_datetime) AS yyyy_mm,
        service_type,

        SUM(fare_amount) AS fare_amount,
        SUM(extra) AS extra,
        SUM(mta_tax) AS mta_tax,
        SUM(tip_amount) AS tip_amount,
        SUM(tolls_amount) AS tolls_amount,
        SUM(improvement_surcharge) AS improvement_surcharge,
        SUM(total_amount) AS total_amount,
        SUM(congestion_surcharge) AS congestion_surcharge,
        AVG(passenger_count) AS avg_passenger_cnt,
        AVG(trip_distance) AS avg_trip_distance
    FROM
        trips_data
    GROUP BY
        1, 2, 3
"""
df_result = spark.sql(query)
df_result.coalesce(1).write.parquet(save_path, mode='overwrite')