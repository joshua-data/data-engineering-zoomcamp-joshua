import os
from time import time

import pandas as pd

from sqlalchemy import create_engine
import argparse

def main(params):

    # Parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'yellow_tripdata_2021-01'

    wget = f"wget {url} -O {csv_name}.parquet"
    os.system(wget)

    # DB Engine Connection
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(db_url)
    engine.connect()

    # Parquet to CSV
    df = pd.read_parquet(
        f"{csv_name}.parquet",
        engine = 'pyarrow'
    )
    df.to_csv(
        f"{csv_name}.csv"
    )

    # DataFrame Iteration
    df_iter = pd.read_csv(
        f"{csv_name}.csv",
        index_col = 0,
        iterator = True,
        chunksize = 100000
    )

    # Start to Upload Data
    while True:
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("No more chunks to precess. Exiting loop.")
            break

        # Edit Column Data Type
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        # Insert to the Table
        df.to_sql(
            name = 'yellow_taxi_data',
            con = engine,
            if_exists = 'append' # Record to append if the table already exists.
        )

        t_end = time()

        print('Inserted another chunk, took %.3f seconds.' % (t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description = "Ingest CSV data to Postgres"
    )

    # user, password, host, port, db name, table name, url of the csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)