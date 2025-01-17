import pandas as pd
from sqlalchemy import create_engine

import os
from time import time
import argparse

def main(params):

    # Parameters    

    host = params.host
    port = params.port
    user = params.user
    password = params.password

    db = params.db
    table_name = params.table_name
    file_url = params.file_url

    fpath_raw = 'yellow_tripdata_2021-01'

    # 웹에서 파일 가져오기
    wget = f'wget {file_url} -O {fpath_raw}.parquet'
    os.system(wget)

    # Postgres DB 연결하기
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)
    engine.connect()

    # Parquet to CSV
    df = pd.read_parquet(
        f'{fpath_raw}.parquet',
        engine='pyarrow',
    )
    df.to_csv(f'{fpath_raw}.csv', index=False)

    # Iteration 객체 만들기
    df_iter = pd.read_csv(
        f'{fpath_raw}.csv',
        index_col=0,
        iterator=True,
        chunksize=100000,
    )

    # DB 업로드 시작
    while True:

        start_ts = time()

        try:
            df = next(df_iter)
        except:
            break
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
        )

        end_ts = time()

        print(f'Chunk inserted taking {end_ts - start_ts:.2f} seconds.')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Ingest Parquet Data to Postgres.'
    )

    parser.add_argument('--host', help='postgres host')
    parser.add_argument('--port', help='postgres port')
    parser.add_argument('--user', help='postgres user')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--db', help='postgres db name')
    parser.add_argument('--table_name', help='postgres table name')
    parser.add_argument('--file_url', help='parquet file url')

    args = parser.parse_args()
    main(args)