import pandas as pd
from sqlalchemy import create_engine

from time import time

def save_to_csv(fpath_parquet, fpath_csv):

    df = pd.read_parquet(fpath_parquet, engine='pyarrow')
    df.to_csv(fpath_csv, index=False)

def load_to_postgres(host, port, db, table_name, user, password, fpath_csv):

    # Postgres DB 연결하기
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)
    engine.connect()

    # Iteration 객체 만들기
    df_iter = pd.read_csv(
        fpath_csv,
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
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
        )

        end_ts = time()

        print(f'Chunk inserted taking {end_ts - start_ts:.2f} seconds.')