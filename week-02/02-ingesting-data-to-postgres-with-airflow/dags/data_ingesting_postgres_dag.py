import os
from datetime import datetime
import pytz

from ingest_data import save_to_csv, load_to_postgres

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

# ====================================================================================
# 변수 정의
# ====================================================================================

INPUT_FNAME_RAW = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}'
INPUT_FNAME_PARQUET = f'{INPUT_FNAME_RAW}.parquet'
INPUT_FNAME_CSV = f'{INPUT_FNAME_RAW}.csv'
INPUT_URL_PARQUET = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{INPUT_FNAME_PARQUET}'

OUTPUT_FOLDERPATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
OUTPUT_FNAME_PARQUET = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FNAME_CSV = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FPATH_PARQUET = f'{OUTPUT_FOLDERPATH}/{OUTPUT_FNAME_PARQUET}'
OUTPUT_FPATH_CSV = f'{OUTPUT_FOLDERPATH}/{OUTPUT_FNAME_CSV}'

PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DB = os.environ.get("PG_DB")
PG_TABLE_NAME = 'yellow_taxi_trips_{{ execution_date.strftime(\'%Y_%m\') }}'
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# ====================================================================================
# Default Arguments
# ====================================================================================

default_args = {
    "owner": "Joshua Kim",
    "start_date": datetime(2024, 1, 1),
    "depends_on_past": False,
    "retries": 0
}

# ====================================================================================
# 주요 함수 정의
# ====================================================================================

def check_valid_date(**kwargs):

    execution_date_utc = kwargs['execution_date'].astimezone(pytz.UTC)
    deadline_date_utc = datetime(2024, 11, 30, tzinfo=pytz.UTC)

    if execution_date_utc <= deadline_date_utc:
        return 'download_parquet'
    else:
        return 'skip_task'

def skip_task():
    print('현재 다운로드 가능한 파일은 2024년 11월까지입니다.')

# ====================================================================================
# DAG 시작
# ====================================================================================

with DAG(
    dag_id="data_ingesting_postgres_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["de-zoomcamp"],
) as dag:
    
    task_check_valid_date = BranchPythonOperator(
        task_id='check_valid_date',
        python_callable=check_valid_date,
    )

    task_skip_task = PythonOperator(
        task_id='skip_task',
        python_callable=skip_task,
    )

    task_download_parquet = BashOperator(
        task_id='download_parquet',
        bash_command=f"curl -sSL {INPUT_URL_PARQUET} > {OUTPUT_FPATH_PARQUET}",
    )

    task_save_to_csv = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        op_kwargs=dict(
            fpath_parquet=OUTPUT_FPATH_PARQUET,
            fpath_csv=OUTPUT_FPATH_CSV,
        )
    )

    task_load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs=dict(
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DB,
            table_name=PG_TABLE_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            fpath_csv=OUTPUT_FPATH_CSV,
        )
    )

    task_check_valid_date >> [task_download_parquet, task_skip_task]
    task_download_parquet >> task_save_to_csv >> task_load_to_postgres