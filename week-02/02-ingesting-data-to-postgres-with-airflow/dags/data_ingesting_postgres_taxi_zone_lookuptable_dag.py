import os
from datetime import datetime
import pytz

from ingest_taxi_zone_lookuptable import save_to_csv, load_to_postgres

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

# ====================================================================================
# 변수 정의
# ====================================================================================

INPUT_FNAME_RAW = 'taxi_zone_lookup'
INPUT_FNAME_CSV = f'{INPUT_FNAME_RAW}.csv'
INPUT_URL_CSV = f'https://d37ci6vzurychx.cloudfront.net/misc/{INPUT_FNAME_CSV}'

OUTPUT_FOLDERPATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
OUTPUT_FNAME_CSV = 'output_green_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FPATH_CSV = f'{OUTPUT_FOLDERPATH}/{OUTPUT_FNAME_CSV}'

PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DB = os.environ.get("PG_DB")
PG_TABLE_NAME = 'taxi_zone_lookup'
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# ====================================================================================
# Default Arguments
# ====================================================================================

default_args = {
    "owner": "Joshua Kim",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 0
}

# ====================================================================================
# DAG 시작
# ====================================================================================

with DAG(
    dag_id="data_ingesting_postgres_taxi_zone_lookuptable_dag",
    schedule_interval="0 6 2 1 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["de-zoomcamp"],
) as dag:

    task_download_csv = BashOperator(
        task_id='download_csv',
        bash_command=f"curl -sSL {INPUT_URL_CSV} > {OUTPUT_FPATH_CSV}",
    )

    task_save_directly_to_csv = PythonOperator(
        task_id='save_directly_to_csv',
        python_callable=save_to_csv,
        op_kwargs=dict(
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
        ),
    )

    task_download_csv >> task_save_directly_to_csv >> task_load_to_postgres