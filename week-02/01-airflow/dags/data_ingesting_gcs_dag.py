import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage

from airflow.utils.dates import days_ago

import pyarrow.csv as pv
import pyarrow.parquet as pq

# ====================================================================================
# 변수 정의
# ====================================================================================

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "demo_dataset")

dataset_parquet_fname = "yellow_tripdata_2021-01.parquet"
dataset_parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_parquet_fname}"
local_home_folderpath = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# ====================================================================================
# Default Arguments
# ====================================================================================

default_args = {
    "owner": "Joshua Kim",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

# ====================================================================================
# 주요 함수 정의
# ====================================================================================

def upload_file_to_gcs_bucket(bucket, local_fpath, object_fpath):

    # Reference: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    # bucket: GCS bucket name
    # local_fpath: source path & file-name
    # object_fpath: target path & file-name

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024 # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    blob = bucket.blob(object_fpath)
    blob.upload_from_filename(local_fpath)

# ====================================================================================
# DAG 시작
# ====================================================================================

with DAG(
    dag_id="data_ingesting_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["de-zoomcamp"],
) as dag:
    
    task_download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl -sSL {dataset_parquet_url} > {local_home_folderpath}/{dataset_parquet_fname}",
    )
    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    task_upload_file_to_bucket = PythonOperator(
        task_id="upload_file_to_bucket",
        python_callable=upload_file_to_gcs_bucket,
        op_kwargs={
            "bucket": GCS_BUCKET,
            "local_fpath": f"{local_home_folderpath}/{dataset_parquet_fname}",
            "object_fpath": f"raw/{dataset_parquet_fname}",
        }
    )

    task_create_bigquery_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_bigquery_external_table",
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{GCS_BUCKET}/raw/{dataset_parquet_fname}"],
            }
        }
    )

    task_download_dataset >> task_upload_file_to_bucket >> task_create_bigquery_external_table