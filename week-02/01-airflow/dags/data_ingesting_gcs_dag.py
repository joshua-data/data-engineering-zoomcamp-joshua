import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from airflow.utils.dates import days_ago

import pyarrow.csv as pv
import pyarrow.parquet as pq

# Define Variables

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'demo_dataset')

dataset_parquet_fname = "yellow_tripdata_2021-01.parquet"
dataset_parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_parquet_fname}"

folderpath_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# UDFs

# NOTE: takes 20 mins, at an upload speed of 800kbps.
def upload_file_to_gcs(bucket, local_fpath, object_fpath):
    # Reference: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    # bucket: GCS bucket name
    # local_fpath: source path & file-name
    # object_fpath: target path & file-name

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_fpath)
    blob.upload_from_filename(local_fpath)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id = "data_ingestion_gcs_dag",
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ['de-zoomcamp']
) as dag:

    download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command=f"curl -sSL {dataset_parquet_url} > {folderpath_to_local_home}/{dataset_parquet_fname}"
    )    

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    upload_file_to_gcs_task = PythonOperator(
        task_id = "upload_file_to_gcs_task",
        python_callable = upload_file_to_gcs,
        op_kwargs = {
            "bucket": GCS_BUCKET,
            "object_fpath": f"raw/{dataset_parquet_fname}",
            "local_fpath": f"{folderpath_to_local_home}/{dataset_parquet_fname}"
        }
    )

    create_bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id = "create_bigquery_external_table_task",
        table_resource = {
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table"
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{GCS_BUCKET}/raw/{dataset_parquet_fname}"]
            }
        }
    )

    download_dataset_task >> upload_file_to_gcs_task >> create_bigquery_external_table_task