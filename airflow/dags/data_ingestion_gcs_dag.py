import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_ZIP = os.path.join(AIRFLOW_HOME, "steam-dataset.zip")
DATASET_DIR = os.path.join(AIRFLOW_HOME, "dataset/")


GCS_DIR = "raw"


def upload_to_gcs(bucket_name, local_dir):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    hook = GCSHook()
    for path, currentDirectory, files in os.walk(local_dir):
        for file in files:
            file_path = os.path.join(path, file)
            hook.upload(bucket_name = bucket_name,
                        filename = file_path,
                        object_name = os.path.relpath(file_path, local_dir))


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"kaggle datasets download -d souyama/steam-dataset && cp steam-dataset.zip {AIRFLOW_HOME}"
    )


    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip {DATASET_ZIP} -d {DATASET_DIR}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "local_dir": os.path.dirname(DATASET_DIR)
        },
    )


    rm_task = BashOperator(
        task_id='remove_files_from_local',
        bash_command=f'rm -rf {DATASET_ZIP} {DATASET_DIR}',
        trigger_rule="all_done"

    )

    download_dataset_task >> unzip_dataset_task >> local_to_gcs_task >> rm_task
