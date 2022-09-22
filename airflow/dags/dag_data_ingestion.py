import os
from threading import local
import dask.dataframe as dd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from zipfile import ZipFile
from zipfile import is_zipfile
from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_DATASET = os.environ.get("GCP_GCS_BUCKET_DATASET")
BUCKET_REVIEWS = os.environ.get("GCP_GCS_BUCKET_REVIEWS")
BIGQUERY_DATASET = os.environ.get('GCP_BQ_DATASET')
#DATASET_ZIP = os.path.join(AIRFLOW_HOME, "steam-dataset.zip")    # !! no more local files
#DATASET_REVIEWS_ZIP = os.path.join(AIRFLOW_HOME, "archive.zip")  # !! no more local files

DATASET_DIR = os.path.join(AIRFLOW_HOME, "dataset/") 

#GCS_DIR = "raw"



def download_gcs(bucket, local_dir):

#    if not os.path.exists(local_dir):
#        os.makedirs(local_dir)

    hook = GCSHook()
    gcs_objs_list = hook.list(bucket_name = bucket)
    for obj in gcs_objs_list : 
        if (obj.endswith(".json")):
            local_obj = os.path.join(local_dir, obj) 
            if not os.path.exists(local_obj):
                os.makedirs(os.path.dirname(local_obj), exist_ok=True)

            hook.download(bucket_name = bucket,
                         object_name = obj,
                         filename = local_obj)

def modify_path(path, old_prefix, new_prefix) : 
   return path.replace(old_prefix, new_prefix)


def upload_gcs(bucket, local_dir):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    hook = GCSHook()
    for path, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(path, file)
                file_path_mod = modify_path(file_path, "raw", "proc")
                file_path_mod = os.path.relpath(file_path_mod, local_dir)
                hook.upload(bucket_name = bucket,
                            filename = file_path,
                            object_name = file_path_mod)


def format_to_parquet(local_dir):

    for path, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(path, file)
                file_path_parquet = file_path.replace('.json', '.parquet')
                ddf = dd.read_json(file_path, orient='index')
                ddf = ddf.astype('string') 
                ddf.to_parquet(file_path_parquet, compression='snappy')





default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['steam-de'],
) as dag:

    
    download_steam_dataset_from_datalake_task = PythonOperator(
        task_id="download_steam_dataset_from_datalake_task",
        python_callable=download_gcs,
        op_kwargs={
            "bucket": BUCKET_DATASET,
            "local_dir": DATASET_DIR 
        }
    )

    format_to_parquet_inlocal_task = PythonOperator(
        task_id="format_to_parquet_inlocal_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "local_dir": DATASET_DIR
        }
    )

    rm_files_task = BashOperator(
        task_id="remove_needless_files_inlocal_task",
        bash_command=f"find {DATASET_DIR} -type d -name news_data -prune -exec rm -rf {{}} \; &&  find {DATASET_DIR} -type d -name steam_charts -prune -exec rm -rf {{}} \; && find {DATASET_DIR} -name 'missing.json' -delete"
    )


    upload_steam_dataset_proc_task = PythonOperator(
        task_id="upload_steam_dataset_proc_task",
        python_callable=upload_gcs,
        op_kwargs={
            "bucket": BUCKET_DATASET,
            "local_dir": DATASET_DIR 
        }
    )


    hook = GCSHook()
    bq_parallel_tasks = list()
    gcs_objs_list = hook.list(bucket_name = BUCKET_DATASET, prefix = "proc")
    for obj in gcs_objs_list: 
        TABLE_NAME = obj.split("/")[-2].replace('.parquet', '')
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_external_table_{TABLE_NAME}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": TABLE_NAME,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET_DATASET}/{obj}"]
                },
            }
        )
        bq_parallel_tasks.append(bigquery_external_table_task)


    rm_task = BashOperator(
        task_id='remove_files_from_local',
        bash_command=f'rm -rf {DATASET_DIR}',
        trigger_rule="all_done"
    )


    download_steam_dataset_from_datalake_task >> format_to_parquet_inlocal_task >> rm_files_task >> upload_steam_dataset_proc_task >> bq_parallel_tasks >> rm_task
    #download_reviews_task >> unzip_reviews_gcs_task >> rm_task