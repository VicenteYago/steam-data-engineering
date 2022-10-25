import os
import json
import glob
import sys
import dask.dataframe as dd
from google.api_core import page_iterator
from google.cloud import storage

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator

sys.path.append("airflow/dags/common_package")                                                    
from common_package.utils_module import *

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_STORE = os.environ.get("GCP_GCS_BUCKET_STORE")
BUCKET_REVIEWS = os.environ.get("GCP_GCS_BUCKET_REVIEWS")
BUCKET_SUBDIR = "raw"
BUCKET_SUBDIR_PROC = "proc"
BIGQUERY_DATASET = os.environ.get('GCP_BQ_DATASET')
DATASETS_LOCAL_DIR = os.path.join(AIRFLOW_HOME, "datasets/") 
STEAM_STORE_LOCAL_DIR = os.path.join(DATASETS_LOCAL_DIR, "steamstore")
REVIEWS_LOCAL_DIR = os.path.join(DATASETS_LOCAL_DIR, "reviews")
REVIEWS_LOCAL_DIR_PROC = os.path.join(DATASETS_LOCAL_DIR, "reviews_proc")
DBT_DIR = os.path.join(AIRFLOW_HOME, "dbt")

BQ_TABLE = 'steam_reviews'
TEMP_BUCKET = 'steam-datalake-store-alldata'
JOB_ARGUMENTS = [PROJECT_ID, BIGQUERY_DATASET, BQ_TABLE, BUCKET_REVIEWS, BUCKET_SUBDIR_PROC, TEMP_BUCKET]

CLUSTER_NAME = "steam-reviews-spark-cluster" #"spark-cluster-reviews"
CLUSTER_REGION = "europe-west1"
PYSPARK_FILE = "spark_all_games_reviews.py" # decouple in a separate (previous) task

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
}

'''
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_REVIEWS}/{PYSPARK_FILE}",
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.0-preview.jar"]},
    "arguments":JOB_ARGUMENTS

}
'''


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="steam_store_ingestion_dag",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['steam-de'],
) as dag:

    #-------------------------------------------STORE    
    download_steam_dataset_from_datalake_task = PythonOperator(
        task_id="download_steam_dataset_from_datalake_task",
        python_callable=download_gcs,
        op_kwargs={
            "bucket": BUCKET_STORE,
            "local_dir": STEAM_STORE_LOCAL_DIR 
        }
    )

    format_to_parquet_inlocal_task = PythonOperator(
        task_id="format_to_parquet_inlocal_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "local_dir": STEAM_STORE_LOCAL_DIR
        }
    )

    rm_files_store_task = BashOperator(
        task_id="remove_needless_files_store_inlocal_task",
        bash_command=f"find {STEAM_STORE_LOCAL_DIR} -type d -name news_data -prune -exec rm -rf {{}} \; &&  find {STEAM_STORE_LOCAL_DIR} -type d -name steam_charts -prune -exec rm -rf {{}} \; && find {STEAM_STORE_LOCAL_DIR} -name 'missing.json' -delete"
    )

    upload_steam_dataset_proc_task = PythonOperator(
        task_id="upload_steam_dataset_proc_task",
        python_callable=upload_gcs,
        op_kwargs={
            "bucket": BUCKET_STORE,
            "local_dir": STEAM_STORE_LOCAL_DIR 
        }
    )

    hook = GCSHook()
    bq_parallel_tasks = list()
    gcs_objs_list = hook.list(bucket_name = BUCKET_STORE, prefix = "proc")
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
                    "sourceUris": [f"gs://{BUCKET_STORE}/{obj}"]
                },
            }
        )
        bq_parallel_tasks.append(bigquery_external_table_task)

    rm_store_task = BashOperator(
        task_id='remove_store_files_from_local',
        bash_command=f'rm -rf {STEAM_STORE_LOCAL_DIR}',
        trigger_rule="all_done"
    )

    #-------------------------------------------DBT
    run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command=f'cd {DBT_DIR} && dbt run --profile airflow',
    trigger_rule="all_success"
    )

    download_steam_dataset_from_datalake_task >> format_to_parquet_inlocal_task >> rm_files_store_task >> upload_steam_dataset_proc_task >> bq_parallel_tasks >> rm_store_task >> run_dbt_task
