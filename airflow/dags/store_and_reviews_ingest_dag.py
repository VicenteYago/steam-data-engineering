import os
import json
import glob
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
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator \
                                                              


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

def download_gcs(bucket, local_dir, prefix=None):

#    if not os.path.exists(local_dir):
#        os.makedirs(local_dir)

    hook = GCSHook()
    gcs_objs_list = hook.list(bucket_name = bucket, prefix = prefix)
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
                filename = os.path.join(path, file)

                object_name = modify_path(filename, "raw", "proc")
                object_name = os.path.relpath(object_name, local_dir)

                hook.upload(bucket_name = bucket,
                            filename = filename,
                            object_name = object_name)

def format_to_parquet(local_dir):

    for path, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(path, file)
                file_path_parquet = file_path.replace('.json', '.parquet')
                ddf = dd.read_json(file_path, orient='index')
                ddf = ddf.astype('string') 
                ddf.to_parquet(file_path_parquet, compression='snappy')

#https://stackoverflow.com/questions/37074977/how-to-get-list-of-folders-in-a-given-bucket-using-google-cloud-api
def _item_to_value(iterator, item):
    return item

def list_directories(bucket_name, prefix):
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    extra_params = {
        "projection": "noAcl",
        "prefix": prefix,
        "delimiter": '/'
    }

    gcs = storage.Client()

    path = "/b/" + bucket_name + "/o"

    iterator = page_iterator.HTTPIterator(
        client=gcs,
        api_request=gcs._connection.api_request,
        path=path,
        items_key='prefixes',
        item_to_value=_item_to_value,
        extra_params=extra_params,
    )
    return [x for x in iterator]


def n_files_in_bucket_dir(bucket, prefix, client):
    blob_list = client.list_blobs(bucket_or_name=bucket, prefix = prefix)
    return(sum([1 for i in blob_list]))

def compact_dir(input_dir, output_dir, filename):
    glob_data = []
    for file in glob.glob(os.path.join(input_dir, "*.json")):
        with open(file) as json_file:
            data = json.load(json_file)
            glob_data.append(data)
    with open(os.path.join(output_dir, filename), 'w') as f:
        json.dump(glob_data, f, indent=4)

def filter_dir_by_nfiles(dir_list, bucket, gcs_client): 
    review_pgs_by_game_filter = { 
        dir : v  for dir in dir_list if (v := n_files_in_bucket_dir(bucket, dir, gcs_client)) > 1
    }
    return review_pgs_by_game_filter



def upload_gcs_reviews(bucket, local_dir):  # shared function used in dag_data_ingestion.py
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
            if file.endswith(".json"): # !!!! MODIFIED ".parquet"
                filename = os.path.join(path, file)

                object_name = os.path.join(BUCKET_SUBDIR_PROC, file)

                hook.upload(bucket_name = bucket,
                            filename = filename,
                            object_name = object_name)


def build_compacted_reviews_dataset(bucket,
                                    bucket_subdir,
                                    reviews_local_dir,
                                    reviews_local_dir_proc): 

    # list all directories in gcp bucket 
    gcs_client = storage.Client()
    dir_list = list_directories(bucket, prefix=bucket_subdir)

    # calculate dict w/ dirs that contains more than 1 file
    dir_list_filter_dict = filter_dir_by_nfiles(dir_list, bucket, gcs_client)

    # download selected dirs in local
    for dir in dir_list_filter_dict.keys() : 
        download_gcs(bucket, local_dir = reviews_local_dir, prefix=dir)

    # compress local dirs into 1 json file per dir 
    if not os.path.exists(reviews_local_dir_proc):
        os.makedirs(reviews_local_dir_proc)
    
    for root, dirs, files in os.walk(os.path.join(reviews_local_dir, bucket_subdir)):
        for dir in dirs:
            dir_path = os.path.join(os.path.join(reviews_local_dir, bucket_subdir), dir)
            compact_dir(dir_path, reviews_local_dir_proc, f'{dir}-compacted.json')

    # upload dirs to bucket in /proc folder
    upload_gcs_reviews(bucket = bucket, local_dir = reviews_local_dir_proc)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="steam_store_and_reviews_ingestion_dag",
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

    #-------------------------------------------REVIEWS

    download_steam_reviews_from_datalake_task = PythonOperator(
        task_id="download_steam_reviews_from_datalake_task",
        python_callable=build_compacted_reviews_dataset,
        op_kwargs={
            "bucket": BUCKET_REVIEWS,
            "bucket_subdir": BUCKET_SUBDIR,
            "reviews_local_dir" : REVIEWS_LOCAL_DIR,
            "reviews_local_dir_proc" : REVIEWS_LOCAL_DIR_PROC
        }
    )

    create_cluster_dataproc_task = DataprocCreateClusterOperator(
        task_id="create_cluster_dataproc_task",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=CLUSTER_REGION,
        trigger_rule="all_success",
        cluster_name=CLUSTER_NAME
    )

    '''
    submit_spark_job_task = DataprocSubmitJobOperator(
        task_id="submit_spark_job_task",
        job=PYSPARK_JOB,
        region=CLUSTER_REGION,
        project_id=PROJECT_ID
    )
    '''
    submit_spark_job_task = DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_spark_job_task",
        main = f"gs://{BUCKET_REVIEWS}/{PYSPARK_FILE}",
        arguments = JOB_ARGUMENTS,
        cluster_name = CLUSTER_NAME,
        region = CLUSTER_REGION,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.0-preview.jar"]
    )

    delete_cluster_dataproc_task = DataprocDeleteClusterOperator(
        task_id="delete_cluster_dataproc_task",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=CLUSTER_REGION,
        trigger_rule="all_done"
    )

    rm_reviews_task = BashOperator(
        task_id='remove_reviews_files_from_local',
        bash_command=f'rm -rf {REVIEWS_LOCAL_DIR} {REVIEWS_LOCAL_DIR_PROC}',
        trigger_rule="all_done"
    )

    #-------------------------------------------DBT
    run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command=f'cd /dbt && dbt run --profile airflow',
    trigger_rule="all_done"
    )

    download_steam_dataset_from_datalake_task >> format_to_parquet_inlocal_task >> rm_files_store_task >> upload_steam_dataset_proc_task >> bq_parallel_tasks >> rm_store_task >> run_dbt_task
    download_steam_reviews_from_datalake_task >> create_cluster_dataproc_task >> submit_spark_job_task >> delete_cluster_dataproc_task >> rm_reviews_task >> run_dbt_task