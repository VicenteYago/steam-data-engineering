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



def download_gcs(bucket, local_dir, prefix=None):

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