import os
import logging
from airflow import DAG
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}


def download_parquetize_upload_dag(
    dag,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )
    
        format_to_parquet_task >> local_to_gcs_task >> rm_task

Tree1995Census_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_census_1995.csv'
Tree1995Census_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_census_1995.parquet'
Tree1995Census_GCS_PATH_TEMPLATE = "NY_Tree_Census/new_york_tree_census_1995.parquet"

Tree1995_dag = DAG(
    dag_id="tree1995_dag",
    default_args=default_args,
    tags=['Tree-census-project'],
)

download_parquetize_upload_dag(
    dag=Tree1995_dag,
    local_csv_path_template = Tree1995Census_CSV_FILE_TEMPLATE,
    local_parquet_path_template = Tree1995Census_PARQUET_FILE_TEMPLATE,
    gcs_path_template  =Tree1995Census_GCS_PATH_TEMPLATE
)

Tree2005Census_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_census_2005.csv'
Tree2005Census_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_census_2005.parquet'
Tree2005Census_GCS_PATH_TEMPLATE = "NY_Tree_Census/new_york_tree_census_2005.parquet"

Tree2005_dag = DAG(
    dag_id="tree2005_dag",
    default_args=default_args,
    tags=['Tree-census-project'],
)

download_parquetize_upload_dag(
    dag=Tree2005_dag,
    local_csv_path_template = Tree2005Census_CSV_FILE_TEMPLATE,
    local_parquet_path_template = Tree2005Census_PARQUET_FILE_TEMPLATE,
    gcs_path_template  =Tree2005Census_GCS_PATH_TEMPLATE
)

Tree2015Census_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_census_2015.csv'
Tree2015Census_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_census_2015.parquet'
Tree2015Census_GCS_PATH_TEMPLATE = "NY_Tree_Census/new_york_tree_census_2015.parquet"

Tree2015_dag = DAG(
    dag_id="tree2015_dag",
    default_args=default_args,
    tags=['Tree-census-project'],
)

download_parquetize_upload_dag(
    dag=Tree2015_dag,
    local_csv_path_template = Tree2015Census_CSV_FILE_TEMPLATE,
    local_parquet_path_template = Tree2015Census_PARQUET_FILE_TEMPLATE,
    gcs_path_template  =Tree2015Census_GCS_PATH_TEMPLATE
)


TreeSpecies_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_species.csv'
TreeSpecies_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/data/new_york_tree_species.parquet'
TreeSpecies_GCS_PATH_TEMPLATE = "NY_Tree_Census/new_york_tree_species.parquet"

TreeSpecies_dag = DAG(
    dag_id="tree_species_dag",
    default_args=default_args,
    tags=['Tree-census-project'],
)

download_parquetize_upload_dag(
    dag=TreeSpecies_dag,
    local_csv_path_template = TreeSpecies_CSV_FILE_TEMPLATE,
    local_parquet_path_template = TreeSpecies_PARQUET_FILE_TEMPLATE,
    gcs_path_template  =TreeSpecies_GCS_PATH_TEMPLATE
)