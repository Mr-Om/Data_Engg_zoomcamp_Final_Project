import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
datasets = ["new_york_tree_census_1995.csv","new_york_tree_census_2005.csv", "new_york_tree_census_2015.csv", "new_york_tree_species.csv"]

def download_from_s3(key, bucket_name: str, local_path: str):
    hook = S3Hook('s3_conn')
    file_name = []
    for each in key:
        file_name.append(hook.download_file(each, bucket_name=bucket_name, local_path=local_path))
    return file_name

def rename_file(ti, new_name) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    for (i,j) in zip(downloaded_file_name[0],new_name):
        downloaded_file_path='/'.join(i.split('/')[:-1])
        os.rename(src=i, dst=f"{downloaded_file_path}/{j}")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

download_dag = DAG(
    dag_id = "Tree_Dataset",
    schedule_interval="@once",
    default_args = default_args,
)

def download_dag_execute(
    dag,
    download_from_s3,
    rename_file
):
    with dag:
        task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': datasets,
            'bucket_name': 'demobucket3535',
            'local_path': '/opt/airflow/data/'
            }
        )

        task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': datasets
            }
        )

        task_download_from_s3 >> task_rename_file

download_dag_execute(download_dag,download_from_s3,rename_file)   

