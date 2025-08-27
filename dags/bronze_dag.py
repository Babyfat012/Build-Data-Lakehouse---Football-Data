from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl_pipeline.assets.bronze_tasks import (
    create_bucket_if_not_exists, 
    load_csv_to_minio, 
    load_json_to_minio, 
    load_mysql_to_minio
)
# from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="bronze",
    default_args=default_args,
    description="Load data to Bronze Layer in MinIO",
    schedule_interval="@once"
)as dag:
    

    bucket_name = "lakehouse"

    create_bucket= PythonOperator(
        task_id="create_bucket_if_not_exists",
        python_callable=create_bucket_if_not_exists,
        op_kwargs={'bucket_name': bucket_name}
    )

    # Task to create bucket if not exists
    task_load_csv = PythonOperator(
        task_id="load_csv_to_minio",
        python_callable=load_csv_to_minio,
        op_kwargs={'bucket': bucket_name}
    )
    task_load_json = PythonOperator(
        task_id="load_json_to_minio",
        python_callable=load_json_to_minio,
        op_kwargs={'bucket': bucket_name}
    )
    task_load_mysql = PythonOperator(
        task_id="load_mysql_to_minio",
        python_callable=load_mysql_to_minio,
        op_kwargs={'bucket': bucket_name}
    )

    # Define task dependencies
    create_bucket >> [task_load_csv, task_load_json, task_load_mysql]