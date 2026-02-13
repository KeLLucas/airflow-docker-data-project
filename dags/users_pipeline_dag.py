from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 2, 13),
    "retries": 1,
}

with DAG(
    dag_id="users_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Pipeline ETL Users API",
) as dag:

    extract_task = BashOperator(
        task_id="extract_users",
        bash_command="python /opt/airflow/dags/scripts/extract_users.py",
    )

    transform_task = BashOperator(
        task_id="transform_users",
        bash_command="python /opt/airflow/dags/scripts/transform_users.py",
    )

    extract_task >> transform_task
