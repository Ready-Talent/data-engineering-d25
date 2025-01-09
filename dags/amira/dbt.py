from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

default_args = {
    'retries': 1
}
dag1 = DAG(
    'dbt_dag_amira',
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    description='Run DBT on airflow',
    schedule_interval=None
)

run_dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_job_amira',
        dbt_cloud_conn_id='dbt_cloud_conn_id', #Get from airflow
        job_id=70471823400129,
        wait_for_completion=True,
        timeout=300,
        dag = dag1
    )

run_dbt_job