from datetime import datetime
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Replace these with your dbt Cloud details
DBT_CLOUD_ACCOUNT_ID = "70471823400444"  
DBT_CLOUD_JOB_ID = "70471823400127"          

# Define the DAG
with DAG(
    dag_id='trigger_dbt_cloud_job_operator',
    default_args=default_args,
    description='A DAG to trigger a dbt Cloud job using DbtCloudRunJobOperator',
    schedule_interval=None,  
    start_date=datetime(2024, 1, 1),  
    catchup=False,
    tags=['dbt', 'trigger'],
) as dag:

    # Task to trigger the dbt Cloud job
    trigger_dbt_job = DbtCloudRunJobOperator(
        task_id='trigger_dbt_job',
        dbt_cloud_conn_id='dbt_cloud_default',  
        account_id=DBT_CLOUD_ACCOUNT_ID,
        job_id=DBT_CLOUD_JOB_ID,
        cause="Triggered by Airflow DbtCloudRunJobOperator",
    )