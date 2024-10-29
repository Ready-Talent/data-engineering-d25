from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'retries': 1
}


dag1 = DAG(
    'amira_first_dag',
    default_args=default_args,
    description='creating a test DAG',
    schedule_interval=None
)