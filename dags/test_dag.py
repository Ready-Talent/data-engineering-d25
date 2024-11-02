from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'retries': 1
    
}
 
dag1 = DAG(
    'test_dag',
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    description='creating a test DAG',
    schedule_interval=None
)


def print_three(date):
    return date




execution_date = '{{execution_date}}'

task_number_one = PythonOperator(
    task_id = "task_1",
    python_callable = print_text,
    dag = dag1   
)

task_number_two = PythonOperator(
    task_id = "task_2",
