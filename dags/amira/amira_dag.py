from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'retries': 1
}


dag1 = DAG(
    'amira_first_dag',
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    description='creating a test DAG',
    schedule_interval=None
)


def print_text():
    return "First task dag for Amira"

def print_sum(variable1,variable2):
    return variable1,variable2

def print_execution_time(variable):
    return variable

execution_date = '{{ execution_date }}'


task_number_one = PythonOperator(
    task_id = "first_task",
    python_callable = print_text,
    dag = dag1   
)

task_number_two = PythonOperator(
    task_id = "second_task",
    python_callable = print_sum,
    op_kwargs = {"variable1": 1,"variable2":2},
    dag = dag1   
)

task_number_three = PythonOperator(
    task_id = "Third_task",
    python_callable = print_execution_time,
    op_kwargs = {"variable": execution_date},
    dag = dag1 
)

task_number_one >> task_number_two >> task_number_three
