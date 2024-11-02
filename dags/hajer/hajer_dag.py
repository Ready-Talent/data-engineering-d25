from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'retries': 1
}


dag1 = DAG(
    'hajer_dag_1',
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    description='creating assignment 1 DAG',
    schedule_interval=None
)


def print_text():
    return "First dag assignment by hajer"

def print_sum(variable1,variable2):
    return variable1,variable2

def print_execution_time(variable):
    return variable

execution_date = '{{ execution_date }}'


task_number_one = PythonOperator(
    task_id = "task_1",
    python_callable = print_text,
    dag = dag1   
)

task_number_two = PythonOperator(
    task_id = "task_2",
    python_callable = print_sum,
    op_kwargs = {"variable1": 1,"variable2":2},
    dag = dag1   
)

task_number_three = PythonOperator(
    task_id = "task_3",
    python_callable = print_execution_time,
    op_kwargs = {"date": execution_date},
    dag = dag1 
)

task_number_one >> task_number_two >> task_number_three