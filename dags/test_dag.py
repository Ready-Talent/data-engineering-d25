from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'retries': 1
}


dag1 = DAG(
    'test_dag',
    default_args=default_args,
    description='creating a test DAG',
    schedule_interval=None
)

def print_one():
    return "Hello Ready, this is Ziad."


def print_two():
    return "function number 2"


task_number_one = PythonOperator(
    task_id = "task_1",
    python_callable = print_one,
    dag = dag1   
)

task_number_two = PythonOperator(
    task_id = "task_2",
    python_callable = print_two,
    dag = dag1   
)


task_number_one >> task_number_two






