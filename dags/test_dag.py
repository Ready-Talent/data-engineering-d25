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

def print_text():
    return "Hello Ready"



def sum(**kwargs):
    x = kwargs['dag_run'].conf.get('x', 0)
    y = kwargs['dag_run'].conf.get('y', 0)
    z = x + y
    print(f"The sum of {x} and {y} is {z}")
    return z


task_number_one = PythonOperator(
    task_id = "task_1",
    python_callable = print_text,
    dag = dag1   
)

task_number_two = PythonOperator(
    task_id = "task_2",
    python_callable = sum,
    dag = dag1   
)

task_number_one >> task_number_two