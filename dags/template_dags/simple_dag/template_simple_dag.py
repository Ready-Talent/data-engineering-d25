from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "Omar Thabet",
    "depends_on_past": False,
    "catchup": False,
    "email": ["omarthabet@email.com"],
    "email_on_failure": True,
    "max_active_runs": 1,
}

dag = DAG(
    "test_dag",
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    description="creating a test DAG",
    schedule_interval=None,
)


def print_one():
    return "hello world"


def print_two(x, y):
    return x + y


def print_three(date):
    return date


execution_date = "{{execution_dates}}"

task_number_one = PythonOperator(task_id="task_1", python_callable=print_one, dag=dag)

task_number_two = PythonOperator(
    task_id="task_2", python_callable=print_two, op_kwargs={"x": 1, "y": 2}, dag=dag
)

task_number_three = PythonOperator(
    task_id="task_3",
    python_callable=print_three,
    op_kwargs={"date": execution_date},
    dag=dag,
)

task_number_one >> task_number_two >> task_number_three
