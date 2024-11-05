from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
import json
from pathlib import Path
import os

json_file_path = Path(r'E:\ready\airflow_task\data-engineering-d25\dags\Menna\customer_table.json')

parent_directory = json_file_path.parent
schema_file_path = parent_directory / json_file_path.name
schema_fields = os.open(schema_file_path)

table_id = 'customers'
project_id = 'ready-de-25'
dataset_id = 'airflow_star_schema'

default_args = {
    'retries': 1
    
}
 
dag1 = DAG(
    'transformation_dag_menna',
    default_args=default_args,
    description='creation of dimension table',
    schedule_interval=None
)


CreateTable = BigQueryCreateEmptyTableOperator(
    task_id="BqTableCreation_menna",
    dataset_id=dataset_id,
    table_id=table_id,
    project_id=project_id,
    schema_fields = schema_fields,
    dag=dag1
)


 