from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import json
from pathlib import Path
import os

current_file_path = Path(__file__).resolve()
parent_directory = current_file_path.parent
schema_file_path = parent_directory / "customer_table.json"

with open(schema_file_path) as schema_file:
    schema_fields = json.load(schema_file)


table_id = 'customers_menna'
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


sql_query = """
INSERT ready-de-25.airflow_star_schema.customers_menna (customer_id,name,email,address,phone)
SELECT customer_id,name,email,address,phone
FROM ready-de-25.ecommerce.customers
"""

insert_job = BigQueryInsertJobOperator(
    task_id='insert_customer_table_menna',
    configuration={
        "query": {
            "query": sql_query,
            "useLegacySql": False
        }
    },
    dag =  dag1
)