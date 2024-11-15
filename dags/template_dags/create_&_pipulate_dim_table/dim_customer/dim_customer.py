from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
import os
import json
from pathlib import Path
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
    'dim_customer_fill',
    default_args=default_args,
    start_date=datetime(2024, 4, 20),
    description='creating dim cusotmer',
    schedule_interval=None
)

parent_path = str(Path(__file__).parent)
schema_file = os.path.join(parent_path, "schema/dim_customer.json")
project_id = "project_id"
dataset_id = "dataset"
table_id = "table_id"


create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_empty_table",
    project_id=project_id,
    dataset_id=dataset_id,
    table_id=table_id,
    table_resource=json.load(open(schema_file)),
    dag=dag,
)


load_table = BigQueryInsertJobOperator(
    task_id="load_table",
    configuration={
        "query": {
            "query": "{% include 'sql/dim_customer.sql' %}",
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_NEVER",
            "destinationTable": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
        }
    },
    dag=dag,
)





