from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

default_args = {
    'retries': 1,
}

dag = DAG(
    'create_table_abdelgawad',
    default_args=default_args,
    description='Creating tables',
    schedule_interval=None,
)



create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table_abdelgawad",
    project_id="ready-de-25",
    dataset_id="airflow_star_schema",
    table_id="dim_customer_abdelgawad",
    schema_fields=
[
  {
    "name": "customer_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
  },
  {
    "name": "name",
    "mode": "NULLABLE",
    "type": "STRING",
  },
  {
    "name": "email",
    "mode": "NULLABLE",
    "type": "STRING",
  },
  {
    "name": "address",
    "mode": "NULLABLE",
    "type": "STRING",
  },
  {
    "name": "phone",
    "mode": "NULLABLE",
    "type": "STRING",
  },
  {
    "name": "created_at_timestamp",
    "mode": "NULLABLE",
    "type": "DATETIME",
  },
  {
    "name": "updated_at_timestamp",
    "mode": "NULLABLE",
    "type": "DATETIME",
  }
],
    dag=dag
)

