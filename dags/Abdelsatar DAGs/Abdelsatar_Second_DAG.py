from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

DATASET_ID = "airflow_star_schema"

default_args = {
    'retries': 1,
}

dag = DAG(
    'Abdelsatar_ByCreate',
    default_args=default_args,
    description='Just Creation For a table',
    schedule_interval=None,
    catchup=False
)

Task1 = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id=DATASET_ID,
    table_id="Dim_Cust_Abdelsatar",
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Address", "type": "STRING", "mode": "NULLABLE"},
    ],
)
