from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


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
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "DATE", "mode": "NULLABLE"},
        {"name": "updated_at", "type": "DATE", "mode": "NULLABLE"},
    ],
    dag = dag
)

Task2 = BigQueryInsertJobOperator(
    task_id="inserting",
    configuration={
        "query": {
            "query": """
                INSERT INTO `ready-de-25.airflow_star_schema.Dim_Cust_Abdelsatar` (customer_id, name, Email, Address,phone,created_at,updated_at)
                Select
                customer_id, name, Email, Address,phone,created_at,updated_at
                FROM `ready-de-25.ecommerce.customers`
            """,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    dag=dag
)


Task1 > Task2