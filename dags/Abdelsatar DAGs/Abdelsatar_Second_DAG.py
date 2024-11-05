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
    ],
    dag = dag
)

Task2 = BigQueryInsertJobOperator(
    task_id="inserting",
    configuration={
        "query": {
            "query": """
                INSERT INTO `ready-de-25.airflow_star_schema.Dim_Cust_Abdelsatar` (customer_id, name, Email, Address)
                VALUES
                    (1, 'John Doe', 'john.doe@example.com', '123 Main St, Springfield'),
                    (2, 'Jane Smith', 'jane.smith@example.com', '456 Oak St, Springfield'),
                    (3, 'Alice Johnson', 'alice.johnson@example.com', '789 Pine St, Springfield'),
                    (4, 'Bob Brown', 'bob.brown@example.com', '101 Maple St, Springfield'),
                    (5, 'Charlie White', 'charlie.white@example.com', '202 Birch St, Springfield')
            """,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    dag=dag
)


Task1 > Task2