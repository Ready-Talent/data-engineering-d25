from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'create_table_abdelgawad',
    default_args=default_args,
    description='Creating tables and loading data',
    schedule_interval=None,
    start_date=datetime(2024, 11, 1),  # Set an appropriate start date
)

# Task to create the BigQuery table
create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table_abdelgawad",
    project_id="ready-de-25",
    dataset_id="airflow_star_schema",
    table_id="dim_customer_abdelgawad",
    schema_fields=[
        {"name": "customer_id", "mode": "NULLABLE", "type": "INTEGER"},
        {"name": "name", "mode": "NULLABLE", "type": "STRING"},
        {"name": "email", "mode": "NULLABLE", "type": "STRING"},
        {"name": "address", "mode": "NULLABLE", "type": "STRING"},
        {"name": "phone", "mode": "NULLABLE", "type": "STRING"},
        {"name": "created_at_timestamp", "mode": "NULLABLE", "type": "DATETIME"},
        {"name": "updated_at_timestamp", "mode": "NULLABLE", "type": "DATETIME"},
    ],
    dag=dag,
)

# Task to load data from another BigQuery table
load_data = BigQueryInsertJobOperator(
    task_id="load_data_from_customer_table",
    configuration={
        "query": {
            "query": """
                INSERT INTO `ready-de-25.airflow_star_schema.dim_customer_abdelgawad`
                SELECT * FROM `ready-de-25.ecommerce.customers`
            """,
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
        }
    },
    dag=dag,
)

# Set task dependencies
create_table >> load_data