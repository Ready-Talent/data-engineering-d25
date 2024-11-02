from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

dag = DAG(
    'transfer_dag_abdelgawad',
    default_args=default_args,
    description='Creating a transfer DAG',
    schedule_interval=None,
    
)

postgres_to_gcs = PostgresToGCSOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id='postgres_conn',
    sql='SELECT * FROM orders',
    bucket='ready-d25-postgres-to-gcs',
    filename='abdelgawad/orders.csv',
    export_format="csv",
    dag=dag,
)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="ready-d25-postgres-to-gcs",
    source_objects=["abdelgawad/orders.csv"],
    destination_project_dataset_table="airflow_transfers.orders_abdelgawad",
    schema_fields=[
        {"name": "order_id", "mode": "NULLABLE", "type": "INTEGER"},
        {"name": "customer_id", "mode": "NULLABLE", "type": "INTEGER"},
        {"name": "order_date", "mode": "NULLABLE", "type": "DATETIME"},
        {"name": "created_at_timestamp", "mode": "NULLABLE", "type": "DATETIME"},
        {"name": "updated_at_timestamp", "mode": "NULLABLE", "type": "DATETIME"},
    ],
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

# Set task dependencies
postgres_to_gcs >> load_csv
