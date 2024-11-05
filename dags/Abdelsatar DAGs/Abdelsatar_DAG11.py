from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = 'postgres_conn'
GCS_BUCKET = 'ready-d25-postgres-to-gcs/abdelsatar'
GCS_FILENAME = 'ready-de-25.ecommerce.orders'
BIGQUERY_DATASET = 'ready-de-25.airflow_transfers'
BIGQUERY_TABLE = 'orders_abdelsatar'

default_args = {
    'retries': 1,
}

dag = DAG(
    'postgres_to_gcs_to_bigquery_dag',
    default_args=default_args,
    description='PostgreSQL to GCS then to BigQuery',
    schedule_interval=None,
    catchup=False
)

# Task 1: Transfer data from PostgreSQL to GCS
transfer_postgres_to_gcs = PostgresToGCSOperator(
    task_id='transfer_postgres_to_gcs',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql='SELECT * FROM orders1', 
    bucket=GCS_BUCKET,
    filename=GCS_FILENAME, 
    export_format='json', 
    dag=dag
)

# Task 2: Load data from GCS to BigQuery
load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id='load_gcs_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILENAME], 
    destination_project_dataset_table=f'{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
    source_format='NEWLINE_DELIMITED_JSON',  
    write_disposition='WRITE_TRUNCATE', 
    dag=dag
)

transfer_postgres_to_gcs >> load_gcs_to_bigquery