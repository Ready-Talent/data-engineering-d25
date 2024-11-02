from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime

# Configuration
POSTGRES_CONN_ID = 'postgres_conn' 
GCS_BUCKET = 'ready-d25-postgres-to-gcs/abdelsatar'
GCS_FILENAME = 'ready-de-25.airflow_transfers/orders_abdelsatar.json'

# Default arguments for the DAG
default_args = {
    'retries': 1
}

# Define the DAG
dag = DAG(
    'postgres_to_gcs_optimized_dag',
    default_args=default_args,
    description='PostgreSQL to GCS',
    schedule_interval=None,
    start_date=datetime(2023, 11, 1),
    catchup=False
)

# Define the task using PostgresToGCSOperator
transfer_task = PostgresToGCSOperator(
    task_id='transfer_orders_to_gcs',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT * FROM orders",
    bucket=GCS_BUCKET,
    filename=GCS_FILENAME,
    export_format='json',
    gzip=False,  # Set to True if you want to compress the output
    dag=dag
)

transfer_task