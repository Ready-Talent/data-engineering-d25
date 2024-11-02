from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'retries': 1
}


dag1 = DAG(
    'postgres_to_bigquery_hajer',
    default_args=default_args,
    start_date=datetime(2024, 11, 1),
    description='creating transfer dag',
    schedule_interval=None,
    catchup=False,
)


postgres_to_gcs_1 = PostgresToGCSOperator(
    task_id="export_postgres_to_gcs",
    sql="select * from orders",
    postgres_conn_id="postgres_conn",
    bucket="ready-d25-postgres-to-gcs",
    filename="hajer/orders.csv",
    export_format="csv",
    dag = dag1   
)

gcs_to_bigquery_2 = GCSToBigQueryOperator(
    task_id="load_csv_to_bigquery",
    bucket="ready-d25-postgres-to-gcs",  # Same GCS bucket name
    source_objects=["hajer/orders.csv"],  # List of files to load
    destination_project_dataset_table="ready-de-25.airflow_transfers.orders_hajer",  # BigQuery destination
    source_format="CSV",  # Specify the source format
    skip_leading_rows=1,  # Skip header row if present
    autodetect=True,  # Enable schema auto-detection
    create_disposition="CREATE_IF_NEEDED",
    dag = dag1
)

postgres_to_gcs_1 >> gcs_to_bigquery_2

