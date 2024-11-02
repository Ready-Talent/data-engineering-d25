from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime


default_args = {
    'retries': 1
}


dag1 = DAG(
    'Transfer_postgres_to_bigquery_amira',
    default_args=default_args,
    start_date=datetime(2024, 11, 2),
    description='Transfer postgres to gcs and from gcs to bigquery',
    schedule_interval=None
)

transfer_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id="transfer_postgres_to_gcs",
        postgres_conn_id="postgres_conn",
        sql="SELECT * FROM orders",
        bucket="ready-d25-postgres-to-gcs",
        filename="amira/orders_amira.json",
        export_format="json",
        dag=dag1
)
load_from_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket='ready-d25-postgres-to-gcs',
        source_objects=['amira/orders_amira.json'],
        destination_project_dataset_table='ready-de-25.airflow_transfers.orders_amira',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE', 
        dag=dag1
)

transfer_from_postgres_to_gcs >> load_from_gcs_to_bigquery