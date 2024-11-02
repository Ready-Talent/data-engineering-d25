from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
#from airflow.providers.postgres.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
#from airflow.providers.google.cloud.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


default_args = {
    'retries': 1
}

with DAG(
    'transfer_dag_f',
    default_args=default_args,
    description= 'postgres_to_bigquery_transfer',
    schedule_interval=None
) as transfer_dag_fatima:
    
    postgres_to_gcs = PostgresToGCSOperator(
            task_id="postgres_to_gcs",
            postgres_conn_id='postgres_conn',
            sql="SELECT * FROM orders",
            bucket= "ready-platform-3",
            filename= "dag/fatima/orders_transfer_fa.csv",
             export_format="csv"
        )
                

    load_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id='load_to_bq',
            bucket='ready-platform-3',
            source_objects=['dag/fatima/orders_transfer_fa.csv'],
            destination_dataset_table='ready-de-25.airflow_transfers',
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            source_format='CSV',
            google_cloud_storage_conn_id='google_cloud_storage_default',
            bigquery_conn_id='google_cloud_default'
        )

    extract_to_gcs >> load_to_bq

