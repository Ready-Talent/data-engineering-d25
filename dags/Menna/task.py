from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

bucket_name= 'ready-d25-postgres-to-gcs'
postgres_conn_id = 'postgres_conn'
table_name = 'orders'
file_format = 'csv'
file_name = f'menna/{table_name}.{file_format}'
project_id = 'ready-de-25'
dataset_id = 'airflow_transfers'
default_args = {
    'retries': 1

}


with DAG(
    'transfer_dag_menna',
    default_args=default_args,
    description='transfer tables from postgres to bq',
    schedule_interval=None
) as dag:

    postgres_to_gcs= PostgresToGCSOperator(
        task_id='trn_postgres_to_gcs',
        postgres_conn_id=postgres_conn_id,
        sql=f'select * from {table_name};',
        bucket=bucket_name,
        filename=file_name
        export_format='csv',
    )
    

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='trn_gcs_to_bq',
        bucket=bucket_name,               
        source_objects=file_name, 
        destination_project_dataset_table=f'{project_id},{dataset_id}.{table_name}_menna', 
        create_disposition='CREATE_IF_NEEDED',
        autodetect= True,
        source_format='csv',
    )

    postgres_to_gcs >> gcs_to_bigquery
