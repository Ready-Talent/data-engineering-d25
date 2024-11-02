from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'retries': 1,
}

# Define DAG
with DAG(
    dag_id='postgres_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['data-transfer', 'postgres', 'bigquery', 'gcs']
) as dag:

    # Configuration
    postgres_conn_id = 'postgres_conn'              # PostgreSQL connection ID
    gcp_conn_id = 'google_cloud_default'            # GCP connection ID in Composer
    gcs_bucket = 'ready-d25-postgres-to-gcs/ziad'   # GCS bucket name
    project_id = 'ready-de-25'
    dataset_id = ' airflow_transfers'
    
    # Define table mapping
    tables_to_load = {
        'orders': 'ziad_orders'  # Mapping of PostgreSQL table to BigQuery table
    }

    for postgres_table, bigquery_table in tables_to_load.items():
        # Task 1: Export data from PostgreSQL to GCS as a JSON file
        export_to_gcs = PostgresToGCSOperator(
            task_id=f'export_{postgres_table}_to_gcs',
            postgres_conn_id=postgres_conn_id,
            sql=f'SELECT * FROM {postgres_table}',
            bucket=gcs_bucket,
            filename=f'data/{postgres_table}.json',
            export_format='json'
        )

        # Task 2: Load the JSON file from GCS into BigQuery
        load_to_bigquery = GCSToBigQueryOperator(
            task_id=f'load_{postgres_table}_to_bigquery',
            bucket=gcs_bucket,
            source_objects=[f'data/{postgres_table}.json'],
            destination_project_dataset_table=f'{project_id}.{dataset_id}.{bigquery_table}',
            source_format='NEWLINE_DELIMITED_JSON',
            write_disposition='WRITE_TRUNCATE',
            create_disposition= "CREATE_IF_NEEDED",  
            autodetect=True
        )

        # Set task dependencies
        export_to_gcs >> load_to_bigquery
