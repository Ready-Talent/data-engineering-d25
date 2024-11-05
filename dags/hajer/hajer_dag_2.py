from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'retries': 1
}

with DAG(
    'postgres_to_bq-hajer',
    describtion='creating transfer dag',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    postgres_to_gcs = PostgresToGCSOperator(
        task_id="export_postgres_to_gcs",
        sql="select * from orders",
        postgres_conn_id='postgres_conn',
        bucket='ready-d25-postgres-to-gcs',
        filename="hajer/orders.csv",
        export_format='csv',   
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='ready-d25-postgres-to-gcs',
        source_objects='hajer/orders.csv',
        destination_project_dataset_table='ready-de-25.airflow_transfers.orders_hajer',
        source_format='csv',
        skip_leading_rows=1,
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
    )

    postgres_to_gcs >> gcs_to_bigquery

