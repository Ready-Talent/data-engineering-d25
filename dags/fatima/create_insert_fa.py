from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import datetime
from google.cloud import bigquery
import os

with DAG(
    'create_and_populate_bigquery_table',
    start_date=datetime(2024, 4, 20),
    schedule_interval=None
) as create_insert_dag:

    def create_table():
        client = bigquery.Client()
        dataset_id = 'airflow_star_schema'
        table_id = 'dim_customer_fatima'
        table_ref = client.dataset(dataset_id).table(table_id)


        # Get the current working directory
        current_file_path = os.path.abspath(__file__)
        parent_directory = os.path.dirname(current_file_path)

        # Join the current directory with a filename to create a full path
        json_directory = os.path.join(parent_directory,"your_schema.json")

        # Load the JSON schema from a file or string
        with open(json_directory, 'r') as f:
            schema_json = json.load(f)

        table = bigquery.Table(table_ref, schema=schema_json)
        table = client.create_table(table)  # API request

    
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )
    

    insert_data_task = BigQueryInsertJobOperator(
        task_id='insert_data',
        sql= """INSERT INTO `ready-de-25.airflow_star_schema.dim_customer_fatima`;SELECT * FROM `ready-de-25.ecommerce.customers`"""
        destination_dataset_table='ready-de-25.airflow_star_schema.dim_customer_fatima',
        configuration=bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV)
    )

    create_table_task >> insert_data_task