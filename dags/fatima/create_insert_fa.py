from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import datetime
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
        current_dir = os.getcwd()

        # Join the current directory with a filename to create a full path
        file_path = os.path.join(current_dir, "your_schema.json")

        # Load the JSON schema from a file or string
        with open(file_path, 'r') as f:
            schema_json = json.load(f)

        table = bigquery.Table(table_ref, schema=schema_json)
        table = client.create_table(table)  # API request

    
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    insert_data_task = BigQueryInsertJobOperator(
        task_id='insert_data',
        sql="""
            INSERT INTO `airflow_star_schema.dim_customer_fatima`
            SELECT * FROM `ecommerce.customers`
        """,
        destination_dataset_table='airflow_star_schema.dim_customer_fatima',
    )

    create_table_task >> insert_data_task