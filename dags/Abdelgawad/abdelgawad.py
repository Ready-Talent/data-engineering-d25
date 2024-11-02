from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from datetime import datetime


def transfer_postgres_to_bigquery():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    sql_query = "SELECT * FROM orders;"
    df = postgres_hook.get_pandas_df(sql_query)

    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default')
    client = bq_hook.get_client()

    table_id = "ready-de-25.airflow_transfers.orders_abdelgawad"
    job = client.load_table_from_dataframe(df, table_id)
    job.result() 


    default_args = {'start_date': datetime(2024, 11, 2)}
    
with DAG('postgres_to_bigquery', default_args=default_args, schedule_interval=None) as dag:
    transfer_task = PythonOperator(
        task_id='transfer_postgres_to_bigquery',
        python_callable=transfer_postgres_to_bigquery
    )