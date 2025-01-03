from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import datetime


default_args = {
    'retries': 1
}

with DAG(
    'dim_customer_hajer',
    description='creating dim_customer dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
) as dag:
    
    create_dim_customers = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        dataset_id='airflow_star_schema',
        table_id='dim_customers_hajer',
        schema_fields=[
            {
                "name": "customer_id",
                "mode": "NULLABLE",
                "type": "INTEGER"
            },
            {
                "name": "name",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "email",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "address",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "phone",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "created_at",
                "mode": "NULLABLE",
                "type": "DATE"
            },
            {
                "name": "updated_at",
                "mode": "NULLABLE",
                "type": "DATE"
            }
            ]
    )



    populate_dim_customers = BigQueryInsertJobOperator(
        task_id='insert_query_job',
        configuration={
            "query": {
                "query": 'insert into ready-de-25.airflow_star_schema.dim_customers_hajer select * from ready-de-25.ecommerce.dim_customer',
                "useLegacySql": False,
            }
        }
    )

    create_dim_customers >> populate_dim_customers