from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from datetime import datetime
import os
from pathlib import Path
import json

default_args = {
    'retries': 1
}


dag1 = DAG(
    'create_table_dimcustomer_amira',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    description='Create Table on bigquery called dim_customer',
    schedule_interval=None
)

json_file_path = Path(__file__).parent / 'D:/Users/amira.hussein/Downloads/data-engineering-d25/dags/amira/schema.json'  

with open(json_file_path) as f:
    schema_fields = json.load(f)

create_table = BigQueryCreateEmptyTableOperator(
    task_id="dim_customer_amira",
    dataset_id="airflow_star_schema",
    table_id="dim_customer_amira",
    schema_fields=schema_fields,
    dag=dag1
)
load_dim_customer_data = BigQueryInsertJobOperator(
    task_id="load_dim_customer_table",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `ready-de-25.airflow_star_schema.dim_customer_amira`
                    SELECT 
                        customer_id,
                        name,
                        email,
                        address,
                        phone,
                        created_at,
                        updated_at
                    FROM `ready-de-25.ecommerce.customers`
                """,
                "useLegacySql": False,
            }
        },
    dag=dag1

)
    
create_table >> load_dim_customer_data

