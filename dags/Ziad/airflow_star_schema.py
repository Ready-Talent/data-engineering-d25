from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# DAG default arguments
default_args = {
    'owner': 'Ziad',
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='ziad_create_star_schema',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    # Configurations
    project_id = 'ready-de-25'  
    dataset_id = 'star_schema' 
    table_id = 'ziad_dim_customer' 
    schema = [  
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]

    # Task 1: Create an empty BigQuery table with the specified schema
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=schema
    )

    # Task 2: Populate the table with data
    populate_table = BigQueryInsertJobOperator(
        task_id='populate_table',
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{project_id}.{dataset_id}.{table_id}`
                    SELECT 
                        customer_id,
                        name,
                        email,
                        phone,
                        address,
                        created_at_timestamp
                    FROM `{project_id}.ecommerce.customers`
                """,
                "useLegacySql": False,
            }
        }
    )

    create_table >> populate_table
