
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator



# Define the DAG
with DAG(
    dag_id='dim_customer_dag_SaifAmir',
    description='Creating a dim customer table and filling it with data' ,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Task 1: Create the Dim_Customer table
    create_dim_customer_table = BigQueryCreateEmptyTableOperator(
        task_id='create_dim_customer_table',
        project_id='ready-de-25',
        dataset_id='airflow_star_schema',
        table_id='dim_customer_SaifAmir',
        schema_fields=[

  {
    "name": "customer_id",
    "mode": "",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "name",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "email",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "address",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "phone",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "created_at",
    "mode": "",
    "type": "DATE",
    "description": "",
    "fields": []
  },
  {
    "name": "updated_at",
    "mode": "",
    "type": "DATE",
    "description": "",
    "fields": []
  }

        ],
    )
    INSERT_ROWS_QUERY = (

        """
        INSERT INTO `ready-de-25.airflow_star_schema.dim_customer_SaifAmir` (customer_id, name, email, address, phone, created_at, updated_at)
        SELECT
            customer_id,
            name,
            email,
            address,
            phone,
            created_at,
            updated_at
        FROM
            `ready-de-25.ecommerce.dim_customer`
        """

    )

    # Task 2: Insert data into Dim_Customer
    load_data_into_dim_customer = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": INSERT_ROWS_QUERY,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
      )




    # Define task dependencies
    start >> create_dim_customer_table >> load_data_into_dim_customer

