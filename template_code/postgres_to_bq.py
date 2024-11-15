import psycopg2
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# Function to extract data from PostgreSQL
def extract_postgres_table(connection, table_name):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, connection)


# Function to load data to BigQuery
def load_to_bigquery(df, dataset_id, table_id, project_id, credentials):
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", create_disposition="CREATE_IF_NEEDED"
    )

    # Load the dataframe to BigQuery
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {len(df)} rows to {table_ref}")


# PostgreSQL connection parameters
postgres_config = {
    "user": "username",
    "password": "password",
    "host": "host",
    "port": "5432",
}

# BigQuery parameters
bigquery_project_id = "ready-de-25"
bigquery_dataset_id = "landing"
credentials_path = "your_path"

# Table mappings between Postgres and BigQuery
table_mappings = {
    "public.customers": "customers_your_name",
    "public.products": "products_your_name",
    "public.orders": "orders_your_name",
    # Add more tables as needed
}

# Connect to PostgreSQL
conn = psycopg2.connect(**postgres_config)

# Set up BigQuery credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)


# Extract each table from Postgres and load it to BigQuery
for postgres_table, bigquery_table in table_mappings.items():
    print(f"Extracting {postgres_table} from PostgreSQL...")
    df = extract_postgres_table(conn, postgres_table)
    print(f"Loading {bigquery_table} to BigQuery...")
    load_to_bigquery(
        df, bigquery_dataset_id, bigquery_table, bigquery_project_id, credentials
    )

# Close PostgreSQL connection
conn.close()
print("Data transfer complete!")
