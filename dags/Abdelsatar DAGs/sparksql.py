from google.cloud import bigquery
import pandas as pd

# Set up BigQuery client using your service account credentials
client = bigquery.Client()

# Replace with your project, dataset, and table IDs
project_id = "ready-de-25"
dataset_id = "landing"
table_id = "crime"

# Construct the full table name
table_name = f"{project_id}.{dataset_id}.{table_id}"

# Query to fetch data from BigQuery table
query = f"SELECT * FROM `{table_name}`"

# Run the query and get the result in a Pandas DataFrame
df = client.query(query).to_dataframe()

# Show the first few rows of the DataFrame
print(df.head())