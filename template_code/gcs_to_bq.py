from google.cloud import bigquery
from google.oauth2 import service_account


# Function to load CSV file from GCS to BigQuery
def load_csv_to_bigquery(
    bucket_name, file_name, dataset_id, table_id, project_id, credentials
):
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table
    )
    # GCS URI where the CSV file is stored
    uri = f"gs://{bucket_name}/{file_name}"
    # Load the CSV file into BigQuery
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Loaded data from {uri} into BigQuery table {table_ref}")


# GCS and BigQuery parameters
bucket_name = "ready-de-25.chicago-taxi-data-ready-d25"
file_name = "data/Taxi_Trips_-_2024_20240408.csv"
bigquery_project_id = "ready-de-25"
bigquery_dataset_id = "landing"
bigquery_table_id = "chicago_taxi_ot"
credentials_path = "your_path"

# Set up BigQuery credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Load CSV from GCS to BigQuery
load_csv_to_bigquery(
    bucket_name,
    file_name,
    bigquery_dataset_id,
    bigquery_table_id,
    bigquery_project_id,
    credentials,
)
