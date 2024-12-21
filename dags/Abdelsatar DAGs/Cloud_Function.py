from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define DAG
with DAG(
    "pubsub_to_bq_Abdelsatars_dag",
    default_args=default_args,
    description="Call Cloud Function to process Pub/Sub messages and BigQuery",
    schedule_interval=None,  # Trigger manually or via API
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to invoke the Cloud Function
    invoke_cloud_function = CloudFunctionInvokeFunctionOperator(
        task_id="invoke_cloud_function_Abdelsatar",
        project_id="ready-de-25",  # Replace with your project ID
        location="us-central1",  # Replace with your Cloud Function's region
        input_data=None
        function_id="pubsub_to_bq_http",  # Cloud Function name
    )