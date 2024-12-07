from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python import PythonOperator
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import ReceivedMessage
from airflow.exceptions import AirflowException
from google.api_core.exceptions import GoogleAPICallError, RetryError
import json
import os
from datetime import datetime, timedelta

# Set the path to your service account key file for authentication
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\SWE_m\OneDrive\Desktop\pub-sub\ready-de-25-f50d96fb2977.json"

# Configuration
project_id = "ready-de-25"
subscription_id = "ecommerce-events-sub"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

# Function to process and print the message
def process_and_print_message(ti):
    try:
        # Pull the message from Pub/Sub
        client = pubsub_v1.SubscriberClient()
        response = client.pull(subscription=subscription_path, max_messages=1, timeout=10)

        if not response.received_messages:
            raise AirflowException("No messages were received from the subscription.")
        
        for received_message in response.received_messages:
            # Print the message data
            print(f"Received message: {received_message.message.data.decode('utf-8')}")
            
            # Acknowledge the message so it is not processed again
            client.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])

    except GoogleAPICallError as api_error:
        print(f"Google API call error: {api_error}")
        raise
    except RetryError as retry_error:
        print(f"Retry error: {retry_error}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# Create the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your_email@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pubsub_Abdelsatar',
    default_args=default_args,
    description='A DAG that pulls messages from a Pub/Sub topic and prints them',
    schedule_interval=None,  # This DAG runs manually
    start_date=datetime(2024, 12, 7),
    catchup=False,
) as dag:

    # Step 1: Wait for a message to appear in the Pub/Sub subscription
    wait_for_message = PubSubPullSensor(
        task_id='wait_for_message',
        subscription=subscription_path,
        project_id=project_id,
        timeout=60,  # Timeout in seconds
        mode='poke',  # Polling mode
    )

    # Step 2: Process and print the message
    process_message = PythonOperator(
        task_id='process_message',
        python_callable=process_and_print_message,
    )

    # Set task dependencies
    wait_for_message >> process_message