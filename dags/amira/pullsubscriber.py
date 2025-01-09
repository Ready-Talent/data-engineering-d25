import os
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'D:\Users\amira.hussein\Downloads\ready-de-25-f50d96fb2977.json'

project_id = "ready-de-25"
subscription_id = "ecommerce-events-sub"

subscription_name = f"projects/{project_id}/subscriptions/{subscription_id}"

subscriber = pubsub_v1.SubscriberClient()

def callback(message):
    print(f"Received message: {message.data}")
    message.ack()

print(f"Listening for messages on: {subscription_name}")

streaming_pull_future = subscriber.subscribe(subscription_name, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
except Exception as e:
    print(f"An error occurred: {e}")

