from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json

# Path to your service account key JSON file
credentials = service_account.Credentials.from_service_account_file(
    "C:/Users/Omar/Downloads/ready-de-25-f50d96fb2977.json"
)

# Use credentials in your Pub/Sub client
publisher = pubsub_v1.PublisherClient(credentials=credentials)

project_id = "ready-de-25"
topic_id = "ecommerce-events"

topic_path = publisher.topic_path(project_id, topic_id)

messages = [
    {"event": "order_placed", "order_id": 1, "amount": 50},
    {"event": "order_shipped", "order_id": 2, "amount": 100},
]
while True:
    for message in messages:
        print("message is", message)
        job = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        print(job)
        job.result()
        print(f"Published: {message}")
