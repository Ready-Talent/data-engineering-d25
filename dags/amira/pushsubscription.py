import os
from google.cloud import pubsub_v1
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'D:\Users\amira.hussein\Downloads\ready-de-25-f50d96fb2977.json'
project_id = "ready-de-25"
topic_id = "ecommerce-events"

topic_name = f"projects/{project_id}/topics/{topic_id}"

publisher = pubsub_v1.PublisherClient()

data = [
    {"event": "order_placed", "order_id": 123, "name": "Amira"},
    {"event": "order_shipped", "order_id": 123, "tracking_id": "XYZ"},
]

for message in data:
    message_bytes = str(message).encode("utf-8")

    future = publisher.publish(topic_name, message_bytes)
    print(f"Published message ID: {future.result()}")
