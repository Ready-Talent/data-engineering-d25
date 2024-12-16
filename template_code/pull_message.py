from google.cloud import pubsub_v1
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "C:/Users/Omar/Downloads/ready-de-25-f50d96fb2977.json"
)
project_id = "ready-de-25"
subscription_id = "ecommerce-events-sub"

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message):
    print(f"Received: {message.data}")
    message.ack()


streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages...")

try:
    streaming_pull.result()
    print(streaming_pull.result())
except KeyboardInterrupt:
    streaming_pull.cancel()
