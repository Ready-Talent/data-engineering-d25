import requests


def trigger_cloud_function():
    # Replace this URL with your deployed function's endpoint
    url = "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table"

    try:
        # Send a GET request to the function
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            # Save the response content (CSV) to a file
            with open("output.csv", "wb") as file:
                file.write(response.content)
            print("CSV file successfully downloaded as 'output.csv'")
        else:
            # Print error if the request failed
            print(f"Error: Received status code {response.status_code}")
            print(f"Response content: {response.text}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Trigger the function
trigger_cloud_function()
