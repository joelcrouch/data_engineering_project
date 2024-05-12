
import os
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import pandas

import Project2.schema as schema 

# Define the project ID and subscription ID
project_id = "data-engineering-spring-2024"
subscription_id = "my-sub"

# Number of seconds the subscriber should listen for messages
timeout = 5.0

# Create a directory to store received data if it doesn't exist
receiver_data_dir = "receiver_data"
if not os.path.exists(receiver_data_dir):
    os.makedirs(receiver_data_dir)

# Define the subscription path
subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

CURRENT_DATETIME = None

# Callback function to handle received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Parse the received JSON data
        data = json.loads(message.data)

        # Get current date and time
        CURRENT_DATETIME = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Save received data to a file
        file_name = os.path.join(receiver_data_dir, f"data_{CURRENT_DATETIME}.json")
        with open(file_name, "w") as file:
            json.dump(data, file, indent=4)

        # Acknowledge the message
        message.ack()
    except json.JSONDecodeError:
        print("Failed to decode JSON data\n")

# Create a Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()

# Subscribe to the specified subscription
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

# Print listening message
print(f"Listening for messages on {subscription_path}..\n")

# Wait for messages
with subscriber:
    try:
        # Wait for messages until timeout
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown
        streaming_pull_future.result()  # Block until the shutdown is complete
    finally:
        schema.initialize()
        conn = schema.dbconnect()
        rlis = schema.readdata(f"data_{CURRENT_DATETIME}.json")

        df_path = os.path.join(receiver_data_dir, f"data_{CURRENT_DATETIME}.json")
        df = pandas.read_json(df_path)
        df['TIMESTAMP'] = df.apply(lambda row: schema.calculate_timestamp(row), axis=1)
        
        df = df.drop(columns=['ACT_TIME', 'OPD_DATE'])

        df['dTIMESTAMP'] = df['TIMESTAMP'].diff()
        df['dMETERS'] = df['METERS'].diff()

        df['SPEED'] = df.apply(lambda row: (row['dMETERS'] / row['dTIMESTAMP'].total_seconds()), axis = 1)

        df.at[0, 'SPEED'] = df.at[1, 'SPEED']

        df = df.drop(columns=['dTIMESTAMP', 'dMETERS'])

        cmdlist = schema.getSQLcmnds(df.iterrows())

        schema.load(conn, cmdlist)
