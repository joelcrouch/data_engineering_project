
import os
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import psycopg2

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

# Callback function to handle received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Parse the received JSON data
        data = json.loads(message.data)
        
        
        #connect ot database
        # Establish connection to PostgreSQL database
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1234",
            host="localhost"  # Or your PostgreSQL server's IP address
        )
        print("Connected to PostgreSQL")
        # validate& transform data data
        
        
        
        
        
        # # Assertion 1: Ensure 'tstamp' exists in the data
        # assert 'tstamp' in data, "Missing 'tstamp' field in received data."

        # # Assertion 2: Validate 'latitude' limit
        # assert -90 <= data['latitude'] <= 90, "Latitude value is out of range."

        # # Assertion 3: Validate 'longitude' limit
        # assert -180 <= data['longitude'] <= 180, "Longitude value is out of range."

        # # Assertion 4: Ensure 'speed' is non-negative
        # assert data['speed'] >= 0, "Speed value cannot be negative."

        # # Assertion 5: Ensure 'trip_id' exists in the data
        # assert 'trip_id' in data, "Missing 'trip_id' field in received data."

        # # Assertion 6: Ensure 'vehicle_id' exists in the data
        # assert 'vehicle_id' in data, "Missing 'vehicle_id' field in received data."

        # # Query the database for unique tstamp for the given vehicle_id, latitude, and longitude
        # unique_tstamp = query_database_for_unique_tstamp(data['vehicle_id'], data['latitude'], data['longitude'])

        # # Assertion 7: Check if unique tstamp exists
        # assert unique_tstamp is None or unique_tstamp == data['tstamp'], "Duplicate tstamp for vehicle at the same location."
        # Get current date and time
        current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Save received data to a file
        # file_name = os.path.join(receiver_data_dir, f"data_{current_datetime}.json")
        # with open(file_name, "w") as file:
        #     json.dump(data, file, indent=4)
        file_name = os.path.join(receiver_data_dir, f"data_{current_date}.json")
        with open(file_name, "a") as file:  # Append mode to append to the file
            json.dump(data, file)
            file.write("\n")  # Add a newline between each record

        # Acknowledge the message
        message.ack()
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
    except json.JSONDecodeError:
        print("Failed to decode JSON data\n")
    finally:
        # Close the database connection
        if conn is not None:
            conn.close()
            print("Connection closed")
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

