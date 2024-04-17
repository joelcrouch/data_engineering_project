from google.cloud import pubsub_v1
import json
import os
import subprocess
from datetime import datetime

# Function to fetch data for a vehicle ID and save it to a file
def fetch_data_and_save(vehicle_id):
    # Fetch JSON data using curl command
    curl_command = f'curl https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}'
    json_data = subprocess.check_output(curl_command, shell=True, text=True)

    # Get current date and time
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H-%M-%S")

    # Create directory for the current date if it doesn't exist
    directory_name = f"{current_date}"
    try:
        os.makedirs(directory_name)
    except FileExistsError:
        pass

    # Save the JSON data to a file within the directory
    file_name = f"{directory_name}/time_and_date_{vehicle_id}_{current_time}.json"
    with open(file_name, "w") as file:
        file.write(json_data)

    return file_name

# Function to publish fetched data to Pub/Sub
def publish_data(data, publisher, topic_path):
    # Iterate over each record in the JSON data
    for record in data:
        # Convert the record to a JSON string
        record_str = json.dumps(record)
        # Publish the JSON string as a message to Pub/Sub
        future = publisher.publish(topic_path, record_str.encode("utf-8"))
        print(f"Published message: {future.result()}")

    print("Published messages.")

project_id = "data-engineering-spring-2024"
topic_id = "my-topic"

# Create Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# List of vehicle IDs
vehicle_ids = [
    4212, 3802, 3249, 3507, 4234, 4227, 2938, 3929, 4036, 2907, 3007, 4528, 4237, 3131, 4052, 3616, 3959, 3634, 3328, 3264, 3218, 2916, 2920, 3748, 4205, 4001, 2903, 3624, 3631, 3110, 2908, 4202, 3031, 3518, 3226, 3022, 3262, 2936, 3626, 3525, 3245, 3615, 3720, 3201, 3153, 3958, 3932, 3253, 3324, 2930, 3921, 3543, 3524, 3223, 3010, 3001, 4020, 3318, 3241, 3509, 3718, 3647, 3123, 3267, 3413, 3157, 3639, 3213, 4010, 3515, 2902, 3151, 2918, 3013, 4529, 3122, 3027, 4024, 3221, 3549, 3136, 3142, 3158, 3404, 3236, 4216, 3202, 3711, 3502, 3015, 3134, 3713, 3531, 3127, 3619, 4209, 4207, 3163, 3629, 3736

    # Add more vehicle IDs for project (read:ALL)
]

# Iterate over each vehicle ID
for vehicle_id in vehicle_ids:
    # Fetch data for the vehicle ID and save it to a file
    file_name = fetch_data_and_save(vehicle_id)
    print(f"Data fetched and saved to {file_name}")

    # Publish the fetched data to Pub/Sub
    with open(file_name, "r") as file:
        data = json.load(file)
    publish_data(data, publisher, topic_path)