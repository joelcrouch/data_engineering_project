import requests
import json
from datetime import datetime
from google.cloud import pubsub_v1

def fetch_bus_data(vehicle_id):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Successfully fetched data for vehicle ID {vehicle_id}.")
        return response.json(), True
    else:
        print(f"Failed to fetch data for vehicle ID {vehicle_id}. Status code: {response.status_code}")
        return None, False

def publish_data(data, publisher, topic_path):
    count = 0
    # Iterate over each record in the JSON data
    for record in data:
        # Convert the record to a JSON string
        record_str = json.dumps(record)
        # Publish the JSON string as a message to Pub/Sub
        future = publisher.publish(topic_path, record_str.encode("utf-8"))
        if future:
            count += 1

    print(f"Published {count} messages.")

# Google Cloud project and topic configuration
project_id = "data-engineering-spring-2024"
topic_id = "my-topic"

# Create Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# List of vehicle IDs
vehicle_ids = [
    4212, 3802, 3249, 3507, 4234, 4227, 2938, 3929, 4036, 2907, 3007, 4528, 4237, 3131, 4052, 3616, 3959, 3634, 3328, 3264, 3218, 2916, 2920, 3748, 4205, 4001, 2903, 3624, 3631, 3110, 2908, 4202, 3031, 3518, 3226, 3022, 3262, 2936, 3626, 3525, 3245, 3615, 3720, 3201, 3153, 3958, 3932, 3253, 3324, 2930, 3921, 3543, 3524, 3223, 3010, 3001, 4020, 3318, 3241, 3509, 3718, 3647, 3123, 3267, 3413, 3157, 3639, 3213, 4010, 3515, 2902, 3151, 2918, 3013, 4529, 3122, 3027, 4024, 3221, 3549, 3136, 3142, 3158, 3404, 3236, 4216, 3202, 3711, 3502, 3015, 3134, 3713, 3531, 3127, 3619, 4209, 4207, 3163, 3629, 3736
]

# Counters for successful and failed requests
successful_requests = 0
failed_requests = 0

# Iterate over each vehicle ID
for vehicle_id in vehicle_ids:
    # Fetch data for the vehicle ID
    bus_data, success = fetch_bus_data(vehicle_id)
    if success:
        successful_requests += 1
        if bus_data:
            # Publish the fetched data to Pub/Sub
            publish_data(bus_data, publisher, topic_path)
    else:
        failed_requests += 1

# Print the number of successful and failed requests
print(f"Successful requests: {successful_requests}")
print(f"Failed requests: {failed_requests}")

# Compare with the number of vehicle IDs
total_vehicles = len(vehicle_ids)
print(f"Total vehicles: {total_vehicles}")

if total_vehicles == successful_requests:
    print("All vehicles were successfully fetched and published to Pub/Sub.")
else:
    print("Some vehicles failed to fetch data or publish to Pub/Sub.")


# from google.cloud import pubsub_v1
# import json
# import subprocess

# project_id = "data-engineering-spring-2024"
# topic_id = "my-topic"

# # Create Pub/Sub publisher
# publisher = pubsub_v1.PublisherClient()
# topic_path = publisher.topic_path(project_id, topic_id)

# # Function to fetch and return data for a vehicle ID 
# def fetch_bus_data(vehicle_id):
#     # Fetch JSON data using curl command
#     curl_command = f'curl https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}'
#     json_data = subprocess.check_output(curl_command, shell=True, text=True)
#     return json.loads(json_data)

# # List of vehicle IDs
# vehicle_ids = [
#     4212, 3802, 3249, 3507, 4234, 4227, 2938, 3929, 4036, 2907, 3007, 4528, 4237, 3131, 4052, 3616, 3959, 3634, 3328, 3264, 3218, 2916, 2920, 3748, 4205, 4001, 2903, 3624, 3631, 3110, 2908, 4202, 3031, 3518, 3226, 3022, 3262, 2936, 3626, 3525, 3245, 3615, 3720, 3201, 3153, 3958, 3932, 3253, 3324, 2930, 3921, 3543, 3524, 3223, 3010, 3001, 4020, 3318, 3241, 3509, 3718, 3647, 3123, 3267, 3413, 3157, 3639, 3213, 4010, 3515, 2902, 3151, 2918, 3013, 4529, 3122, 3027, 4024, 3221, 3549, 3136, 3142, 3158, 3404, 3236, 4216, 3202, 3711, 3502, 3015, 3134, 3713, 3531, 3127, 3619, 4209, 4207, 3163, 3629, 3736
# ]

# # Iterate over each vehicle ID
# for vehicle_id in vehicle_ids:
#     # Fetch data for the vehicle ID
#     print(f"Fetching data for vehicle ID: {vehicle_id}")
#     bus_data = fetch_bus_data(vehicle_id)

#     # Publish each record in the fetched data to Pub/Sub
#     for record in bus_data:
#         # Convert the record to a JSON string
#         record_str = json.dumps(record)
#         # Publish the JSON string as a message to Pub/Sub
#         future = publisher.publish(topic_path, record_str.encode("utf-8"))
#         print(f"Published message: {future.result()}")

