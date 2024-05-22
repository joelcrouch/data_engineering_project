import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import io
#from io import StringIO
from google.cloud import pubsub_v1

# Pub/Sub configuration
project_id = "data-engineering-spring-2024"
topic_id_bus = "my-topic"
topic_id_stop = "bus-stop"

publisher = pubsub_v1.PublisherClient()
topic_path_bus = publisher.topic_path(project_id, topic_id_bus)
topic_path_stop = publisher.topic_path(project_id, topic_id_stop)

# List of vehicle IDs
vehicle_ids = [
    4212, 3802, 3249, 3507, 4234, 4227, 2938, 3929, 4036, 2907, 3007, 4528, 4237, 3131, 4052, 3616, 3959, 3634, 3328, 3264, 3218, 2916, 2920, 3748, 4205, 4001, 2903, 3624, 3631, 3110, 2908, 4202, 3031, 3518, 3226, 3022, 3262, 2936, 3626, 3525, 3245, 3615, 3720, 3201, 3153, 3958, 3932, 3253, 3324, 2930, 3921, 3543, 3524, 3223, 3010, 3001, 4020, 3318, 3241, 3509, 3718, 3647, 3123, 3267, 3413, 3157, 3639, 3213, 4010, 3515, 2902, 3151, 2918, 3013, 4529, 3122, 3027, 4024, 3221, 3549, 3136, 3142, 3158, 3404, 3236, 4216, 3202, 3711, 3502, 3015, 3134, 3713, 3531, 3127, 3619, 4209, 4207, 3163, 3629, 3736
]

columns_to_keep = ['PDX_TRIP', 'route_number', 'vehicle_number', 'service_key', 'direction']

# Get the current date
current_date = datetime.now().strftime("%m-%d")

# Create an empty list to store individual DataFrames for stop data
stop_data = []

# Iterate over each vehicle ID for stop data
for vehicle_id in vehicle_ids:
    # Define the URL with the vehicle_id
    URL = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"

    # Send a GET request to fetch the HTML content
    response = requests.get(URL)

    if response.status_code == 200:
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all <h2> tags
        for h2_tag in soup.find_all('h2'):
            # Extract trip number using regular expression
            trip_number_match = re.search(r'\d+', h2_tag.text)
            if trip_number_match:
                trip_number = int(trip_number_match.group())
            else:
                trip_number = None
            
            # Find the table following the <h2> tag
            table = h2_tag.find_next_sibling('table')
            
            # Read the table into a DataFrame
            #df = pd.read_html(str(table))[0]
            html_io = io.StringIO(str(table))
            df = pd.read_html(html_io)[0]

            
            # Add a column for the trip number
            df['PDX_TRIP'] = trip_number
            df = df[columns_to_keep]
            
            # Append DataFrame to data list
            stop_data.append(df)
    else:
        # Print error message if the request was not successful
        print(f"Error fetching data for vehicle {vehicle_id}: Status code {response.status_code}")

# Concatenate all DataFrames for stop data
combined_stop_df = pd.concat(stop_data, ignore_index=True)

# Create a directory if it doesn't exist
stop_directory = f"combined_receiver_data/{current_date}/stop_data"
if not os.path.exists(stop_directory):
    os.makedirs(stop_directory)

# Define the file path for stop data
stop_csv_file_path = f"{stop_directory}/combined_stop_data.csv"
stop_json_file_path = f"{stop_directory}/combined_stop_data.json"

# Save the DataFrame to CSV and JSON files for stop data
combined_stop_df.to_csv(stop_csv_file_path, index=False)
combined_stop_df.to_json(stop_json_file_path, orient='records')

# Print confirmation message
print(f"Stop DataFrames saved to: {stop_csv_file_path} and {stop_json_file_path}")

# Now, let's fetch and publish bus data to Pub/Sub

# Function to fetch bus data
def fetch_bus_data(vehicle_id):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Successfully fetched data for vehicle ID {vehicle_id}.")
        return response.json(), True
    else:
        print(f"Failed to fetch data for vehicle ID {vehicle_id}. Status code: {response.status_code}")
        return None, False

# Function to publish bus data to Pub/Sub
def publish_data(data, topic_path):
    count = 0
    # Iterate over each record in the JSON data
    for record in data:
        # Convert the record to a JSON string
        record_str = json.dumps(record)
        # Publish the JSON string as a message to Pub/Sub
        future = publisher.publish(topic_path, record_str.encode("utf-8"))
        if future:
            count += 1
    print(f"Published {count} messages for Bus data.")

# Create an empty list to store individual DataFrames for bus data
bus_data = []

# Counters for successful and failed requests
successful_requests = 0
failed_requests = 0

# Iterate over each vehicle ID for bus data
for vehicle_id in vehicle_ids:
    # Fetch data for the vehicle ID
    fetched_data, success = fetch_bus_data(vehicle_id)
    if success:
        successful_requests += 1
        if fetched_data:
            # Append fetched data to bus_data list
            bus_data.extend(fetched_data)
    else:
        failed_requests += 1

# Print the number of successful and failed requests
print(f"Successful requests: {successful_requests}")
print(f"Failed requests: {failed_requests}")

# Concatenate all DataFrames for bus data
combined_bus_df = pd.DataFrame(bus_data)

# Create a directory if it doesn't exist
bus_directory = f"final_combined_receiver_data/{current_date}/bus_data"
if not os.path.exists(bus_directory):
    os.makedirs(bus_directory)
