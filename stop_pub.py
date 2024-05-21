import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from io import StringIO
from google.cloud import pubsub_v1
#pubsub boilerplate
# Pub/Sub configuration
project_id = "data-engineering-spring-2024"
topic_id = "bus-stop"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
# List of vehicle IDs
vehicle_ids = [
    4212, 3802, 3249, 3507, 4234, 4227, 2938, 3929, 4036, 2907, 3007, 4528, 4237, 3131, 4052, 3616, 3959, 3634, 3328, 3264, 3218, 2916, 2920, 3748, 4205, 4001, 2903, 3624, 3631, 3110, 2908, 4202, 3031, 3518, 3226, 3022, 3262, 2936, 3626, 3525, 3245, 3615, 3720, 3201, 3153, 3958, 3932, 3253, 3324, 2930, 3921, 3543, 3524, 3223, 3010, 3001, 4020, 3318, 3241, 3509, 3718, 3647, 3123, 3267, 3413, 3157, 3639, 3213, 4010, 3515, 2902, 3151, 2918, 3013, 4529, 3122, 3027, 4024, 3221, 3549, 3136, 3142, 3158, 3404, 3236, 4216, 3202, 3711, 3502, 3015, 3134, 3713, 3531, 3127, 3619, 4209, 4207, 3163, 3629, 3736
]

# Get the current date
current_date = datetime.now().strftime("%m-%d")

# Create an empty list to store individual DataFrames
data = []

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
            
            #new IO change
            html_string=str(table)
            html_io = StringIO(html_string)

            # Read the table into a DataFrame
            #df = pd.read_html(str(table))[0]
            df = pd.read_html(html_io)[0]
           # and change the above 
            # Add a column for the trip number
            df['PDX_TRIP'] = trip_number
            
            # Append DataFrame to data list
            data.append(df)
        if data:
            # Concatenate all DataFrames for the current vehicle_id
            vehicle_df = pd.concat(data, ignore_index=True)

            # Convert the DataFrame to JSON
            json_data = vehicle_df.to_json(orient='records', lines=True)

            # Send each line of the JSON data to Pub/Sub
            for line in json_data.splitlines():
                future = publisher.publish(topic_path, line.encode('utf-8'))
                #print(f"Published message ID: {future.result()}")
    else:
        # Print error message if the request was not successful
        print(f"Error fetching data for vehicle {vehicle_id}: Status code {response.status_code}")



# Concatenate all DataFrames
combined_df = pd.concat(data, ignore_index=True)

# Create a directory if it doesn't exist
directory = f"stop_event_data/{current_date}"
if not os.path.exists(directory):
    os.makedirs(directory)
pd.set_option('display.max_columns', None)  # Display all columns
print(combined_df)
# Define the file path
csv_file_path = f"{directory}/combined_data.csv"
json_file_path=f"{directory}/combined_data.json"
# Save the DataFrame to a CSV file
combined_df.to_csv(csv_file_path, index=False)
combined_df.to_json(json_file_path, orient='records')
json_file_path = f"{directory}/combined_data.json"
print(f"DataFrames saved to: {csv_file_path} and {json_file_path}")

# Read the JSON file and publish each line as a message to Pub/Sub
#TOO big try to do it wihtin the loop
#with open(json_file_path, 'r') as f:
#    for line in f:
#        future = publisher.publish(topic_path, line.encode('utf-8'))

