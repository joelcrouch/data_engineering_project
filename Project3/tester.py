from io import StringIO
import psycopg2
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import os
import json
import pandas as pd
from dateutil import parser

DBname = "postgres"
DBuser = "postgres"
DBpwd = "1234"

project_id = "data-engineering-spring-2024"
#project_id = "dataeng-spring-2024"
subscription_id_1 = "my-sub"
subscription_id_2 = "bus-stop-sub"
subscriber = pubsub_v1.SubscriberClient()

subscription_path_1 = subscriber.subscription_path(project_id, subscription_id_1)
subscription_path_2 = subscriber.subscription_path(project_id, subscription_id_2)

bus_data_list = []
stop_data_list = []

def callback_1(message: pubsub_v1.subscriber.message.Message) -> None:
    response_message = json.loads(message.data.decode('utf-8'))
    bus_data_list.append(response_message)
    print("Receiving message from subscription 1.....")
    message.ack()

def callback_2(message: pubsub_v1.subscriber.message.Message) -> None:
    response_message = json.loads(message.data.decode('utf-8'))
    stop_data_list.append(response_message)
    print("Receiving message from subscription 2.....")
    message.ack()

streaming_pull_future_1 = subscriber.subscribe(
    subscription_path_1, callback=callback_1)
streaming_pull_future_2 = subscriber.subscribe(
    subscription_path_2, callback=callback_2)

print(f"Listening for messages on {subscription_path_1} and {subscription_path_2}..\n")

with subscriber:
    try:
        streaming_pull_future_1.result(timeout=40.0)
        streaming_pull_future_2.result(timeout=40.0)
    except TimeoutError:
        streaming_pull_future_1.cancel()
        streaming_pull_future_1.result()
        streaming_pull_future_2.cancel()
        streaming_pull_future_2.result()

# Create DataFrames with the lists
bus_data = pd.DataFrame(bus_data_list)
stop_data = pd.DataFrame(stop_data_list)
print(bus_data)
print(stop_data)
stop_data.rename(columns={'PDX_TRIP': 'trip_id',
                         'route_number': 'route_id',
                         'vehicle_number': 'vehicle_id',
                         'service_key': 'service_key',
                         'direction': 'direction'}, inplace=True)
# Print the DataFrame with renamed columns
print(bus_data)


print(stop_data)

# Get the current date
current_date = datetime.now().strftime('%d-%m')

# Define the directory structure
base_dir = "combined_receiver_data"
date_dir = os.path.join(base_dir, current_date)
bus_data_dir = os.path.join(date_dir, "bus_data")
stop_data_dir = os.path.join(date_dir, "stop_data")

# Create directories if they don't exist
os.makedirs(bus_data_dir, exist_ok=True)
os.makedirs(stop_data_dir, exist_ok=True)

# Save DataFrames to CSV files
bus_data_file = os.path.join(bus_data_dir, "bus_data.json")
stop_data_file = os.path.join(stop_data_dir, "stop_data.json")

bus_data.to_json(bus_data_file, orient='records', lines=True, index=False)
stop_data.to_json(stop_data_file, orient='records', lines=True,  index=False)



print(f"Data saved to {bus_data_file} and {stop_data_file}")

pd.options.display.max_columns = None
#bus_data= pd.read_json('bus3736.json')
#print(bus_data)

bus_data.drop_duplicates(inplace=True)
print(bus_data)


def create_timestamp(row):
    opd_date = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
    act_time = timedelta(seconds=row['ACT_TIME'])
    timestamp = opd_date + act_time
    return pd.Timestamp(timestamp)

bus_data['TIMESTAMP'] = bus_data.apply(create_timestamp, axis=1)
bus_data['DATE_UPDATED'] = pd.to_datetime(bus_data['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

# Extract day of the week
bus_data['DAY_OF_WEEK'] = bus_data['DATE_UPDATED'].dt.dayofweek

# Map day of the week to name
name_of_days = {0: 'Weekday', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Saturday', 6: 'Sunday'}
bus_data['DAY_NAME'] = bus_data['DAY_OF_WEEK'].map(name_of_days)
bus_data.drop('OPD_DATE', axis=1, inplace=True)

print("\n converted tmestamp an ddropped opd_date\n")
print(bus_data)

bus_data.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP', 'VEHICLE_ID'], inplace=True)
print("\nsorted by trip, time and vehicle id\n")
bus_data=bus_data.reset_index(drop=True)
print(bus_data)

print("\nCalculate and add speed\n")
bus_data['SPEED'] = bus_data.groupby('EVENT_NO_TRIP')['METERS'].diff() /bus_data.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()
# # Count the number of NaN values in the SPEED column
# number_of_nan = bus_data['SPEED'].isna().sum()

# # Print the result=>all new trips so set Nan to zero
# print(f"Number of NaN values in SPEED: {number_of_nan}")
# def print_surrounding_rows(df, column_name, window=2):
#   """
#   Prints the immediate window rows surrounding each NaN value in a column.

#   Args:
#       df (pandas.DataFrame): The DataFrame to analyze.
#       column_name (str): The name of the column to check for NaN values.
#       window (int, optional): The number of rows before and after the NaN value to print. Defaults to 2.
#   """
#   for index, row in df.iterrows():
#     if pd.isna(row[column_name]):
#       # Get starting and ending indices for surrounding rows
#       start_index = max(0, index - window)
#       end_index = min(index + window + 1, len(df))
#       # Print surrounding rows
#       print(f"\nRow {index} (NaN):")
#       print(df.iloc[start_index:end_index])

# # Apply the function to bus_data
# print_surrounding_rows(bus_data.copy(), 'SPEED', window=2)
bus_data['SPEED'] = bus_data['SPEED'].fillna(0)  # Replace all NaN values with 0
bus_data['SPEED'] = bus_data['SPEED'].round(3)
print(bus_data)
bus_data['SPEED'] = bus_data['SPEED'].clip(lower=0)  # No negative speeds

bus_data = bus_data.dropna(subset=['GPS_LATITUDE', 'GPS_LONGITUDE'])
#drop nan lat/long
#bus_data = bus_data[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP, DAY_OF_WEEK, DAY_NAME']].rename(
   # columns={'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed', 'EVENT_NO_TRIP': 'trip_id', 'DAY_OF_WEEK': 'day_of_week', 'DAY_NAME':'day_name'})
print(bus_data)
desired_columns = ['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP', 'DAY_OF_WEEK', 'DAY_NAME']
bus_data=bus_data[desired_columns]
print(bus_data)
column_renaming = {'TIMESTAMP': 'tstamp',
                   'GPS_LATITUDE': 'latitude',
                   'GPS_LONGITUDE': 'longitude',
                   'SPEED': 'speed',
                   'EVENT_NO_TRIP': 'trip_id',  
                   'DAY_OF_WEEK': 'day_of_week',
                   'DAY_NAME': 'day_name'}

# Rename the columns in bus_data
bus_data = bus_data.rename(columns=column_renaming)
print(bus_data)


# Establish a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database=DBname,
    user=DBuser,
    password=DBpwd
    )

cursor=conn.cursor()

create_trip_table = """
CREATE TABLE IF NOT EXISTS trip (
    trip_id VARCHAR(255) PRIMARY KEY,
    route_id INTEGER,
    vehicle_id VARCHAR(255),
    service_key VARCHAR(255),
    direction VARCHAR(255)
);
"""

create_breadcrumb_table = """
CREATE TABLE IF NOT EXISTS breadcrumb (
    tstamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    speed FLOAT,
    trip_id VARCHAR(255),
    FOREIGN KEY (trip_id) REFERENCES trip(trip_id)
);
"""

def create_tables(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(create_trip_table)
        cursor.execute(create_breadcrumb_table)
        conn.commit()
        print("Tables created successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error creating tables: %s" % error)
        conn.rollback()
    finally:
        cursor.close()

create_tables(conn)

def copy_from_trip(conn, df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, 'trip', sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Loading of Trip table done")
    cursor.close()

def copy_from_breadcrumb(conn, df):
       
    # save dataframe to an in memory buffer
    buffer = StringIO()

    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, 'breadcrumb', sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Loading of breadcrumb table done")
    cursor.close()


print("Columns in stop_data:")
print(stop_data.columns)

print("\nColumns in bus_data:")
print(bus_data.columns)



def validate_trip_ids(bus_data, stop_data):
    bus_trip_ids = bus_data['trip_id'].unique()
    stop_trip_ids = stop_data['trip_id'].unique()
    
    # Find trip_ids in bus_data not present in stop_data
    missing_trip_ids = set(bus_trip_ids) - set(stop_trip_ids)
    
    if missing_trip_ids:
        print("The following 'trip_id's in bus_data are missing in stop_data:")
        print(missing_trip_ids)
    else:
        print("All 'trip_id's in bus_data are present in stop_data.")
    
    # Find trip_ids that are present in both bus_data and stop_data
    matching_trip_ids = set(bus_trip_ids) & set(stop_trip_ids)
    
    if matching_trip_ids:
        print("The following 'trip_id's are present in both bus_data and stop_data:")
        print(matching_trip_ids)
    else:
        print("No matching 'trip_id's found in bus_data and stop_data.")


validate_trip_ids(bus_data, stop_data)

def filter_bus_data(bus_data, stop_data):
    stop_trip_id=set(stop_data['trip_id'].unique())
    filtered_bus_data=bus_data[bus_data['trip_id'].isin(stop_trip_id)]
    return filtered_bus_data

filtered_bus_data=filter_bus_data(bus_data, stop_data)

def remove_duplicate_trip_ids(stop_data):
    stop_data = stop_data.drop_duplicates(subset='trip_id', keep='first')
    return stop_data

stop_data = remove_duplicate_trip_ids(stop_data)
bus_data_filtered = bus_data.drop(columns=['day_of_week', 'day_name'])
print("Columns in stop_data:")
print(stop_data.columns)

print("\nColumns in filtered bus_data:")
print(filtered_bus_data.columns)
weekday_mask = filtered_bus_data['trip_id'] == "weekday"
number_of_weekday_rows = weekday_mask.sum()

print(f"Number of rows with 'weekday' in trip_id: {number_of_weekday_rows}")
filter_bus_data= filtered_bus_data.drop(weekday_mask.index)
copy_from_trip(conn, stop_data)
copy_from_breadcrumb(conn, filtered_bus_data)
