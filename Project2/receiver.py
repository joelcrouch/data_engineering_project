from io import StringIO
import psycopg2
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import os
import json
import pandas as pd

project_id = "dataeng-spring-2024"
subscription_id = "my-sub"

DBname = "postgres"
DBuser = "postgres"
DBpwd = "smriti"

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)
message_list = []

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    response_message = json.loads(message.data.decode('utf-8'))
    message_list.append(response_message)
    print("Receving message.....")
    message.ack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=40.0)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()


df = pd.DataFrame(message_list)

def validate_data(df):
    # Assert 'TIMESTAMP' column exists
    if 'TIMESTAMP' in df.columns:
        print("Missing 'TIMESTAMP' field in received data.")

    # Assert 'latitude' values are within the valid range
    if ((df['GPS_LATITUDE'] >= -90) & (df['GPS_LATITUDE'] <= 90)).all():
        print("Latitude value is out of range.")

    # Assert 'longitude' values are within the valid range
    if((df['GPS_LONGITUDE'] >= -180) & (df['GPS_LONGITUDE'] <= 180)).all():
        print("Longitude value is out of range.")

    # Assertion 4: Ensure 'SPEED' is non-negative
    if(df['SPEED'] >= 0).all():
        print("Speed value cannot be negative.")

    # Assertion 5: Ensure 'trip_id' exists in the data
    if 'EVENT_NO_TRIP' in df.columns:
        print("Missing 'EVENT_NO_TRIP' field in received data.")

    # Assertion 6: Ensure 'vehicle_id' exists in the data
    if 'VEHICLE_ID' in df.columns:
        print("Missing 'VEHICLE_ID' field in received data.")
    # Assertion 7: Ensure 'EVENT_NO_TRIP' column doesn't contain null values
    if not df['EVENT_NO_TRIP'].isnull().any():
        print("'EVENT_NO_TRIP' column contains null values.")

# Assertion 8 Ensure 'VEHICLE_ID' column doesn't contain null values
    if not df['VEHICLE_ID'].isnull().any():
        print("'VEHICLE_ID' column contains null values.")
    
    #10
    #assert not df.isnull().values.any(), "DataFrame contains missing values."
    if not df.duplicated(subset=['TIMESTAMP']).any():
        print("Duplicate timestamps found in the DataFrame.")


df['DATE_UPDATED'] = pd.to_datetime(
df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

# Extract day of the week
df['DAY_OF_WEEK'] = df['DATE_UPDATED'].dt.dayofweek

# Map day of the week to name
name_of_days = {0: 'Weekday', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Saturday', 6: 'Sunday'}
df['DAY_NAME'] = df['DAY_OF_WEEK'].map(name_of_days)

result_df = df.drop_duplicates(subset=['EVENT_NO_TRIP'], keep='first')
    
# Define function to create timestamp
def create_timestamp(row):
    opd_date = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
    act_time = timedelta(seconds=row['ACT_TIME'])
    timestamp = opd_date + act_time
    return pd.Timestamp(timestamp)

# Apply the function to create the TIMESTAMP column
df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)

df.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP', 'VEHICLE_ID'], inplace=True)

df['SPEED'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff() / df.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()

# Backfill to handle the first record of each trip
df['SPEED'] = df['SPEED'].fillna(method='bfill')

df['SPEED'] = df['SPEED'].clip(lower=0)  # No negative speeds

df['GPS_LATITUDE'] = df['GPS_LATITUDE'].fillna(0.0)

df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].fillna(0.0)

#Data Validation

validate_data(df)

# Add dummy columns with default value
result_df['ROUTE_ID'] = 0
result_df['DIRECTION'] = 'Undefined'

# Select only required columns and rename them
df_trip = result_df[['EVENT_NO_TRIP', 'ROUTE_ID', 'VEHICLE_ID', 'DAY_NAME', 'DIRECTION']].rename(
    columns={'EVENT_NO_TRIP': 'trip_id', 'ROUTE_ID': 'route_id', 'VEHICLE_ID': 'vehicle_id', 'DAY_NAME': 'service_key', 'DIRECTION': 'direction'})
# Select only required columns and rename them
df_breadcrumb = df[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].rename(
    columns={'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed', 'EVENT_NO_TRIP': 'trip_id'})

# Assuming you have a DataFrame called df with columns: 'TIMESTAMP', 'SPEED', and 'VEHICLE_ID'

# Create a new column for the day of the week
df['DAY_OF_WEEK'] = df['TIMESTAMP'].dt.dayofweek

# Map day of the week to 'Weekday' or 'Weekend'
df['DAY_TYPE'] = df['DAY_OF_WEEK'].map({0: 'Weekend', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Weekday', 6: 'Weekend'})


# Establish a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database=DBname,
    user=DBuser,
    password=DBpwd
    )

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

copy_from_trip(conn, df_trip)
copy_from_breadcrumb(conn, df_breadcrumb)
