import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine

def validate_data(df):
    # Assert 'TIMESTAMP' column exists
    assert 'TIMESTAMP' in df.columns, "Missing 'TIMESTAMP' field in received data."

    # Assert 'latitude' values are within the valid range
    assert ((df['GPS_LATITUDE'] >= -90) & (df['GPS_LATITUDE'] <= 90)).all(), "Latitude value is out of range."

    # Assert 'longitude' values are within the valid range
    assert ((df['GPS_LONGITUDE'] >= -180) & (df['GPS_LONGITUDE'] <= 180)).all(), "Longitude value is out of range."

    # Assertion 4: Ensure 'SPEED' is non-negative
    assert (df['SPEED'] >= 0).all(), "Speed value cannot be negative."

    # Assertion 5: Ensure 'trip_id' exists in the data
    assert 'EVENT_NO_TRIP' in df.columns, "Missing 'EVENT_NO_TRIP' field in received data."

    # Assertion 6: Ensure 'vehicle_id' exists in the data
    assert 'VEHICLE_ID' in df.columns, "Missing 'VEHICLE_ID' field in received data."
    # Assertion 7: Ensure 'EVENT_NO_TRIP' column doesn't contain null values
    assert not df['EVENT_NO_TRIP'].isnull().any(), "'EVENT_NO_TRIP' column contains null values."

# Assertion 8 Ensure 'VEHICLE_ID' column doesn't contain null values
    assert not df['VEHICLE_ID'].isnull().any(), "'VEHICLE_ID' column contains null values."
    
    #9
    calculated_speed = df['dMETERS'] / df['dTIMESTAMP'].dt.total_seconds()
    assert df['SPEED'][1:].equals(calculated_speed[1:]), "Speed values are inconsistent with calculated speed."
    
    #10
    #assert not df.isnull().values.any(), "DataFrame contains missing values."
    assert not df.duplicated(subset=['TIMESTAMP']).any(), "Duplicate timestamps found in the DataFrame."



    
    

# Define the directory path and file name
directory = "2024-05-11"
file_name = "breadcrumb_data_3748.json"
pd.set_option('display.max_columns', None)
# Construct the full file path
file_path = f"{directory}/{file_name}"

# try:
#     # Read the JSON file into a DataFrame
#     df = pd.read_json(file_path)
#     print(df.head())
# except Exception as e:
#     print("Error reading JSON file:", e)
try:
    # Read the JSON file into a DataFrame
    df = pd.read_json(file_path)
    
    # Define a function to calculate timestamp using row number as a base time
    def calculate_timestamp(row):
        base_time = datetime.strptime(df.iloc[0]['OPD_DATE'], '%d%b%Y:%H:%M:%S')  # Using the first row's OPD_DATE as base time
        return base_time + timedelta(seconds=row['ACT_TIME'])

    # Apply the function to create a new column 'TIMESTAMP'
    df['TIMESTAMP'] = df.apply(calculate_timestamp, axis=1)
    #speed part
    # Calculate the time differences and derive speed
    df['dTIMESTAMP'] = df['TIMESTAMP'].diff()
    df['dMETERS'] = df['METERS'].diff()
    df['SPEED'] = df['dMETERS'] / df['dTIMESTAMP'].dt.total_seconds()

    # Set the first SPEED value based on the second row (avoid NaN)
    df.at[0, 'SPEED'] = df.at[1, 'SPEED']

    # Drop unnecessary columns 'ACT_TIME' and 'OPD_DATE'
    df = df.drop(columns=['ACT_TIME', 'OPD_DATE', 'GPS_SATELLITES', 'GPS_HDOP'])
    # Validate the data
    validate_data(df)
    print("Data validation successful. All assertions passed.")
    print(df.head())
except Exception as e:
    print("Error reading JSON file:", e)
    
    


# Define database connection parameters
dbname = "postgres"
user = "postgres"
password = "1234"


# Create a connection string
connection_string = f"postgresql://{user}:{password}/{dbname}"

# Create SQLAlchemy engine
engine = create_engine(connection_string)

# Define the DataFrame with the columns you want to copy to the BreadCrumb table
df_breadcrumb = df[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']]

# Define the DataFrame with the columns you want to copy to the Trip table
#df_trip = df[['EVENT_NO_TRIP', 'ROUTE_ID', 'VEHICLE_ID', 'SERVICE_KEY', 'DIRECTION']]

# Write the DataFrames to the database tables
try:
    # Write the DataFrame to the BreadCrumb table
    df_breadcrumb.to_sql(name='BreadCrumb', con=engine, if_exists='append', index=False)

    # Write the DataFrame to the Trip table
    #df_trip.to_sql(name='Trip', con=engine, if_exists='append', index=False)

    print("Data inserted successfully.")
except Exception as e:
    print("Error inserting data:", e)
