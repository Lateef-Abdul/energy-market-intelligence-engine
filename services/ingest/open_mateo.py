import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 52.52,
	"longitude": 13.41,
	"hourly": "temperature_2m",
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["temperature_2m"] = hourly_temperature_2m

hourly_dataframe = pd.DataFrame(data = hourly_data)

print("\nHourly data\n", hourly_dataframe)

# 2. Azure Storage Configuration
# IMPORTANT: Replace with your actual connection string or use environment variables
# It will look something like: "DefaultEndpointsProtocol=https;AccountName=youraccount;..."
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") 
CONTAINER_NAME = "landing"
BLOB_NAME = "data/my_dataframe_data.csv"

def upload_dataframe_to_blob(dataframe: pd.DataFrame, connection_string: str, container_name: str, blob_name: str):
    """
    Converts a pandas DataFrame to a CSV string and uploads it as a blob.
    """
    if not connection_string:
        raise ValueError("Azure Storage Connection String is not set.")
    
    try:
        # 1. Convert DataFrame to a CSV string
        # index=False prevents pandas from writing the DataFrame index to the CSV file
        csv_data = dataframe.to_csv(index=False, encoding='utf-8')

        # 2. Get the Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # 3. Get the Container Client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Optional: Check if the container exists, create if it doesn't
        if not container_client.exists():
            print(f"Container '{container_name}' not found. Creating it...")
            container_client.create_container()
            
        # 4. Get the Blob Client and upload the data
        blob_client = container_client.get_blob_client(BLOB_NAME)
        
        print(f"Uploading data to {CONTAINER_NAME}/{BLOB_NAME}...")
        
        # Upload the CSV string data
        blob_client.upload_blob(csv_data, overwrite=True)
        
        print("Upload complete!")

    except Exception as e:
        print(f"An error occurred: {e}")

# Execute the upload function
upload_dataframe_to_blob(hourly_dataframe, CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME)