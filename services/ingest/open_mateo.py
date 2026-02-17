import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
import datetime # <-- Import needed for dynamic date

# Load environment variables from .env file
load_dotenv()

# --- Azure Storage Configuration (Loaded Once) ---
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") 
CONTAINER_NAME = "landing"
BASE_BLOB_PATH_HORLY = "data/weather_data/hourly/weather_data"
BASE_BLOB_PATH_DAILY = "data/weather_data/daily/weather_data"

if not CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set. Please check your .env file.")

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def upload_dataframe_to_blob(dataframe: pd.DataFrame, connection_string: str, container_name: str, blob_name: str):
    """
    Converts a pandas DataFrame to a Parquet file in memory and uploads it as a blob.
    """
    try:
        # --- KEY CHANGE 1: Convert DataFrame to Parquet bytes ---
        # to_parquet with engine='pyarrow' saves the data to a byte-stream (buffer)
        parquet_data = dataframe.to_parquet(index=False, engine='pyarrow')

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        if not container_client.exists():
            print(f"Container '{container_name}' not found. Creating it...")
            container_client.create_container()
            
        blob_client = container_client.get_blob_client(blob_name)
        
        print(f"Uploading data to {container_name}/{blob_name}...")
        
        # Upload the Parquet byte data
        blob_client.upload_blob(parquet_data, overwrite=True)
        
        print("Upload complete!")

    except Exception as e:
        print(f"An error occurred during upload: {e}")


# --- 3. Loop through the years and fetch data ---
years_to_fetch = range(2015, 2026) 
# The actual end date from the API error was 2025-12-08 
today = datetime.date(2025, 12, 8) 

for year in years_to_fetch:
    print(f"\n--- Processing Year: {year} ---")
    
    start_date = f"{year}-01-01"
    
    # Dynamic End Date Logic (Fixes previous error)
    if year == today.year:
        # If it's the current year, set the end date to today's available data
        end_date = today.strftime("%Y-%m-%d")
        print(f"Current year detected. End date set to: {end_date}")
    else:
        # For historical years (2023, 2024), request the full year
        end_date = f"{year}-12-31" 

    # Setup API Parameters for the current year
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
        "daily": ["sunrise", "sunset"],
		"hourly": ["temperature_2m", "cloud_cover", "wind_speed_100m", "surface_pressure", "wind_gusts_10m", "wind_direction_100m", "direct_normal_irradiance"],
    }
    
    try:
        # Fetch Data and create hourly_dataframe (unchanged from your original)
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(1).ValuesAsNumpy()
        hourly_wind_speed_100m = hourly.Variables(2).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(3).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_direction_100m = hourly.Variables(5).ValuesAsNumpy()
        hourly_direct_normal_irradiance = hourly.Variables(6).ValuesAsNumpy()
        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
        hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
        hourly_data["direct_normal_irradiance"] = hourly_direct_normal_irradiance
        hourly_dataframe = pd.DataFrame(data = hourly_data)
        print(f"Data fetched for {year}. Rows: {len(hourly_dataframe)}")
        
        # --- KEY CHANGE 2: Update the blob name extension ---
        BLOB_NAME_YEARLY = f"{BASE_BLOB_PATH_HORLY}_{year}_hourly.parquet"
        
        # Execute the upload function
        upload_dataframe_to_blob(hourly_dataframe, CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME_YEARLY)

        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_sunrise = daily.Variables(0).ValuesInt64AsNumpy()
        daily_sunset = daily.Variables(1).ValuesInt64AsNumpy()

        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}

        daily_data["sunrise"] = daily_sunrise
        daily_data["sunset"] = daily_sunset

        daily_dataframe = pd.DataFrame(data = daily_data)
        print(f"Data fetched for {year}. Rows: {len(daily_dataframe)}")
        
        BLOB_NAME_YEARLY = f"{BASE_BLOB_PATH_DAILY}_{year}_daily.parquet"
        upload_dataframe_to_blob(daily_dataframe, CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME_YEARLY)
        
    except Exception as e:
        print(f"Error fetching or processing data for year {year}: {e}")

print("\n--- All years processed and uploaded. ---")