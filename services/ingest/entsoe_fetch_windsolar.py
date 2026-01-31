import requests
import pandas as pd
import xml.etree.ElementTree as ET
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
import os
import time

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# List of years to process
YEARS = [2025] #2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

# read API key from environment variable
ENTSOE_API_KEY = os.environ.get("ENTSOE_API_KEY")
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "landing"

def upload_file_to_blob(file_path: str, connection_string: str, container_name: str, blob_name: str):
    """
    Uploads a local file (Parquet/CSV/etc.) to Azure Blob Storage.
    """
    if not connection_string:
        raise ValueError("Azure Storage Connection String is not set.")
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            print(f"Container '{container_name}' not found. Creating it...")
            container_client.create_container()
        
        blob_client = container_client.get_blob_client(blob_name)
        print(f"Uploading {file_path} to {container_name}/{blob_name}...")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        print("Upload complete!")
    except Exception as e:
        print(f"An error occurred: {e}")



ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
for Year in YEARS:
        print(f"\n--- Processing Year {Year} ---")
        
        # Initialize list to store all points for the ENTIRE year
        year_data_rows = []
        
        # Define start and end dates for the loop
        current_date = datetime(Year, 1, 1)
        end_date = datetime(Year, 2, 20)

        while current_date <= end_date:
            # Format: YYYYMMDD2200
            period_start = current_date.strftime("%Y%m%d0000")
            period_end = (current_date + timedelta(days=1)).strftime("%Y%m%d0000")
            
            print(f"  Fetching: {period_start} to {period_end}")

            url = (
                f"https://web-api.tp.entsoe.eu/api?securityToken={ENTSOE_API_KEY}"
                f"&documentType=A69&processType=A01&in_Domain=10Y1001A1001A83F"
                f"&periodStart={period_start}&periodEnd={period_end}"
            )

            try:
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"    Error: Received status {response.status_code}")
                    current_date += timedelta(days=1)
                    continue

                xml_text = response.text
                root = ET.fromstring(xml_text)

                # Metadata (Top level)
                # Some fields might be missing in error responses, using find logic
                doc_id = root.find('ns:mRID', ns).text if root.find('ns:mRID', ns) is not None else "N/A"
                
                # Loop over all TimeSeries
                for ts in root.findall('ns:TimeSeries', ns):
                    ts_id = ts.find('ns:mRID', ns).text
                    business_type = ts.find('ns:businessType', ns).text
                    psr_type = ts.find('ns:MktPSRType/ns:psrType', ns).text
                    unit = ts.find('ns:quantity_Measure_Unit.name', ns).text

                    # Loop over each Period
                    for period in ts.findall('ns:Period', ns):
                        p_start = period.find('ns:timeInterval/ns:start', ns).text
                        p_end = period.find('ns:timeInterval/ns:end', ns).text
                        res = period.find('ns:resolution', ns).text
                        
                        for point in period.findall('ns:Point', ns):
                            year_data_rows.append({
                                'year_context': Year,
                                'time_series_id': ts_id,
                                'business_type': business_type,
                                'psr_type': psr_type,
                                'unit': unit,
                                'period_start': p_start,
                                'period_end': p_end,
                                'resolution': res,
                                'position': int(point.find('ns:position', ns).text),
                                'quantity': float(point.find('ns:quantity', ns).text)
                            })

            except Exception as e:
                print(f"    Failed to process day {period_start}: {e}")
            
            # Increment day and add a tiny delay to be polite to the API
            current_date += timedelta(days=1)
            time.sleep(0.2)

        # After the while loop finishes for the year, create DataFrame
        if year_data_rows:
            df = pd.DataFrame(year_data_rows)

            # Save as Parquet
            output_file = f"/home/niitiin/projects/energy-market-intelligence-engine/services/ingest/data/entsoe_generation_ws{Year}.parquet"
            df.to_parquet(output_file, index=False)
            print(f"--- Saved {len(df)} rows to {output_file} ---")

            # Upload to Azure Blob Storage
            #BLOB_NAME = f"data/entsoe/wind_solar_forecast/entsoe_data_ws{Year}.parquet"
            #upload_file_to_blob(output_file, CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME)
        else:
            print(f"No data collected for Year {Year}")
