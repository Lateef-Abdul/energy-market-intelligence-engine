import requests
import pandas as pd
import xml.etree.ElementTree as ET
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# List of years to process
YEARS = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

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


# Process each year
for Year in YEARS:
    print(f"\n--- Processing Year {Year} ---")
    
    # Construct URL dynamically based on Year variable
    period_start = f"{Year}01010000"
    period_end = f"{Year}12312300"
    url = f"https://web-api.tp.entsoe.eu/api?securityToken={ENTSOE_API_KEY}&documentType=A69&processType=A01&in_Domain=10YBE----------2&periodStart={period_start}&periodEnd={period_end}"
    
    response = requests.get(url)
    xml_text = response.text

    # Parse XML → DataFrame
    root = ET.fromstring(xml_text)

    # Namespace handling
    ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}

    # Extract common metadata
    doc_id = root.find('ns:mRID', ns).text
    revision = root.find('ns:revisionNumber', ns).text
    doc_type = root.find('ns:type', ns).text
    process_type = root.find('ns:process.processType', ns).text
    created_time = root.find('ns:createdDateTime', ns).text
    start_time = root.find('ns:time_Period.timeInterval/ns:start', ns).text
    end_time = root.find('ns:time_Period.timeInterval/ns:end', ns).text

    # Initialize list to store all points
    data_rows = []

    # Loop over all TimeSeries
    for ts in root.findall('ns:TimeSeries', ns):
        ts_id = ts.find('ns:mRID', ns).text
        business_type = ts.find('ns:businessType', ns).text
        psr_type = ts.find('ns:MktPSRType/ns:psrType', ns).text
        unit = ts.find('ns:quantity_Measure_Unit.name', ns).text

        
        # Loop over each Period (usually one per TimeSeries)
        for period in ts.findall('ns:Period', ns):
            period_start = period.find('ns:timeInterval/ns:start', ns).text
            period_end = period.find('ns:timeInterval/ns:end', ns).text
            resolution = period.find('ns:resolution', ns).text
            
            # Loop over all points
            for point in period.findall('ns:Point', ns):
                position = int(point.find('ns:position', ns).text)
                quantity = float(point.find('ns:quantity', ns).text)
                
                data_rows.append({
                    'document_id': doc_id,
                    'revision': revision,
                    'doc_type': doc_type,
                    'process_type': process_type,
                    'created_time': created_time,
                    'time_series_id': ts_id,
                    'business_type': business_type,
                    'psr_type': psr_type,
                    'unit': unit,
                    'period_start': period_start,
                    'period_end': period_end,
                    'resolution': resolution,
                    'position': position,
                    'quantity': quantity
                })

    # Convert to DataFrame
    df = pd.DataFrame(data_rows)


    # Save as Parquet
    output_file = f"/data/entsoe_generation_ws{Year}.parquet"
    df.to_parquet(output_file, index=False)

    # Upload to Azure Blob Storage
    BLOB_NAME = f"data/entsoe/wind_solar_forecast/entsoe_data_ws{Year}.parquet"
    upload_file_to_blob(output_file, CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME)
 