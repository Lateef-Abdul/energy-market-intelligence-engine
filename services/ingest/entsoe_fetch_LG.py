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
    url = f"https://web-api.tp.entsoe.eu/api?securityToken={ENTSOE_API_KEY}&documentType=A75&processType=A16&in_Domain=10Y1001A1001A83F&periodStart={period_start}&periodEnd={period_end}"
    
    response = requests.get(url)
    xml_text = response.text

    # Parse XML → DataFrame
    root = ET.fromstring(xml_text)
    ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}

    data = []

    for ts in root.findall('.//ns:TimeSeries', ns):
        psr = ts.find('.//ns:psrType', ns)
        psr_text = psr.text if psr is not None else "Unknown"

        period = ts.find('.//ns:Period', ns)
        if period is not None:
            start_str = period.find('ns:timeInterval/ns:start', ns).text
            resolution = period.find('ns:resolution', ns).text

            start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            minutes = 60
            if resolution.startswith("PT") and resolution.endswith("M"):
                try:
                    minutes = int(resolution[2:-1])
                except:
                    minutes = 60
            step = timedelta(minutes=minutes)

            for point in period.findall('ns:Point', ns):
                pos = int(point.find('ns:position', ns).text)
                qty = float(point.find('ns:quantity', ns).text)
                
                timestamp = start_time + (pos - 1) * step

                data.append({
                    "psr_type": psr_text,
                    "datetime": timestamp,
                    "quantity": qty
                })

    df = pd.DataFrame(data)

    # Save as Parquet
    output_file = f"entsoe_generation_{Year}.parquet"
    df.to_parquet(output_file, index=False)

    # Upload to Azure Blob Storage
    BLOB_NAME = f"data/entsoe/load_generation/entsoe_data_{Year}.parquet"
    upload_file_to_blob(output_file, CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME)
