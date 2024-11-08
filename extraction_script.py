import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, JSON
import boto3
import os
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging
import json
from datetime import datetime

s3_client = boto3.client('s3')

logging.basicConfig(
    filename='C:/Users/vasuv/OneDrive/Desktop/DE/AWSBlockChain/logs/python_script_logs/extraction.log',  
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format of the log messages
)

# List all Parquet files in the S3 directory
def list_parquet_files(bucket_name, prefix):
    # List files in the specified bucket and prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    # Extract the list of parquet files
    parquet_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]
    logging.info('list_parquet_files method is executed sucessfully')
    return parquet_files

# Download and combine Parquet files
def download_and_combine_parquet(bucket_name, s3_prefix, local_directory, combined_file_path):
    # List all Parquet files in the S3 directory
    parquet_files = list_parquet_files(bucket_name, s3_prefix)
    # List to hold all dataframes
    data_frames = []

    for s3_file in parquet_files:
        local_file = os.path.join(local_directory, os.path.basename(s3_file))
        
        # Download each Parquet file
        s3_client.download_file(bucket_name, s3_file, local_file)
        
        # Load Parquet file into DataFrame
        df = pd.read_parquet(local_file)
        data_frames.append(df)
        
        # Optionally, remove the downloaded local file to save space
        os.remove(local_file)

    # Combine all dataframes
    combined_df = pd.concat(data_frames, ignore_index=True)
    # Convert combined dataframe to a Parquet file
    table = pa.Table.from_pandas(combined_df)
    pq.write_table(table, combined_file_path)
    logging.info(f"Combined Parquet file saved at {combined_file_path}")
    logging.info("download and combine parquet is executed successfully.")

# Load configuration from JSON file
config_file_path = 'C:/Users/vasuv/OneDrive/Desktop/DE/AWSBlockChain/python_pipeline/transaction/config.json'

with open(config_file_path) as config_file:
    config = json.load(config_file)

# Get today's date in the required format (YYYY-MM-DD)
current_date = datetime.now().strftime('%Y-%m-%d')

# Set parameters from the config file
bucket_name = config['bucket_name']
s3_prefix = config['s3_prefix'] + current_date + '/'
local_directory = config['local_directory']
combined_file_path = os.path.join(local_directory, f'combined_bitcoin_{current_date}.parquet')

logging.info("Bucket Name:%s", bucket_name)
logging.info("S3 Prefix:%s", s3_prefix)
logging.info("Local Directory:%s", local_directory)
logging.info("Combined File Path:%s", combined_file_path)

 
# Run the process
download_and_combine_parquet(bucket_name, s3_prefix, local_directory, combined_file_path)