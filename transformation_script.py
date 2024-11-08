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
import glob
from datetime import datetime
s3_client = boto3.client('s3')

def log():
    logging.basicConfig(
        filename='C:/Users/vasuv/OneDrive/Desktop/DE/AWSBlockChain/logs/python_script_logs/transformation.log',  
        level=logging.INFO,   
        format='%(asctime)s - %(levelname)s - %(message)s'  # Format of the log messages
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def load_config():
    # Load configuration from JSON file
    log()
    config_file_path = 'C:/Users/vasuv/OneDrive/Desktop/DE/AWSBlockChain/python_pipeline/transaction/config.json'

    with open(config_file_path) as config_file:
        config = json.load(config_file)
    # Get today's date in the required format (YYYY-MM-DD)
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_date_str = str(current_date)
    logging.info("current_date for which this script running is :%s",str(current_date_str))
    return config,current_date

def file_read():
    # Here reading only a single file
    config,current_date = load_config()
    file_pattern = os.path.join(config['local_directory'], f'combined_bitcoin_{current_date}.parquet')
    logging.info("file pattern is %s",str(file_pattern))
    matching_files = file_pattern
    #print('matching file name is :',matching_files)

    if matching_files:
        file_name=matching_files
        logging.info("file name to be read is :%s",str(file_name))
        combined_file_path_df=pd.read_parquet(file_name)
    else:
        logging.info("No files to read")
        return []
    return combined_file_path_df

def to_json_compatible(value):
    if isinstance(value, np.ndarray):
        return to_json_compatible(value.tolist())  # Convert ndarray to list and recurse
    elif isinstance(value, list):
        return [to_json_compatible(item) for item in value]  # Recurse into list items
    elif isinstance(value, dict):
        return {k: to_json_compatible(v) for k, v in value.items()}  # Recurse into dict items
    elif isinstance(value, (int, float, str, bool, type(None))):
        return value  # These types are JSON-serializable
    else:
        return str(value)  # Convert other types to strings

def transformation():
    combined_file_path_df = file_read()
    combined_file_path_df.dropna(inplace=True)
    extracted_combined_file_path_df=combined_file_path_df[['version','size','block_number','virtual_size','input_count','output_count','is_coinbase','fee','last_modified','date','block_timestamp','output_value','input_value','inputs','outputs']]
    extracted_input_output = combined_file_path_df[['version','size','virtual_size','inputs','outputs']]

    extracted_combined_file_path_df['date'] = pd.to_datetime(extracted_combined_file_path_df['date'])
    extracted_combined_file_path_df['block_timestamp'] = pd.to_datetime(extracted_combined_file_path_df['block_timestamp'])

    extracted_combined_file_path_df[['version', 'size', 'block_number', 'virtual_size', 'input_count', 'output_count', 'fee', 'output_value', 'input_value']] = extracted_combined_file_path_df[['version', 'size', 'block_number', 'virtual_size', 'input_count', 'output_count', 'fee', 'output_value', 'input_value']].apply(pd.to_numeric)
    extracted_input_output[['version','size','virtual_size']].apply(pd.to_numeric)

    extracted_combined_file_path_df['output_size_ratio'] = extracted_combined_file_path_df['output_value'] / extracted_combined_file_path_df['size']
    extracted_combined_file_path_df['fee_input_ratio'] = extracted_combined_file_path_df['fee'] / extracted_combined_file_path_df['input_value']
    extracted_derived_df = extracted_combined_file_path_df[['output_size_ratio','fee_input_ratio']] 
    extracted_combined_file_path_df = extracted_combined_file_path_df[(extracted_combined_file_path_df['input_value'] > 0) & (extracted_combined_file_path_df['output_value'] > 0) & (extracted_combined_file_path_df['fee'] >= 0)]
    extracted_input_output = combined_file_path_df[['version','size','virtual_size','inputs','outputs']]
    extracted_input_output['inputs'] = extracted_input_output['inputs'].apply(to_json_compatible).apply(json.dumps)
    extracted_input_output['outputs'] = extracted_input_output['outputs'].apply(to_json_compatible).apply(json.dumps) 
    extracted_combined_file_path_df.to_csv('extracted_combined_file_path_df.csv')
    logging.info("Creating extracted_combined_file_path_df.csv file")
    extracted_input_output.to_csv('extracted_input_output.csv')
    logging.info("Creating extracted_input_output.csv file")
    extracted_derived_df.to_csv('extracted_derived_df.csv')
    logging.info("Creating extracted_derived_df.csv file")
    #return extracted_combined_file_path_df,extracted_input_output

if __name__ == "__main__":
    transformation()
