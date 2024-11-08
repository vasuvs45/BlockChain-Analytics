import pandas as pd
#print(pd.__version__)
import numpy as np
from sqlalchemy import create_engine, JSON
#import sqlalchemy
#print(sqlalchemy.__version__)
import boto3
import os
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging
import json
import glob
from datetime import datetime
#from transformation_script import transformation

def log():
    logging.basicConfig(
        filename='C:/Users/vasuv/OneDrive/Desktop/DE/AWSBlockChain/logs/python_script_logs/loading_script.log',  
        level=logging.INFO,   
        format='%(asctime)s - %(levelname)s - %(message)s'  # Format of the log messages
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def db_connect():
    log()
    logging.info("Inside db connect before calling transformation function")
    #extracted_combined_file_path_df,extracted_input_output = transformation()
    extracted_combined_file_path_df = pd.read_csv("extracted_combined_file_path_df.csv")
    logging.info("Reading extracted_combined_file_path_df into dataframe")
    extracted_input_output = pd.read_csv("extracted_input_output.csv")
    logging.info("Reading extracted_input_output into dataframe")
    extracted_derived_df = pd.read_csv("extracted_derived_df.csv")
    logging.info("Reading extracted_derived_df into dataframe")
    #with engine.connect() as connection:
    try:
        logging.info("Inside try block")
        #engine = create_engine('driver://username@localhost:portnumber/db')
        engine = create_engine('postgresql://postgres:postgres@localhost:5432/analytics')
        extracted_combined_file_path_df.to_sql('filtered_transaction',con=engine, if_exists='append', index=False)
        logging.info("filtered_transaction table updated successfully")
    
        extracted_input_output.to_sql('input_output',con=engine,if_exists='append',index=False)
        logging.info("input_output table updated successfully")

        extracted_derived_df.to_sql('derived_columns',con=engine,if_exists='append',index=False)
        logging.info("derived_columns table updated successfully")

    except Exception as e:
        print("Inside except block")
        print(f"Error: {e}")

if __name__ == "__main__":
    db_connect()