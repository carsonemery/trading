from ast import List
import boto3
import aioboto3
import pandas as pd
from botocore.config import Config
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
import asyncio

# Load environment variables and API key
load_dotenv()

# Initialize a session (async compatible)
session = aioboto3.Session(
  aws_access_key_id = os.getenv('ACCESS_KEY_ID'),
  aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY'),
)

# Initialize a paginator for listing objects
paginator_spec = 'list_objects_v2'

# Using US stocks
prefix = 'us_stocks_sip/day_aggs_v1/'

# Define a bucket name 
bucket_name = 'flatfiles'

# Define date range
START_DATE = '2016-01-01'
END_DATE = '2025-10-26'

# Convert to datetime objects
start_dt = datetime.strptime(START_DATE, '%Y-%m-%d')
end_dt = datetime.strptime(END_DATE, '%Y-%m-%d')

# Construct the FULL S3 key for the day BEFORE start date
day_before = start_dt - timedelta(days=1)
year = day_before.strftime('%Y')
month = day_before.strftime('%m')
date_str = day_before.strftime('%Y-%m-%d')

# Key that allows us to define what time period to start pulling data
start_after_key = f'us_stocks_sip/day_aggs_v1/{year}/{month}/{date_str}.csv.gz'

# Define a local path to download files 
local_filepath = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test-fullpull'

### ====================================================================================================== ###

def build_download_list(
      bucket_name: str, 
      prefix: str, 
      start_after_date: str,
      end_date: str,
      paginator_spec: str
      ) -> []:
      """ Build and return a list of files to download from S3
      """

      # Initialize list to store downloads
      files_to_download = []

      # Create a regular boto3 client for pagination (async not needed here)
      s3_sync = boto3.client(
            's3',
            endpoint_url='https://files.polygon.io',
            config=Config(signature_version='s3v4')
      )

      # Init the paginator
      paginator = s3_sync.get_paginator(paginator_spec)

      # List the objects using our prefix 
      # Page will be a grouping of files, we call these contents pages and each file in the contents an obj
      for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfer=start_after_date):
            for obj in page['Contents']:
                  # Explicitly extract filename to a string
                  object_name = str(obj['Key'])

                  # Extract the date of the file based on the expected format from Polygon
                  match = re.search(r'(\d{4}-\d{2}-\d{2})', object_name)
                  date_str = match.group(1)
                  # Convert the date string to datetime object for comparison
                  date = datetime.strptime(date_str, '%Y-%m-%d')

                  # Check if we passed our end date, used to control the date range we pull data
                  if date >= datetime.strptime(end_date, '%Y-%m-%d'):
                        return files_to_download

                  # Add file info to the list
                  filename = f"{date_str}.csv.gz"
                  path = os.path.join(local_filepath, filename)
                  
                  # 
                  files_to_download.append({
                        'object_name': object_name,
                        'path': path,
                        'date_str': date_str
                  })
            
      return files_to_download

async def download_flatfiles_async(
      files_to_download: [],
      ):
      """
      """
      
      async with session.client(
            's3',
            endpoint_url='https://files.polygon.io',
            config=Config(signature_version='s3v4'), 
      ) as s3_async:
      
            for page in list_to_download:

                 


                              # Print the object name and type for logging
                              print(object_name)
                              print(type(object_name))
                              # maybe add this info to a datastructure later as well




                        else:
                              print("no data found in filename")
                              continue
                  if status:
                        break

async def download_file(
      s3, 
      bucket_name: str,
      file_info: {}
      ) -> bool:
      """ Download individual files logic
      """

      # Download the filename to the file path
      try:
            await s3.download_file(bucket_name, file_info[], path)
            print(f"Downloaded {path}")
            return True
      except Exception as e:
            print(f"could not download {path}")
            return False
      

      return


def read_files_into_df(
      local_filepath: str) -> []:
      """
      """
      # Create list to hold dataframes
      dataframes = []

      # Get all files in directory 
      files = [f for f in os.listdir(local_filepath) if f.endswith('csv.gz')]

      for file in sorted(files):
            filepath = os.path.join(local_filepath, file)

            df = pd.read_csv(filepath, compression='gzip')

            dataframes.append(df)

      return pd.concat(dataframes, ignore_index=True)

def main():
      # download_flatfiles()
      df = read_files_into_df(local_filepath)
      print(df.head())
      df.to_csv(r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\polygon_test_10yrs.csv', index=False)
      

if __name__ == "__main__":
      main()
