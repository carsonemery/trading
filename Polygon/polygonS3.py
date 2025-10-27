import boto3
import aioboto3
import pandas as pd
from botocore.config import Config
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta

# Load environment variables and API key
load_dotenv()

# Initialize a session (async)
session = aioboto3.Session(
  aws_access_key_id = os.getenv('ACCESS_KEY_ID'),
  aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY'),
)

# Create a client and specify the endpoint 
# s3 = session.client(
#   's3',
#   endpoint_url='https://files.polygon.io',
#   config=Config(signature_version='s3v4'),
# )

# Initialize a paginator for listing objects
paginator = s3.get_paginator('list_objects_v2')

# Using us stocks
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

def build_download_list(
      bucket, 
      prefix, 
      start_after_date) -> []:
      """ Gets a list of all files to download from S3 for our desired universe
          returns the list.
      """
      list_to_download = []

      # List the objects using our prefix 
      for page in paginator.paginate(bucket, prefix,start_after_date):
            list_to_download.append(page)

      return list_to_download

async def download_flatfiles_async(list_to_download: []):
      async with session.client(
            's3',
            endpoint_url='https://files.polygon.io',
            config=Config(signature_version='s3v4'),
      ) as s3:
      
      async with asyncio.TaksGroup() as tg:
      
            for page in list_to_download:

                  for obj in page['Contents']:
                  
                  # explicitly extract filename to a string
                  object_name = str(obj['Key'])

                  match = re.search(r'(\d{4}-\d{2}-\d{2})', object_name)
                  if match:
                        date_str = match.group(1)
                        # Convert the date string to datetime object for comparison
                        date = datetime.strptime(date_str, '%Y-%m-%d')

                        # Check if we passed our end date
                        if date >= datetime.strptime(END_DATE, '%Y-%m-%d'):
                              status = True
                              break

                        # Print the object name
                        print(object_name)

                        local_filename = f"{date_str}.csv.gz"
                        path = os.path.join(local_filepath, local_filename)

                        # Download the filename to the file path
                        try: 
                              s3.download_file(bucket_name, object_name, path)
                              print(f"Downloaded {path}")
                              downloaded += 1
                        except Exception as e:
                              print(f"could not download {path}")
                  else:
                        print("no data found in filename")
                        continue
            if status:
                  break





def async download_file(
      s3, 
      bucket_name,
      object_name, 
      local_path):
      """ Download individual files logic
      """

      pass


def read_files_into_df(
      local_filepath: str):
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
