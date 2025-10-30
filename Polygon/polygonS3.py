import boto3
import aioboto3
import pandas as pd
from botocore.config import Config
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
import asyncio
from dataclasses import dataclass

@dataclass (frozen = True, slots = True)
class DownloadJob:
      object_name: str # the S3 key, the full path to download a days worth of data
      path: str # the path to download that days worth of data, will be made unique for each day using the date 
      date_str: str # date of the day we are downloading

# Load environment variables and API key
load_dotenv()

# Initialize a session (async compatible)
session = aioboto3.Session(
  aws_access_key_id = os.getenv('ACCESS_KEY_ID'),
  aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY'),
)

### ====================================================================================================== ###

def build_download_list(
      bucket_name: str, 
      prefix: str, 
      start_after_date: str,
      end_date: str,
      paginator_spec: str,
      file_path: str,
      endpoint_url: str,
      signature_version: str,
      ) -> list[DownloadJob]:
      """ 
            Build and return a list of files to download from S3
            Each item in the list should be a Download Job which is
            a single days daily data of all stocks on that day to download
            looks like this:

            us_stocks_sip/day_aggs_v1/2016/01/2016-01-01.csv.gz
      """

      # Initialize list to store DownloadJobs
      jobs: list[DownloadJob] = []
      
      # Create a regular boto3 client for pagination (async not needed here)
      s3_sync = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            config=Config(signature_version=signature_version)
      )

      # Init the paginator
      paginator = s3_sync.get_paginator(paginator_spec)

      # List the objects using our prefix 
      # Page will be a grouping of files, we call these contents pages and each file in the contents an obj
      for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after_date):
            # Use page.get syntax to return an empty list instead of a key error if there are any quirks with each "Page"
            # A page is just an arbitrary number of day objects
            for obj in page.get('Contents', []):
                  # Explicitly extract filename to a string
                  object_name = str(obj['Key'])

                  # Extract the date of the file based on the expected format from Polygon
                  match = re.search(r'(\d{4}-\d{2}-\d{2})', object_name)
                  date_str = match.group(1)
                  # Convert the date string to datetime object for comparison
                  date = datetime.strptime(date_str, '%Y-%m-%d')

                  # Check if we passed our end date, used to control the date range we pull data
                  if date >= datetime.strptime(end_date, '%Y-%m-%d'):
                        return jobs

                  # Add file info to the list
                  filename = f"{date_str}.csv.gz"
                  path = os.path.join(file_path, filename)
                  
                  # Build the list of DownLoad jobs with download job objects
                  jobs.append(DownloadJob(
                        object_name = object_name,
                        path = path,
                        date_str = date_str
                  ))
            
      return jobs

async def download_flatfiles_async(
      jobs: list[DownloadJob],
      bucket_name: str,
      max_concurrency: int,
      skip_existing: bool,
      retries: int, 
      backoff_seconds: float,
      endpoint_url: str,
      signature_version: str,
      filepath: str
      ) -> None:
      """ Orchestrate concurrent downloads with TaskGroup and a semaphore
      """
      # Use makedirs to create a folder endpoint for the specified path we chose
      # if it doesnt not exist yet
      os.makedirs(filepath, exist_ok=True)

      # Create shared sempahore for bounded concurrency 
      semaphore = asyncio.Semaphore(max_concurrency)

      async with session.client(
            's3',
            endpoint_url='endpoint_url',
            config=Config(signature_version='signature_version'),
      ) as s3_async:
            async with asyncio.TaskGroup() as tg:
                  for job in jobs:
                        tg.create_task(guarded_download(
                              s3=s3_async,
                              semaphore=semaphore,
                              bucket_name=bucket_name,
                              job=job,
                              skip_existing=skip_existing,
                              retries=retries,
                              backoff_seconds=backoff_seconds,
                        ))

async def guarded_download(
      s3,
      semaphore: asyncio.Semaphore,
      bucket_name: str,
      job: DownloadJob,
      skip_existing: bool,
      retries: int,
      backoff_seconds: float
      ) -> None:
      """
            Bounded concurrency + skip if exists + retries
      """

      if skip_existing and os.path.exists(job.path):
            return

      async with semaphore: 
            attempt = 0
            while True:
                  try:
                        await download_file(s3, bucket_name, job)
                        return
                  except Exception as e:
                        attempt += 1
                        if attempt > retries:
                              print(f"Failed: {job.object_name} -> {job.path}: {e}")
                              return
                        await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))

async def download_file(
      s3, 
      bucket_name: str,
      job: DownloadJob
      ) -> None:
      """ 
            Single attempt download
      """
      os.makedirs(os.path.dirname(job.path), exist_ok = True)
      await s3.download_file(bucket_name, job.object_name, job.path)
      print(f"Downloaded {job.path}")

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

def run_download(
      *,
      bucket_name: str,
      prefix: str,
      start_after_key: str,
      end_date: str,
      paginator_spec: str,
      local_folder: str,
      max_concurrency: int,
      skip_existing: bool,
      retries: int,
      backoff_seconds: float,
      endpoint_url: str,
      output_csv_path: str
      ) -> pd.DataFrame:
      # 1) Build the plan (sync)
      jobs = build_download_list(
            bucket_name=bucket_name,
            prefix=prefix,
            start_after_date=start_after_key,
            end_date=end_date,
            paginator_spec=paginator_spec,
            file_path=local_folder,
      )

      # 2) Download concurrently (async)
      asyncio.run(download_flatfiles_async(
            jobs=jobs,
            bucket_name=bucket_name,
            max_concurrency=max_concurrency,
            skip_existing=skip_existing,
            retries=retries,
            backoff_seconds=backoff_seconds,
            endpoint_url=endpoint_url,
            filepath=local_folder,
      ))

      # 3) Load local files into a single dataframe (sync)
      df = read_files_into_df(local_folder)

      # 4) Optional: write aggregated CSV (sync)
      if output_csv_path:
            df.to_csv(output_csv_path, index=False)

      return df

def main():

      ## ========================================= ## 
      # S3 Variables #
      ## ========================================= ##
      
      paginator_spec = 'list_objects_v2' # Initialize a paginator for listing objects
      prefix = 'us_stocks_sip/day_aggs_v1/' # Using US stocks
      bucket_name = 'flatfiles' # Define a bucket name 
      endpoint_url = 'https://files.polygon.io' # Define the endpoint URL
      signature_version = 's3v4'
      
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

      # File paths
      file_path_for_downloads = r"C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test-fullpull10-30"
      file_path_for_csv = r"C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\polygon_test_10yrs.csv"

      df = run_download(
            bucket_name=bucket_name,
            prefix=prefix,
            start_after_key=start_after_key,
            end_date=END_DATE,
            paginator_spec=paginator_spec,
            local_folder=file_path_for_downloads,
            max_concurrency=20,
            skip_existing=True,
            retries=3,
            backoff_seconds=0.5,
            output_csv_path=file_path_for_csv
      )
      
if __name__ == "__main__":
      main()
