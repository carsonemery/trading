import boto3
from botocore.config import Config
import os
from dotenv import load_dotenv

# Load environment variables and API key
load_dotenv()

# Initialize a session
session = boto3.Session(
  aws_access_key_id = os.getenv('ACCESS_KEY_ID'),
  aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY'),
)

# Create a client and specify the endpoint 
s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'), # what version should I be using?
)

# Initialize a paginator for listing objects
paginator = s3.get_paginator('list_objects_v2')

# Using us stocks
prefix = 'us_stocks_sip'

# Define a bucket name 
bucket_name = 'flatfiles'

# List the objects using our prefix 
for page in paginator.paginate(Bucket = bucket_name, Prefix = prefix):
    for obj in page['Contents']:
        print(obj['Key']) 


# # Define a local file path to download to 
# local_file_path = ''

# # Download a file (bucket name, endpoint, path to download to locally)
# # could put this in a loop the loop above I think to download
# s3.download_file(bucket_name, )


