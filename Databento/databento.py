import os
from databento.common.enums import JobState
from dotenv import load_dotenv
import databento as db
import pandas as pd
import numpy as np

# load environment variables and API key
load_dotenv()
data_client = db.Historical(os.getenv('DATABENTO_API_KEY'))
reference_client = db.Reference(os.getenv('DATABENTO_API_KEY'))

# test symbols
test_symbols = ["TMQ", "SOPA", "ONMD", "BJDX", 
                "SCWO", "CRML", "ARBK", "CLBT",
                "KRYS", "RYN", "PKST", "GXO", 
                "ASMIY", "ASTH", "OS", "INTA", 
                "HYPD", "SONN", "TSLA", "META",
                "STI", "AQMS", "GWH", "NVA", 
                "CRML", "ELBM", "WWR", "GWAV"]

# function to run a batch download
def test_batch_download():
    # send job
    job = data_client.batch.submit_job(
        dataset = "DBEQ.BASIC",
        symbols = test_symbols,
        stype_in = "raw_symbol",
        encoding = "dbn",
        schema = "ohlcv-1d",
        start = "2020-01-01",
        end = "2025-10-10"
    )

def download_from_databento():
    # download OHLCV data files
    print("\nDownloading files...")
    files = data_client.batch.download(
        # job_id=job, @TODO NEED TO FIX, find a way to get the job id from the call/return
        output_dir=r"C:\Users\carso\Development\emerytrading\Data\Stocks"
    )
    
    # save job metadata to JSON file
    # metadata_path = rf"C:\Users\carso\Development\emerytrading\Data\Stocks\job_{job_id}_metadata.json"
    # with open(metadata_path, "w") as f:
    #     json.dump(job_metadata, f, indent=2)
    
    # print(f"\nMetadata saved: {metadata_path}")


# this wont work without a subscription
def test_corp_actions():
    corp_actions = reference_client.corporate_actions.get_range(
        symbols=["AAPL"],
        stype_in="raw_symbol",
        start="2020-01-01",
        pit=True
    )
    print(corp_actions.head())
