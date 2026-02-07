import os

import databento as db
from databento.common.enums import JobState
from dotenv import load_dotenv

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
                "ELBM", "WWR", "GWAV", "COOT", 
                "GME", "OMER", "PFAI", "LAES", 
                "NUAI", "NVTS"]

# function to run a batch download
def test_batch_download():
    # send job
    job = data_client.batch.submit_job(
        dataset = "DBEQ.BASIC", # guess this only goes back until 2023
        symbols = test_symbols,
        stype_in = "raw_symbol",
        encoding = "dbn",
        schema = "ohlcv-1d",
        start = "2023-01-01",
        end = "2025-10-10"
    )

    return job.id

def download_from_databento():
    # download OHLCV data files
    print("\nDownloading files...")
    data_client.batch.download(
        # job_id= (just update manually for now)
        output_dir=r""
    )
    
    # save job metadata to JSON file
    # metadata_path = r""
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
