import os
import wrds
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

# Initialize a connection object using with context manager
with wrds.Connection(wrds_username=wrds_username) as db:

    # Primary CRSP libraries we have access to:
    # crsp   -----> Annual updates
    # crspq  -----> Quarterly updates
    
    # Key CRSP tables in crspq:
    # crspq.dsf              -----> Daily Stock File (returns, prices, volume)
    # crspq.msf              -----> Monthly Stock File
    # crspq.stocknames       -----> Stock names and info
    # crspq.wrds_dsfv2_query -----> Enhanced daily stock file
    
    # Example 1: Get daily stock data for AAPL
    sample_query = """
    SELECT date, permno, permco, ticker, cusip, prc, vol, ret, shrout
    FROM crspq.dsf
    WHERE ticker = 'AAPL'
      AND date >= '2024-01-01'
      AND date <= '2024-01-31'
    ORDER BY date
    LIMIT 50
    """
    
    try:
        df = db.raw_sql(sample_query)
        print("\nCRSP Daily Stock File (dsf) - AAPL Jan 2024:")
        print(df.head(10))
    except Exception as e:
        print(f"\nFailed: {e}")
