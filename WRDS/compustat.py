import os
import wrds
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

# Initialize a connection object using with context manager
with wrds.Connection(wrds_username=wrds_username) as db:

    # Primary Compustat library:
    # comp  -----> Compustat North America
    
    # Key Compustat tables in comp:
    # comp.funda     -----> Fundamentals Annual (yearly financial statements)
    # comp.fundq     -----> Fundamentals Quarterly (quarterly financial statements)
    # comp.company   -----> Company information
    # comp.names     -----> Company names and identifiers
    
    # Example: Get annual fundamentals for Apple
    sample_query = """
    SELECT datadate, gvkey, fyear, tic, conm,
           at, lt, revt, ni, ebitda, capx
    FROM comp.funda
    WHERE tic = 'AAPL'
      AND indfmt = 'INDL'
      AND datafmt = 'STD'
      AND popsrc = 'D'
      AND consol = 'C'
      AND datadate >= '2020-01-01'
    ORDER BY datadate DESC
    LIMIT 10
    """
    
    try:
        df = db.raw_sql(sample_query)
        print("\nCompustat Annual Fundamentals - AAPL:")
        print(df)
    except Exception as e:
        print(f"\nFailed: {e}")

