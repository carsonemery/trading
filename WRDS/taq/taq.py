import os
import wrds
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

# Initialize a connection object using with context manager
with wrds.Connection(wrds_username=wrds_username) as db:

    # WRDS docs say: FROM taqmsec.cqm
    # Actually use: FROM taqmsec.cqm_YYYYMMDD
    # Three Primary TAQ tables we will want
    # taqmsec.cqm_YYYYMMDD -----> Consolidated Quotes
    # taqmsec.ctm_YYYYMMDD -----> Consolidated Trades
    # taqmsec.nbbo_YYYYMMDD ----> NBBO
    
    sample_query = """
    SELECT date, time_m, sym_root, bid, ask, bidsiz, asksiz
    FROM taqmsec.cqm_20240104
    WHERE sym_root = 'AAPL'
    LIMIT 10
    """
    
    
    
    try:
        df = db.raw_sql(sample_query)
        print(df.head())
    except Exception as e:
        print(f"\nFailed: {e}")


