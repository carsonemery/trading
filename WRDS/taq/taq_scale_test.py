import os
import wrds
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import time

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

with wrds.Connection(wrds_username=wrds_username) as db:
    
    # Test 1: Get row count for ONE stock for ONE day
    print("\nTest 1: Single stock, single day")
    test_date = '20240104'
    
    # cqm tests quotes
    # ctm tests trades

    query_single = f"""
    SELECT COUNT(*) as row_count
    FROM taqmsec.cqm_{test_date}
    WHERE sym_root = 'AAPL'
    """
    
    try:
        start_time = time.time()
        result = db.raw_sql(query_single)
        elapsed = time.time() - start_time
        
        rows = result['row_count'].iloc[0]
        print(f"Date: {test_date}")
        print(f"Stock: AAPL")
        print(f"Rows: {rows:,}")
        print(f"Query time: {elapsed:.2f} seconds")
        print(f"Estimated size: ~{rows * 0.1 / 1024:.2f} MB (rough estimate)")
        
    except Exception as e:
        print(f"Failed: {e}")
    
    
    # Test 2: Get total row count for ALL stocks for ONE day
    print("\nTest 2: ALL stocks, single day (THIS WILL TAKE A WHILE)")
    
    query_all = f"""
    SELECT COUNT(*) as row_count
    FROM taqmsec.cqm_{test_date}
    """
    
    try:
        start_time = time.time()
        result = db.raw_sql(query_all)
        elapsed = time.time() - start_time
        
        rows = result['row_count'].iloc[0]
        print(f"   Date: {test_date}")
        print(f"   Total rows (all stocks): {rows:,}")
        print(f"   Query time: {elapsed:.2f} seconds")
        print(f"   Estimated size: ~{rows * 0.1 / 1024 / 1024:.2f} GB")
        
        # Extrapolate
        print("\nEXTRAPOLATION:")
        print(f"One day: ~{rows * 0.1 / 1024 / 1024:.1f} GB")
        print(f"One month (21 days): ~{rows * 21 * 0.1 / 1024 / 1024:.1f} GB")
        print(f"One year (252 days): ~{rows * 252 * 0.1 / 1024 / 1024:.1f} GB")
        print(f"15 years (3,780 days): ~{rows * 3780 * 0.1 / 1024 / 1024 / 1024:.1f} TB")
        
    except Exception as e:
        print(f"Failed (likely timeout): {e}")
        print("This is expected - counting all rows is expensive")
    
    
    # Test 3: Sample data to understand structure
    print("\nTest 3: Sample 10 rows to see data structure")
    
    query_sample = f"""
    SELECT date, time_m, sym_root, bid, ask, bidsiz, asksiz
    FROM taqmsec.cqm_{test_date}
    LIMIT 10
    """
    
    try:
        df = db.raw_sql(query_sample)
        print("\nSample data:")
        print(df)
        print(f"\nMemory size of 10 rows: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
    except Exception as e:
        print(f"Failed: {e}")
