"""Test script to analyze TAQ data scale and performance.

This script helps estimate data sizes and query times for TAQ data.
"""

import time

import pandas as pd

from bearplanes.data.wrds.client import WRDSClient


def test_single_stock_quote_count(date: str = '20240104', symbol: str = 'AAPL'):
    """Test row count and query time for a single stock's quotes."""
    print(f"\nTest 1: Single stock ({symbol}), single day ({date})")
    
    with WRDSClient() as db:
        query = f"""
        SELECT COUNT(*) as row_count
        FROM taqmsec.cqm_{date}
        WHERE sym_root = '{symbol}'
        """
        
        try:
            start_time = time.time()
            result = db.raw_sql(query)
            elapsed = time.time() - start_time
            
            rows = result['row_count'].iloc[0]
            print(f"  Stock: {symbol}")
            print(f"  Rows: {rows:,}")
            print(f"  Query time: {elapsed:.2f} seconds")
            print(f"  Estimated size: ~{rows * 0.1 / 1024:.2f} MB (rough estimate)")
            
        except Exception as e:
            print(f"  Failed: {e}")


def test_all_stocks_quote_count(date: str = '20240104'):
    """Test total row count for all stocks on a single day."""
    print(f"\nTest 2: ALL stocks, single day ({date}) - THIS WILL TAKE A WHILE")
    
    with WRDSClient() as db:
        query = f"""
        SELECT COUNT(*) as row_count
        FROM taqmsec.cqm_{date}
        """
        
        try:
            start_time = time.time()
            result = db.raw_sql(query)
            elapsed = time.time() - start_time
            
            rows = result['row_count'].iloc[0]
            print(f"  Total rows (all stocks): {rows:,}")
            print(f"  Query time: {elapsed:.2f} seconds")
            print(f"  Estimated size: ~{rows * 0.1 / 1024 / 1024:.2f} GB")
            
            # Extrapolate
            print("\n  EXTRAPOLATION:")
            print(f"    One day: ~{rows * 0.1 / 1024 / 1024:.1f} GB")
            print(f"    One month (21 days): ~{rows * 21 * 0.1 / 1024 / 1024:.1f} GB")
            print(f"    One year (252 days): ~{rows * 252 * 0.1 / 1024 / 1024:.1f} GB")
            print(f"    15 years (3,780 days): ~{rows * 3780 * 0.1 / 1024 / 1024 / 1024:.1f} TB")
            
        except Exception as e:
            print(f"  Failed (likely timeout): {e}")
            print("  This is expected - counting all rows is expensive")


def test_sample_data_structure(date: str = '20240104'):
    """Sample a few rows to understand data structure and memory usage."""
    print(f"\nTest 3: Sample 10 rows to see data structure")
    
    with WRDSClient() as db:
        query = f"""
        SELECT date, time_m, sym_root, bid, ask, bidsiz, asksiz
        FROM taqmsec.cqm_{date}
        LIMIT 10
        """
        
        try:
            df = db.raw_sql(query)
            print("\n  Sample data:")
            print(df)
            print(f"\n  Memory size of 10 rows: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
            
        except Exception as e:
            print(f"  Failed: {e}")
