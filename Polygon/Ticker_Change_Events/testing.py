import pandas as pd
import os
import sys
from dotenv import load_dotenv
from polygon import RESTClient
import asyncio
import pickle
from pathlib import Path
import numpy as np

# Add parent directory to path to import utils module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils import add_datetime

# Import functions from polygon_name_change_events.py
from polygon_name_change_events import process_tickers, build_ticker_mapping, get_ticker_event, process_tickers

# Import functions from polygon_symbol_mapping.py
from polygon_symbol_mapping import map_symbols, diagnose_data

from cleaning import run_cleaning

# Load environment variables and setup client (same as polygon_name_change_events.py)
load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

async def main():
    """
    End-to-end test of symbol mapping pipeline
    """
    # Switching to Parquet file format for faster file reading and writing while testing
    ### =============== Step 1 ========================= ###
    # Load in dataframe with historical data
    csv_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26.csv'
    # This path starts around 2021 data
    # parquet_path_2021 = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_HISTORICAL_2016_2025.parquet'

    # if os.path.exists(parquet_path):
    #     print("Loading from Parquet (fast)...")
    #     test_df = pd.read_parquet(parquet_path)
    # else:
    #     print("Loading from CSV (slow, will save to Parquet for next time)...")
    #     # Read in the last ~8M/9M rows with the most recent data for testing
    #     test_df = pd.read_csv(csv_path, skiprows=range(1, 12_000_001)) # If I want to change the range I need to change the file path or delete the file
    #     test_df = add_datetime(test_df)
    #     test_df.to_parquet(parquet_path)
    #     print(f"Saved to {parquet_path} for faster loading next time")

    OHLCV_data = pd.read_csv(csv_path)

    # # If we want a certain number of rows or range or rows we could try to count the total without 
    # # actually counting or observing data, then use the total to inform the split, we are currently 
    # # pulling the most historical data

    # # PRINT SOME STATS
    # print(f"Loaded {len(OHLCV_data)} rows for testing")
    # print(f"Sample of loaded data:\n{OHLCV_data.head()}")

    ### =============== Step 2 ========================= ###
    # Create a datetime column in the first position
    # OHLCV_data = add_datetime(OHLCV_data)

    # diagnose_data(OHLCV_data)

    OHLCV_cleaned = run_cleaning(OHLCV_data)

    # diagnose_data(OHLCV_cleaned)
    

    # # Print to verify structure
    # print(OHLCV_data.head())

    # test = OHLCV_data.sort_values(['ticker', 'date'], kind='stable')
    # print(test)
    # diagnose_data(test)


    # # Count rows per year 
    # rows_per_year = test_df.groupby(test_df['date'].dt.year).size()
    # print("\nRows per year:")
    # print(rows_per_year)
    # print(f"\nTotal rows: {rows_per_year.sum():,}")

    ### =============== Step 3 ========================= ###
    # Get list of unique tickers in the historical time period 
    # unique_tickers = OHLCV_data['ticker'].unique()

    # print(f"Number of unique tickers {len(unique_tickers)}")
    
    # ### =============== Step 4 ========================= ###
    # Call get_events to get the name change events for each ticker from Polygon
    # events_list, failed_tickers = await process_tickers(unique_tickers)

    # # PRINT SOME STATS
    # print(f"Count of Returned Events: {len(events_list)}")
    # print(f"Count of Failed Tickers: {len(failed_tickers)}")
    # # Check the percent of tickers that failed compared to the original unique tickers list
    # percentFailed = len(failed_tickers) / len(unique_tickers)
    # print(f"Percent failed in dataset: {percentFailed}")

    # ### =============== Step 5 ========================= ###
    # Build the reverse mapping dictionary with the events
    # reverse_mapping = build_ticker_mapping(events_list)

    # print(reverse_mapping)
    # print(type(reverse_mapping))

    # Create the directory if it doesnt exist 
    # reverse_mapping_filepath.parent.mkdir(parents=True, exist_ok=True)

    # with open(reverse_mapping_filepath, 'wb') as f:
    #     pickle.dump(reverse_mapping, f)

    # ### =============== Step 6 ========================= ###
    # # Map a 'current_ticker' symbol to everyrow of the OHLCV dataset using 
    # # the reverse mapping
    # # Load in the reverse mapping from the pickle file 
    # with open(reverse_mapping_filepath, 'rb') as of:
    #     reverse_mapping_reloaded = pickle.load(of)

    # reverse_mapping_filepath = Path(r"C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_REVERSEMAPPING.pkl")

    # with open(reverse_mapping_filepath, 'rb') as of:
    #     reverse_mapping_reloaded = pickle.load(of)


    # # for key, value in reverse_mapping_reloaded.items():
    # #     print(f"Mapping: {key, value}")
    # #     print(f"Mapping: {type(key), type(value)}")

    # START_DATE = '2016-01-01'



    # Convert the reverse_mapping_reloaded dictionary into a pandas data frame
    # mapped_df = map_symbols(reverse_mapping_reloaded, OHLCV_cleaned, START_DATE)

    # # Save mapped dataframe 
    # mapped_df_path = Path(r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26_MAPPED.parquet')
    
    # # Create the directory if it doesnt exist and save the mapped historical data frame
    # mapped_df_path.parent.mkdir(parents=True, exist_ok=True)
    # mapped_df.to_parquet(mapped_df_path)

    # # Print head to verify 
    # print(mapped_df.head())

if __name__ == "__main__":
    asyncio.run(main())

