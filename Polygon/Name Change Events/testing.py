import pandas as pd
import os
import sys
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.exceptions import BadResponse
import asyncio
from tqdm import tqdm
from tqdm.asyncio import tqdm

# Add parent directory to path to import polygon_datetime module and
# Import datetime conversion function from Polygon/polygon_datetime.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from polygon_datetime import add_datetime

# Import functions from polygon_name_change_events.py
from polygon_name_change_events import process_tickers, build_ticker_mapping, get_ticker_list, process_tickers

# Import functions from polygon_symbol_mapping.py
from polygon_symbol_mapping import map_symbols, get_current_ticker_for_historical_date

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
    parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_HISTORICAL_2016_2025.parquet'

    if os.path.exists(parquet_path):
        print("Loading from Parquet (fast)...")
        test_df = pd.read_parquet(parquet_path)
    else:
        print("Loading from CSV (slow, will save to Parquet for next time)...")
        # Read in the last ~8M/9M rows with the most recent data for testing
        test_df = pd.read_csv(csv_path, skiprows=range(1, 12_000_001)) # If I want to change the range I need to change the file path or delete the file
        test_df = add_datetime(test_df)
        test_df.to_parquet(parquet_path)
        print(f"Saved to {parquet_path} for faster loading next time")

    # test_df = pd.read_csv(csv_path, skiprows=range(1, 12_000_001))

    # If we want a certain number of rows or range or rows we could try to count the total without 
    # actually counting or observing data, then use the total to inform the split, we are currently 
    # pulling the most historical data

    # PRINT SOME STATS
    print(f"Loaded {len(test_df)} rows for testing")
    print(f"Sample of loaded data:\n{test_df.head()}")

    ### =============== Step 2 ========================= ###
    # Create a datetime column in the first position
    test_df = add_datetime(test_df)

    # Print to verify structure
    print(test_df.head())

    # Count rows per year 
    rows_per_year = test_df.groupby(test_df['date'].dt.year).size()
    print("\nRows per year:")
    print(rows_per_year)
    print(f"\nTotal rows: {rows_per_year.sum():,}")

    ### =============== Step 3 ========================= ###
    # Get list of unique tickers in the historical time period 
    unique_tickers = get_ticker_list(test_df)
    
    print(f"Number of unique tickers {len(unique_tickers)}")
    
    # ### =============== Step 4 ========================= ###
    # # Call get_events to get the name change events for each ticker from Polygon
    events_list, failed_tickers = await process_tickers(unique_tickers)

    # PRINT SOME STATS
    print(f"Count of Returned Events: {len(events_list)}")
    print(f"Count of Failed Tickers: {len(failed_tickers)}")
    # Check the percent of tickers that failed compared to the original unique tickers list
    percentFailed = len(failed_tickers) / len(unique_tickers)
    print(f"Percent failed in dataset: {percentFailed}")

    # # ### =============== Step 5 ========================= ###
    # # # Build the reverse mapping dictionary with the events
    # reverse_mapping = build_ticker_mapping(events_list)

    # # ### =============== Step 6 ========================= ###
    # # # Map a 'current_ticker' symbol to everyrow of the OHLCV dataset using 
    # # # the reverse mapping
    # mapped_dataframe = map_symbols(reverse_mapping, test_df)

    # # # Print head to verify 
    # print(mapped_dataframe.head())

if __name__ == "__main__":
    asyncio.run(main())

