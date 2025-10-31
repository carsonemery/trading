from reprlib import recursive_repr
import pandas as pd
import os
import sys
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.exceptions import BadResponse

# Add parent directory to path to import polygon_datetime module and
# Import datetime conversion function from Polygon/polygon_datetime.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from polygon_datetime import add_datetime

# Import functions from polygon_name_change_events.py
from polygon_name_change_events import get_events, build_ticker_mapping, get_ticker_list

# Import functions from polygon_symbol_mapping.py
from polygon_symbol_mapping import map_symbols, get_current_ticker_for_historical_date

# Load environment variables and setup client (same as polygon_name_change_events.py)
load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

def main():
    """
    End-to-end test of symbol mapping pipeline
    """
    ### =============== Step 1 ========================= ###
    # Load in dataframe with historical data
    csv_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26.csv'
    # Read first 6M rows (~1/4 of 24M)
    test_df = pd.read_csv(csv_path, nrows=6000000)

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

    ### =============== Step 3 ========================= ###
    # Get list of unique tickers in the historical time period 
    unique_tickers = get_ticker_list(test_df)
    
    print(f"Number of unique tickers {len(unique_tickers)}")
    
    # ### =============== Step 4 ========================= ###
    # # Call get_events to get the name change events for each ticker from Polygon
    # events_list, failed_tickers = get_events(unique_tickers)

    # # PRINT SOME STATS
    # print(f"Count of Returned Events: {len(events_list)}")
    # print(f"Count of Failed Tickers: {len(failed_tickers)}")
    # # Check the percent of tickers that failed compared to the original unique tickers list
    # # percentFailed = len(list_failed_tickers) / len(complete_ticker_list)
    # # print(f"Percent failed in dataset: {percentFailed}")

    # ### =============== Step 5 ========================= ###
    # # Build the reverse mapping dictionary with the events
    # reverse_mapping = build_ticker_mapping(events_list)

    # ### =============== Step 6 ========================= ###
    # # Map a 'current_ticker' symbol to everyrow of the OHLCV dataset using 
    # # the reverse mapping
    # mapped_dataframe = map_symbols(reverse_mapping, test_df)

    # # Print head to verify 
    # print(mapped_dataframe.head())

if __name__ == "__main__":
    main()

