from polygon.rest.models.splits import Split
from polygon import RESTClient
import os
from dotenv import load_dotenv
from polygon.exceptions import BadResponse
from dataclasses import dataclass
import asyncio 
from rich import print
from tqdm.asyncio import tqdm
from typing import List, Tuple, Any
import pandas as pd
from dataclasses import asdict

load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

# Data class to hold split info
@dataclass (frozen = True, slots = True)
class Splits:
    ticker: str
    date: str  # execution_date from Polygon
    split_from: float  # Polygon returns as number - can be int (2-for-1) or float (3-for-2 = 1.5, reverse splits, etc.)
    split_to: float    # Polygon returns as number - can be int or float. Use float to preserve all precision.

async def process_tickers(
    tickers_list: List[str]
    ) -> Tuple[List[Splits], List[str]]:
    """
    Process multiple tickers to fetch their split events from Polygon API.
    
    Args:
        tickers_list: List of ticker symbols to fetch split events for
        
    Returns:
        Tuple of (list_of_splits, list_of_failed_tickers):
        - list_of_splits: List of Splits dataclass objects containing split event data
        - list_of_failed_tickers: List of ticker symbols that failed to fetch
    """
    list_of_splits = []
    list_of_failed = []

    # Limit concurrency with a semaphore and use a 0.01 second or 10ms space 
    # between requests to stay under 100 requests per second
    semaphore = asyncio.Semaphore(20)
    rate_limit_delay = 0.01

    # Create tasks for all tickers
    tasks = [get_split_events(ticker, semaphore) for ticker in tickers_list]

    # Process with rate limiting
    for task in tqdm.as_completed(tasks, desc="Fetching split events", total=len(tasks)):
        # Get the data from each call
        result_type, split_event = await task

        if result_type == "success":
            for split in split_event:
                # Create a Splits dataclass object with all values upon construction
                split_data = Splits(
                    ticker=split.ticker,
                    date=split.execution_date,
                    split_from=split.split_from,
                    split_to=split.split_to
                )
                # Append each split inside the loop
                list_of_splits.append(split_data)
        else:
            list_of_failed.append(split_event)

        await asyncio.sleep(rate_limit_delay)

    return list_of_splits, list_of_failed

async def get_split_events(
    ticker: str,
    semaphore: asyncio.Semaphore
    ) -> Tuple[str, Any]:
    """
    Fetches split events for a single ticker from Polygon API.
    Returns tuple: ("success", list_of_splits) or ("failed", ticker)
    
    Note: client.list_splits() returns a generator, so we need to convert it to a list
    by wrapping it in a function that consumes the generator.
    """
    async with semaphore:
        try:
            # Create a helper function that calls list_splits and converts generator to list
            # This is needed because asyncio.to_thread() expects a callable function,
            # not the result of calling that function
            def fetch_splits():
                # list_splits() returns a generator, so we convert it to a list
                splits_generator = client.list_splits(
                    ticker=ticker,
                    execution_date_gte="2016-01-01",
                    order="asc",
                    limit=1000,  # limit should be an int, not a string
                    sort="execution_date"
                )
                # Convert generator to list
                return list[bytes | Split](splits_generator)
            
            # Run the synchronous call in a thread pool
            splits = await asyncio.to_thread(fetch_splits)
            return ("success", splits)
        # Catch the failed requests to track which tickers we did not get any name change info from    
        except BadResponse as e:
            print(f"BadResponse for {ticker}: {e}")
            return ("failed", ticker)
        except Exception as e:
            # Catch any other exceptions (like network errors, etc.)
            print(f"Error fetching splits for {ticker}: {e}")
            return ("failed", ticker)

async def main():

    tickers_list = ['TSLA'] # 'DRYS', 'TOPS']

    # pull in OHLCV data for a select number of stocks to test
    parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_HISTORICAL_2016_2025.parquet'
    test_df = pd.read_parquet(parquet_path)

    filtered_df = test_df[test_df['ticker'].isin(['CYCC'])]

    print(filtered_df)
    print("=== Checking Specific Dates Data ===")

    
    mask = filtered_df['date'] == '2025-07-03'
    print(filtered_df[mask])

    mask2 = filtered_df['date'] == '2025-07-07'
    print(filtered_df[mask2])

    list_of_splits, list_of_failures = await process_tickers(tickers_list)


    # # Inspect the actual types returned by Polygon
    # if list_of_splits:
    #     first_split = list_of_splits[0]
    #     print("\n=== Type Inspection (first split) ===")
    #     print(f"ticker: {first_split.ticker} -> type: {type(first_split.ticker)}")
    #     print(f"execution_date: {first_split.execution_date} -> type: {type(first_split.execution_date)}")
    #     print(f"split_from: {first_split.split_from} -> type: {type(first_split.split_from)}")
    #     print(f"split_to: {first_split.split_to} -> type: {type(first_split.split_to)}")
    #     print(f"id: {first_split.id} -> type: {type(first_split.id)}")
    #     print("=" * 50 + "\n")

    # Convert the splits into a data frame
    splits_df = pd.DataFrame(asdict(split) for split in list_of_splits)

    print(splits_df)
    
    # for split in list_of_splits:
    #     print(split)
    #     print(type(split))
    # #     # They're actual objects, not strings! You can access attributes:
    # #     print(f"  Ticker: {split.ticker}, Date: {split.execution_date}, Split: {split.split_from}:{split.split_to}")

    # for no_split in list_of_failures:
    #     print(no_split)
    #     print(len(no_split))
    
if __name__ == "__main__":
    asyncio.run(main())


# Process to adjust

# Assuming splits are sorted oldest to newest, but we apply cumulatively from newest backward
# For each split, compute factor once, then apply to all data before its date

# General formula (same for forward and reverse):
# adjustment_factor_prices = split_from / split_to
# adjustment_factor_vol = split_to / split_from

# For data strictly prior to split date (up to but not including split date),
# and back to next prior split or dataset start:
# adjusted_prices = prices * adjustment_factor_prices
# adjusted_vol = vol * adjustment_factor_vol

# Example: Forward split (e.g., TSLA 5-for-1: split_from=1, split_to=5)
# factor_prices = 1/5 = 0.2 → historical prices *= 0.2 (lower)
# factor_vol = 5/1 = 5 → historical vol *= 5 (higher)
# Note: Your 'data / split_to' works if split_from=1 (common for forwards), as /5 == * (1/5), but use general formula for robustness.

# Example: Reverse split (e.g., CYCC 1-for-15: split_from=15, split_to=1)
# factor_prices = 15/1 = 15 → historical prices *= 15 (higher)
# factor_vol = 1/15 ≈ 0.0667 → historical vol *= 0.0667 (lower)