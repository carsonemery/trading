import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from typing import Any, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.exceptions import BadResponse
from polygon.rest.models.splits import Split
from rich import print
from tqdm.asyncio import tqdm

# Add parent directory to path to import utils module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils import add_datetime

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
            # split_event is already a list of Splits dataclass objects
            # (transformation happens in get_split_events())
            list_of_splits.extend(split_event)
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
                # This return only exits fetch_splits(), NOT get_split_events()
                return list[bytes | Split](splits_generator)
            
            # Run the synchronous call in a thread pool
            # fetch_splits() executes here and returns a list of Split objects
            # The return inside fetch_splits() only exits that nested function,
            # execution continues here in get_split_events()
            splits = await asyncio.to_thread(fetch_splits)
            
            # Transform the raw Split objects into Splits dataclass objects
            # This happens AFTER fetch_splits() has returned
            split_objects = [
                Splits(
                    ticker=split.ticker,
                    date=split.execution_date,
                    split_from=split.split_from,
                    split_to=split.split_to
                )
                for split in splits
            ]
            
            # Now return from get_split_events() with the transformed data
            return ("success", split_objects)
        # Catch the failed requests to track which tickers we did not get any name change info from    
        except BadResponse as e:
            print(f"BadResponse for {ticker}: {e}")
            return ("failed", ticker)
        except Exception as e:
            # Catch any other exceptions (like network errors, etc.)
            print(f"Error fetching splits for {ticker}: {e}")
            return ("failed", ticker)


