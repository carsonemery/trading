import asyncio
import os
import pickle
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from numpy import result_type
from polygon import RESTClient
from polygon.exceptions import BadResponse
from rich import print
from tqdm import tqdm
from tqdm.asyncio import tqdm

load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

@dataclass(frozen=True, slots=True)
class TickerEvent:
    ticker: str
    name: Optional[str] = None
    composite_figi: Optional[str] = None
    cik: Optional[str] = None
    events: List[Dict[str, Any]] = field(default_factory=list)

async def process_tickers(
    tickers_list: List[str]):
    """
    Get ticker events for a list of tickers and handle errors properly.
    Returns tuple of (successful_events, failed_tickers) which are lists.
    """
    list_of_events = []
    list_failed_tickers = []
    # We also want to track and return a set of each ticker we have successfully processed through the API, using this to track what tickers we 
    # need to send in the future to complete our data set or resume a cancelled or failed request (not currently implemented)
    set_of_processed_tickers = {}

    # Limit concurrency with a semaphore and use a 0.01 second or 10ms space 
    # between requests to stay under 100 requests per second
    semaphore = asyncio.Semaphore(20)
    rate_limit_delay = 0.01

    # Create tasks for all tickers
    tasks = [get_ticker_event(ticker, semaphore) for ticker in tickers_list]
   
    # Process with rate limiting
    # for task in asyncio.as_completed(tasks):
    for task in tqdm.as_completed(tasks, desc="Fetching ticker events", total=len(tasks)):
        # Get the result type and data from each call
        result_type, result_data = await task
        
        # If the result was success append to list_of_events
        if result_type == "success":
            # Want to save each result_data event as well during the run incase we drop off or want to avoid calling the API again 
            # print(f"PRINTING WHAT AN RESULT_DATA EVENT LOOKS LIKE: {result_data}")
            # will be implementing later in development

            # Add to our list of events list
            list_of_events.append(result_data)
        else:
            list_failed_tickers.append(result_data)

        # Basic rate limiting to stay under 100 req/sec
        await asyncio.sleep(rate_limit_delay)
    
    return list_of_events, list_failed_tickers

async def get_ticker_event(
    ticker: str, 
    semaphore: asyncio.Semaphore
    ) -> Tuple[str, Any]:
    """
    Processes and returns the info from one ticker event at a time
    Returns tuple: ("success", event_dict) or ("failed", ticker)
    """
    async with semaphore:
        try:
            # Run the synchronous RESTClient call in a thread
            events = await asyncio.to_thread(client.get_ticker_events, ticker)
            # Construct and event dictionary which holds:
                # 1) the ticker we passed in 
                # 2) the name of the event (should simply be events)
                # 3) the composite figi if it exists
                # 4) the cik if it exists
                # 5) the list of events
            event_dict = {
                'ticker': ticker, 
                'name': getattr(events, 'name', None),
                'composite_figi': getattr(events, 'composite_figi', None),
                'cik': getattr(events, 'cik', None),
                'events': getattr(events, 'events', [])
            }
            return ("success", event_dict)
        # Catch the failed requests to track which tickers we did not get any name change info from    
        except BadResponse as e:
            print(f"BadResponse for {ticker}: {e}")
            return ("failed", ticker)
        
def build_ticker_mapping(events_list: []):
    """
    Build a mapping of all historical tickers to their current ticker.
    Returns a dictionary where key=historical_ticker, value=current_ticker

    """
    reverse_mapping = {}
    
    # Iterate through the list of event dictionaries
    for event_data in events_list:
        # Get the curent ticker from the event_data dictionary
        current_ticker = event_data['ticker']
        # Get the events list for the ticker
        events = event_data.get('events', [])

        # Create a list to store the historical tickers
        historical_tickers = []
        
        # Add all historical tickers from events
        for event in events:
            historical_ticker = event.get('ticker_change', {}).get('ticker')
            historical_date = event.get('date')
            if len(events) <= 1:
                # If there is only one event returned in the 
                historical_date = date.today().strftime('%Y-%m-%d')
            if historical_ticker:
                # Add the historical ticker to a list of all historical tickers (including the current ticker)
                historical_tickers.append((historical_ticker, historical_date))
        
        reverse_mapping[current_ticker] = historical_tickers

    return reverse_mapping



    # # currently testing the endpoint on these tickers
    # tickers_list = ['META', 'BLL', 'BALL',
    #                 'FB', 'AI', 'T', 'PTWO',
    #                 'SBC', 'TWX', 'AOL',
    #                 'WBD', 'HWP', 'HPQ', 'NRXP',
    #                 'OCGN', 'PHUN', 'MARK',
    #                 'GWH', 'XENE', 'RCAT', 'MRIN',
    #                 'HSGX']
    




