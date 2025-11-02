from numpy import result_type
from polygon import RESTClient
import os
from dotenv import load_dotenv
from polygon.exceptions import BadResponse
import pandas as pd
import asyncio 
from typing import List, Tuple, Dict, Any
from rich import print
from tqdm import tqdm
from tqdm.asyncio import tqdm

load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

def get_ticker_list(
    df: pd.DataFrame
    ) -> []:
    """ Gets all unique ticker names from entire corpus of OHCLV daily data
    """
    return df['ticker'].unique()

async def process_tickers(
    tickers_list: List[str]):
    """
    Get ticker events for a list of tickers and handle errors properly.
    Returns tuple of (successful_events, failed_tickers) which are lists.
    """
    list_of_events = []
    list_failed_tickers = []

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
            # Runt the synchronous RESTClient call in a thread
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
        historical_tickers =[]
        
        # Add all historical tickers from events
        for event in events:
            historical_ticker = event.get('ticker_change', {}).get('ticker')
            historical_date = event.get('date')
            if historical_ticker:
                # Add the historical ticker to a list of all historical tickers (including the current ticker)
                historical_tickers.append((historical_ticker, historical_date))
        
        reverse_mapping[current_ticker] = historical_tickers

    return reverse_mapping

async def main():

    # currently testing the endpoint on these tickers
    tickers_list = ['META', 'BLL', 'BALL',
                    'FB', 'AI', 'T', 'PTWO',
                    'SBC', 'TWX', 'AOL',
                    'WBD', 'HWP', 'HPQ', 'NRXP',
                    'OCGN', 'PHUN', 'MARK',
                    'GWH', 'XENE', 'RCAT', 'MRIN',
                    'HSGX']
    
    # Get the list of unique tickers 
    # tickers_list = get_ticker_list()

    # Get the list of ticker events and failed tickers
    list_of_events, list_failed_tickers = await process_tickers(tickers_list)

    print(f"Count of Returned Events: {len(list_of_events)}")
    print(f"Count of Failed Tickers: {len(list_failed_tickers)}")
    # Check the percent of tickers that failed compared to the original unique tickers list
    percentFailed = len(list_failed_tickers) / len(tickers_list)
    print(f"Percent failed in dataset: {percentFailed * 100:.2f}%")

    # Build a ticker mapping
    reverse_mapping = build_ticker_mapping(list_of_events)
    for key, value in reverse_mapping.items():
        print(f"Mapping: {key, value}")

if __name__ == "__main__":
    asyncio.run(main())


# #tickers_list = get_ticker_list()

# # need to implement some async and rate limiting ~100 requests per seconnd per polygon rep

# # currently testing the endpoint on these tickers
# tickers_list = ['META', 'BLL', 'BALL',
#                 'FB', 'AI', 'T', 'PTWO',
#                 'SBC', 'TWX', 'AOL',
#                 'WBD', 'HWP', 'HPQ', 'NRXP',
#                 'OCGN', 'PHUN', 'MARK',
#                 'GWH', 'XENE', 'RCAT', 'MRIN']
