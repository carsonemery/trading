from datetime import date
from numpy import result_type
from polygon import RESTClient
import os
from dotenv import load_dotenv
from polygon.exceptions import BadResponse
import pandas as pd
import asyncio 
from typing import List, Tuple, Dict, Any
from polygon.rest import reference
from rich import print
from tqdm import tqdm
from tqdm.asyncio import tqdm
import pickle
from pathlib import Path
from dataclasses import dataclass

load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

@dataclass (frozen = True, slots = True)
class RefData:
	ticker: str
	name: str
	market: str
	locale: str
	primary_exchange: str
	type: str
	active: bool
	currency_name: str
	cik: str
	composite_figi: str
	share_class_figi: str
	last_updated_utc: str
	date_param: str




def build_payload(
	df: pd.DataFrame
	):
    """ 
	Builds the data that we want to send to the API

		We will be sending a ticker and date, 
    """



	return



async def process_tickers(
	payload_dictionary: {}
	):
	"""
	"""
	# Store the results


    # Limit concurrency with a semaphore and use a 0.01 second or 10ms space 
    # between requests to stay under 100 requests per second
    semaphore = asyncio.Semaphore(20)
    rate_limit_delay = 0.01

	# Create tasks for all tickers
	tasks = [get_reference_data(ticker, date, semaphore) for ticker, date in payload_dictionary]

	# Process with rate limiting 
	for task in tqdm.as_completed(tasks, desc="Fetching reference data", total=len(tasks)):
		# Get the reu



	await.asyncio.sleep(rate_limit_delay)


	




async def get_reference_data():




	async with semaphore:
		try:
			# Run the synchronous RESTClient call in a thread
			events = await asyncio.to_thread(client.get_ticker_events, ticker)


			reference


	tickers = []
	for t in client.list_tickers(
		ticker="tickek",
		active="status",
		order="asc",
		limit="1000",
		sort="ticker",
		):




		tickers.append(t)

	print(tickers)

	pass
