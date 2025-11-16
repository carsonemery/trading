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
from tqdm import tqdm as sync_tqdm
from tqdm.asyncio import tqdm
from pathlib import Path
from dataclasses import dataclass
import aiofiles
import json
import sys

# Add parent directory to path to import cleaning module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from cleaning import run_cleaning

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
	df: pd.DataFrame,
	threshold: int
	) -> List[Tuple[str, date]]:
	""" 
	Builds the data that we want to send to the reference API

		Args:
			df: pandas dataframe containing the full OHLCV data 
			threshold: the frequency with which we call the API on the same ticker
	"""
	
	# Get each unique ticker with its first and last date
	ticker_dates = df.groupby('ticker').agg(
		first_date=('date', 'min'),
		last_date=('date', 'max')
	).reset_index()

	# Generate list of (ticker, date) tuples for each ticker from first_date to first_date + threshold
	list_of_calls = []

	# for task in tqdm.as_completed(tasks, desc="Fetching reference data", total=len(tasks)):

	# Now fill in dates for each ticker group with the threshold we choose
	for _, row in ticker_dates:
		ticker = row['ticker']
		first_date = pd.to_datetime(row['first_date'])
		last_date = pd.to_datetime(row['last_date'])
		
		# Generate dates from first_date to end_date, with threshold as the interval
		date_range = pd.date_range(
			start=first_date,
			end=last_date,
			freq=f'{threshold}D'
		)
		
		# Add (ticker, date) tuple for each date
		for date in date_range:
			list_of_calls.append((ticker, date.date()))
	
	return list_of_calls

async def process_tickers(
	payload_list: [],
	batch_size: int,
	checkpoint_dir: Path
	):
	"""
	Process tickers and checkpoint results incrementally.
	"""
	# Store the results
	list_ref_data_buffer = []
	list_failed_refs = []

	# Limit concurrency with a semaphore and use a 0.01 second or 10ms space 
	# between requests to stay under 100 requests per second
	semaphore = asyncio.Semaphore(20)
	rate_limit_delay = 0.01

	# Create tasks for all tickers
	tasks = [get_reference_data(ticker, date, semaphore) for ticker, date in payload_list]

	# Track the batch numbers separately for success and failed
	success_batch_counter = 0
	failed_batch_counter = 0

	# Process with rate limiting 
	for task in tqdm.as_completed(tasks, desc="Fetching reference data", total=len(tasks)):
		result_type, ref_data = await task

		# Handle the successful responses
		if result_type == 'success':
			# add to the list_of_ref_data
			list_ref_data_buffer.append(ref_data)
			
			# Save the results every batch_size of successful requests
			if len(list_ref_data_buffer) >= batch_size:
				success_batch_counter += 1
				checkpoint_file = checkpoint_dir / "success" / f"batch_{success_batch_counter:04d}.json"
				checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
				await save_checkpoint(list_ref_data_buffer, "json", checkpoint_file)
				list_ref_data_buffer.clear()

		else:
			list_failed_refs.append(ref_data)
			
			# Save the failed results every batch_size
			if len(list_failed_refs) >= batch_size:
				failed_batch_counter += 1
				checkpoint_file = checkpoint_dir / "failed" / f"batch_{failed_batch_counter:04d}.json"
				checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
				await save_checkpoint(list_failed_refs, "json", checkpoint_file)
				list_failed_refs.clear() 

		await asyncio.sleep(rate_limit_delay)

async def get_reference_data(
	ticker: str,
	date,
	semaphore: asyncio.Semaphore
	):
	"""
	"""
	async with semaphore:
		try:
			def fetch_ref_data():
				
				ref_data_generator = client.list_tickers(
					ticker=ticker,
					date=date,
					active="true",
					order="asc",
					limit="1000",
					sort="ticker",
				)
			
				# Convert generator into a list that we return to the awaited references variable
				return list[bytes | RefData(ref_data_generator)]

			# Call the fetch_ref_data function in a thread 
			references = await asyncio.to_thread(fetch_ref_data)

			# Transform the reference data into dataclass objects
			# This happens AFTER fetch_ref_data() has returned
			date_str = date.strftime('%Y-%m-%d') if isinstance(date, date) else str(date)
			# Construct reference data objects of type RefData dataclass 
			reference_objects = [
				RefData(
					ticker=getattr(reference, 'ticker', ''),
					name=getattr(reference, 'name', ''),
					market=getattr(reference, 'market', ''),
					locale=getattr(reference, 'locale', ''),
					primary_exchange=getattr(reference, 'primary_exchange', ''),
					type=getattr(reference, 'type', ''),
					active=getattr(reference, 'active', False),
					currency_name=getattr(reference, 'currency_name', ''),
					cik=getattr(reference, 'cik', ''),
					composite_figi=getattr(reference, 'composite_figi', ''),
					share_class_figi=getattr(reference, 'share_class_figi', ''),
					last_updated_utc=getattr(reference, 'last_updated_utc', ''),
					date_param=date_str
				)
				for reference in references
			]

			# Now return get_reference_data data class objects
			return ("sucess", reference_objects)

		# Catch the failed requests to track which tickers we did not get any name change info from 
		# for now we just send both failed for all types of failures
		except BadResponse as e:
			print(f"BadResponse for {ticker}: {e}")
			return ("failed", ticker)
		except Exception as e:
			# Catch any other exceptions (like network errors, etc.)
			print(f"Error fetching splits for {ticker}: {e}")
			return ("failed", ticker)

async def save_checkpoint(
	data,
	type_save: str,
	filepath
	):
	"""		
	Args:
		data: the data we are saving 
		type_save: the type of filing we are saving the data to
			supported types are
			"csv"
			"json"
			"parquet"
			"pickle"
		filepath: the filpath we are saving the data to
	
	"""

	match type_save:
		case "csv":
			pass  # TODO: implement CSV saving

		case "json":
			async with aiofiles.open(filepath, 'w') as f:
				await f.write(json.dumps(data))

		case "parquet":
			pass  # TODO: implement Parquet saving

		case "pickle":
			pass  # TODO: implement Pickle saving

		case _:
			raise ValueError(f"Unsupported file type: {type_save}")


async def main():
	# Load in OHLCV Data 
	# csv_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26.csv'
	# df = pd.read_csv(csv_path)

	# df = run_cleaning(df)

	# Save the splits dataframe
	parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\cleaned_historical_data_2016-01-01_to_2025-10-26.parquet'
	df = df.read_parquet(parquet_path)

	print(df)

	payload = build_payload(df, 14)

	print(payload)

if __name__ == "__main__":
    asyncio.run(main())