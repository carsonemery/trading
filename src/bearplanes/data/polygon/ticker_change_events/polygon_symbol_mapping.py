import numpy as np
import pandas as pd
from tqdm import tqdm

#EXAMPLE WALKTHROUGH
# OUR MAPPING:
# 'META': [('META', '2022-06-09'), ('FB', '2012-05-18')]
# 'FB': [('FB', '2025-06-26')]  # Some ETF

# For row with ticker='FB', date='2020-01-01':
# - Check META mapping: FB appears with date '2012-05-18' <= '2020-01-01' ✓
# - Check FB mapping: FB appears with date '2025-06-26' <= '2020-01-01' ✗
# - Result: Use 'META' (FB belonged to Meta in 2020)

def prepare_mapping_dataframe(
    reverse_mapping: {},
    start_date: str
    ) -> pd.DataFrame:
    """ Converts the reverse_mapping dictionary into a dataframe

    Format Before (Examples):

    KVP:    
        Mapping: ('DRD', [('DRD', '2012-01-03'), ('DROOY', '2007-08-20'), ('DROOD', '2007-07-23')])
    Type: 
        Mapping: (<class 'str'>, <class 'list'>)

    KVP:
        Mapping: ('PWRD', [('PWRD', '2025-02-03'), ('NETZ', '2022-02-03')])
    Type:
        Mapping: (<class 'str'>, <class 'list'>)


    Format After:

    Long format (what we want)
    current_ticker | historical_ticker | change_date
    DRD           | DRD               | 2012-01-03
    DRD           | DROOY             | 2007-08-20
    DRD           | DROOD             | 2007-07-23
    PWRD          | PWRD              | 2025-02-03
    PWRD          | NETZ              | 2022-02-03

    """

    # Convert start date to Timestamp if provided 
    if start_date is not None:
        start_date = pd.to_datetime(start_date).normalize()

    # Create a list of dictionary objects to store the information we will convert into a dataframe
    # this is faster than concating/appending each row to the dataframe in the loop
    list_of_ticker_changes = []

    # Add progress bar for processing ticker mappings
    for current_ticker, history_list_of_tuples in tqdm(reverse_mapping.items(), desc="Preparing mapping dataframe", unit="ticker"):
        
        # Loop through each tuple in the list for this ticker
        for historical_ticker, change_date in history_list_of_tuples:

            # Convert date to Timestamp
            if not isinstance(change_date, pd.Timestamp):
                change_date = pd.to_datetime(change_date).normalize()

            # Filter out dates (and thereby name change events) that happened
            # before our historical OHLCV data begins (decreases the number of operations
            # we have to do later in map_symbols)
            if start_date is not None and change_date < start_date:
                continue # continue will skip the tuple

            # Add to our list 
            list_of_ticker_changes.append({
                'current_ticker': current_ticker,
                'historical_ticker': historical_ticker,
                'change_date': change_date
            })
        
    # Convert the list 
    mapping_df = pd.DataFrame(list_of_ticker_changes)

    return mapping_df

def map_symbols(
    reverse_mapping_dict: {},
    historical_data: pd.DataFrame,
    start_date: str
    ) -> pd.DataFrame:
    """ 
        Takes a symbol mapping dataframe and OHLCV dataframe and returns the dataframe with a mapped symbols column titled 
        'adjusted_ticker'.
    """

    # Copy the original dataframe to preserve all columns
    print("Creating copy of dataframe")
    historical_data = historical_data.copy()

    # convert mapping dictionary to Dataframe
    mapping_df = prepare_mapping_dataframe(reverse_mapping_dict, start_date)

    # Sort historical_data by ticker and date for merge_asof
    # CRITICAL: merge_asof requires the join key (date) to be sorted within each group (ticker)
    print("Sorting historical data for merge_asof...")
    with tqdm(total=1, desc="Sorting historical data", unit="operation") as pbar:
        historical_data_sorted = historical_data.sort_values(['ticker', 'date'], kind='stable').reset_index(drop=True)
        pbar.update(1)
    
    # Also ensure mapping_df is sorted correctly
    # It should already be sorted from prepare_mapping_dataframe, but let's be explicit
    print("Sorting mapping dataframe...")
    with tqdm(total=1, desc="Sorting mapping dataframe", unit="operation") as pbar:
        mapping_df_sorted = mapping_df.sort_values(['historical_ticker', 'change_date'], kind='stable').reset_index(drop=True)
        pbar.update(1)

    # Perform the merge_asof operation
    # NOTE: merge_asof is a single atomic pandas operation, so we can't show progress during execution
    # It's optimized C code that runs all at once. We'll just show a message.
    print(f"Performing merge_asof on {len(historical_data_sorted):,} rows (this may take a while)...")
    mapped_historical_data  = pd.merge_asof(
        # Left - the ~23M rows of OHLCV data
        historical_data_sorted,
        # Right - ~ 90k rows of ticker changes
        mapping_df_sorted,

        # Join condition - Find matching dates 
        left_on = 'date',
        right_on='change_date',

        # Grouping - only match within the same ticker
        left_by='ticker',
        right_by='historical_ticker',

        # Direction - find the most recent change on or before this date
        direction='backward'
    )
    print("Merge_asof completed!")

    # Handle tickers with no mapping 
    # I believe this is safe to do for now, because we filled tickers that did not return a mapping with the date 
    # 2025-11-08, which is earlier than any date in the OHCLV dataframe so the merge_asof should have some nas we can fill
    # with the original ticker name
    print("Filling missing mappings and cleaning up...")
    mapped_historical_data['adjusted_ticker'] = mapped_historical_data['current_ticker'].fillna(mapped_historical_data['ticker'])

    # Clean up temp columns
    mapped_historical_data = mapped_historical_data.drop(columns=['historical_ticker', 'change_date', 'current_ticker'])

    return mapped_historical_data