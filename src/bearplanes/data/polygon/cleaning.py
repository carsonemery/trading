import re
from typing import Optional

import pandas as pd


def sanitize_non_string_tickers(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    Filters out rows with NaN ticker values (824 rows in current dataset).
    Most efficient: dropna on subset.
    """
    OHLCV_filtered = df.dropna(subset=['ticker'])

    return OHLCV_filtered

def sanitize_non_equities(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    This function removes rights, warrants,  pre-merger spacs, pref shares, etc
    """

    unique_tickers = df['ticker'].unique()

    # Collect a list of all the tickers series we should remove based on the 
    # regex patterns in the extract_tickers helper function
    tickers_to_remove = []

    for ticker in unique_tickers:
        if remove_ticker := extract_tickers(ticker):
            tickers_to_remove.append(remove_ticker)

    # Create a single boolean mask of all tickers we want to remove 
    mask_to_remove = df['ticker'].isin(tickers_to_remove)

    # Negate the mask using the tilde operator, this selects all rows where the ticker is NOT in the list to remove
    mask_to_keep = ~mask_to_remove

    # Apply the mask
    OHLCV_filtered = df[mask_to_keep]

    return OHLCV_filtered

def extract_tickers(
    ticker: str) -> Optional[str]:
    """
        Helper function used to find tickers that match our filtering criteria.

        Based on testing in the exploring_data_quality.ipynb, about 11% of the tickers are not vanilla equities.

        Examples of what we are removing include tickers entirely made up of or including these prefixes:
            .A
            .B
            .C
            .WS
            .U
            pC
            pB
            w
            rw
            TEST

        We are not aggresively filtering purely by number of characters for something like > 1

        If the ticker passed in does not match any of the cases below, we return None, keeping that ticker and its series in the data.
    """
    # Match test tickers (case-insensitive)
    if re.search(r'(?i)test', ticker):
        # Handle test tickers
        return ticker

    # Match ZVZZT/ZWZZT test tickers
    elif re.match(r'^(ZVZZT|ZWZZT)$', ticker):
        return ticker

    # Match non-equities (lowercase/period suffixes)
    elif re.match(r'^([^a-z.]*)([a-z.].*)$', ticker):
        return ticker

    # If no pattern matches None is passed to the calling source by default which is an instance of class 'NoneType'
    pass

def sanitize_duplicates(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """ 
        Based on analysis in exploring_data_quality.ipynb, there are 56 rows of duplicates in the entire OHLCV dataset
        and they all take place on 2019-08-13.

        Most are also very low volume, and because of the insignifance of this single day error we just remove all 
        56 rows.
            
    """ 

    # Find the duplcate rows
    duplicate_rows = df[df.duplicated(subset=['ticker', 'date'], keep=False)]

    # Create a boolean mask using the duplicate rows
    mask = ~df.set_index(['ticker', 'date']).index.isin(duplicate_rows.set_index(['ticker', 'date']).index)

    OHLCV_filtered = df[mask].reset_index(drop=True)    

    return OHLCV_filtered

def sanitize_low_volume(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """ Removes a continuous series of ticker data based on a volume threshold that every day of the series must meet.

        Currently choosing to filter out any ticker that did not have daily volume greater than 1,000 or a price of $0.01 for 
        its entire trading series. This only equates to roughly 50 tickers BEFORE calling sanitize_non_equities()
        so impact is very minimal.
    """

    # Find and analyze the number of tickers that have MAX 100 shares volume across their entire series 
    # Compute max volume and max price per ticker in one pass
    ticker_stats = df.groupby('ticker').agg({
        'volume': 'max',
        'close': 'max'
    }).reset_index()

    # Filter tickers into a list that are below the criteria
    invalid_tickers = ticker_stats[
        (ticker_stats['volume'] < 1000) |
        (ticker_stats['close'] < 0.01)
    ]['ticker'].tolist()

    # Create a single boolean mask of all tickers we want to remove 
    mask_to_remove = df['ticker'].isin(invalid_tickers)

    # Negate the mask using the tilde operator, this selects all rows where the ticker is NOT in the list to remove
    mask_to_keep = ~mask_to_remove

    # Apply the mask
    OHLCV_filtered = df[mask_to_keep]

    return OHLCV_filtered

def normalize_datatypes(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    This function performs a couple basic processes to convert data types and reorder the columns or the dataframe

        Normalize the data types in each of the columns of our data, 
        preparing it for further processing and feature engineering.
        
        Converts ticker to category dtype for memory efficiency and performance
        in groupby, filtering, and sorting operations down the line. 

        Explicitly sets the types of all columns of the dataframe

        Order and type of columns will be as follows:
        ============================================
        date                   datetime64[ns]
        unix_nsec_timestamp             int64
        ticker                       category
        open                          float64
        close                         float64
        high                          float64
        low                           float64
        volume                          int64
        transactions                    int64

    """
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Explicitly set the datatypes for each column
    df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'window_start'], unit='ns').dt.normalize()
    df.loc[:, 'window_start'] = df.loc[:, 'window_start'].astype('int64')
    df.loc[:, 'ticker'] = df.loc[:, 'ticker'].astype('category')
    df.loc[:, 'open'] = df.loc[:, 'open'].astype('float64')
    df.loc[:, 'close'] = df.loc[:, 'close'].astype('float64')
    df.loc[:, 'high'] = df.loc[:, 'high'].astype('float64')
    df.loc[:, 'low'] = df.loc[:, 'low'].astype('float64')
    df.loc[:, 'volume'] = df.loc[:, 'volume'].astype('int64')
    df.loc[:, 'transactions'] = df.loc[:, 'transactions'].astype('int64')

    # Rename window_start to unix_nsec_timestamp
    df = df.rename(columns={'window_start': 'unix_nsec_timestamp'})

    # Reorder columns to match desired order: date, unix_nsec_timestamp, ticker, open, close, high, low, volume, transactions
    desired_order = ['date', 'unix_nsec_timestamp', 'ticker', 'open', 'close', 'high', 'low', 'volume', 'transactions']
    OHLCV_filtered = df[desired_order]

    return OHLCV_filtered

def sanitize_short_series(
    df: pd.DataFrame,
    removal_threshold: int
    ) -> pd.DataFrame:
    """
    Removes tickers that have fewer than the specified number of unique trading days.
    
    This filters out tickers with insufficient data for meaningful analysis.
    Common examples include test tickers, very short-lived IPOs, or data errors.

    Some distributions on small values BEFORE any filtering from the ipynb

    Threshold | Tickers Removed | % of Total | Rows Removed | % of Rows
    ----------------------------------------------------------------------
        10   |       544      |   2.30%  |      2,463   |   0.01%
        20   |       943      |   3.99%  |      8,355   |   0.04%
        30   |     1,292      |   5.47%  |     16,518   |   0.07%
        50   |     1,690      |   7.15%  |     31,991   |   0.14%
        60   |     1,845      |   7.81%  |     40,390   |   0.17%
        90   |     2,383      |  10.09%  |     80,426   |   0.34%
        120  |     2,926      |  12.39%  |    137,460   |   0.59%

    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with ticker and date columns
    removal_threshold : int, 
        Minimum number of unique trading days required to keep a ticker.

    Returns:
    --------
    pd.DataFrame with short-series tickers removed
    """
    
    tickers_too_little_data = []

    for ticker, group in df.groupby('ticker', observed=True):
        dates = group['date'].unique()

        if len(dates) < removal_threshold:
            tickers_too_little_data.append(ticker)
    
    # Create a boolean mask of all tickers we want to remove
    mask_to_remove = df['ticker'].isin(tickers_too_little_data)
    
    # Negate the mask using the tilde operator, this selects all rows where the ticker is NOT in the list to remove
    mask_to_keep = ~mask_to_remove
    
    # Apply the mask
    OHLCV_filtered = df[mask_to_keep].reset_index(drop=True)
    
    return OHLCV_filtered

def sanitize_warrants_rights_units_non_obvious(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    Removes warrants, rights, and units by checking for 5-character tickers ending in U, W, or R
    that have a matching 4-character base ticker.
    """
    unique_tickers = df['ticker'].unique()
    tickers_to_remove = []

    for ticker in unique_tickers:
        if isinstance(ticker, str) and len(ticker) == 5 and ticker[4] in ['U', 'W', 'R']:
            base_ticker = ticker[:4]
            if base_ticker in unique_tickers:
                tickers_to_remove.append(ticker)

    mask_to_keep = ~df['ticker'].isin(tickers_to_remove)

    return df[mask_to_keep]

def run_cleaning(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """
        Run all cleaning functions to return a sanitized dataframe
        Meant to be the public API by chaining together the various functions in this file
    """
    print("Dataframe before cleaning:")
    print("================================")
    print(f"Count of Unique Tickers: {len(df['ticker'].unique())}")
    print(f"Number of Rows: {len(df)}")
    print("================================")

    # 1. Drop rows with NaN ticker values
    sanitized_nan = sanitize_non_string_tickers(df)

    # 2. Update types, rename and reorder columns 
    normalized_dtypes = normalize_datatypes(sanitized_nan) 

    # 3. Remove duplicates
    sanitized_duplicates = sanitize_duplicates(normalized_dtypes)

    # 4. Remove low vol 
    sanitized_low_vol = sanitize_low_volume(sanitized_duplicates)

    # 5. Remove tickers with very low total trading days, currently selecting 30 
    # (for context this is also doing very little to the shape of the data)
    sanitized_short_hist = sanitize_short_series(sanitized_low_vol, 30)

    # 6. Clean for non equities as much as we can using obvious suffixes
    sanitized_non_equities = sanitize_non_equities(sanitized_short_hist)

    # 7. Finally clean for non equities with U, R, and W suffixes based on a base ticker match
    sanitizedd_non_equities_non_obvious = sanitize_warrants_rights_units_non_obvious(sanitized_non_equities)

    # Sort the dataframe by ticker and date in ascending order
    # This is required for merge_asof operations and ensures consistent ordering for downstream processing
    OHLCV_processed = sanitizedd_non_equities_non_obvious.sort_values(['ticker', 'date'], kind='stable').reset_index(drop=True)

    print("Dataframe after cleaning:")
    print("================================")
    print(f"Count of Unique Tickers: {len(OHLCV_processed['ticker'].unique())}")
    print(f"Number of Rows: {len(OHLCV_processed)}")
    print("================================")

    return OHLCV_processed