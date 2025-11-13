import pandas as pd
import re

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
        tickers_to_remove.append(extract_tickers(ticker))

    # Create a single boolean mask of all tickers we want to remove 
    mask_to_remove = df['ticker'].isin(tickers_to_remove)

    # Negate the mask using the tilde operator, this selects all rows where the ticker is NOT in the list to remove
    mask_to_keep = ~mask_to_remove

    # Apply the mask
    OHLCV_filtered = df[mask_to_keep]

    return OHLCV_filtered

def extract_tickers(
    ticker: str) -> str:
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
    if re.match(r'^(ZVZZT|ZWZZT)$', ticker):
        return ticker

    # Match non-equities (lowercase/period suffixes)
    if re.match(r'^([^a-z.]*)([a-z.].*)$', ticker):
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

    # Remove the duplicate rows and return the cleaned dataframe



    return OHLCV_data

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

def normalize_datatypes():
    """
        Normalize the data types in each of the columns of our data, 
        preparing it for further processing and feature engineering.
    """

def run_cleaning():
    """
        Run all cleaning functions to return a sanitized dataframe
        Meant to be the public API by chaining together the various functions in this file
    """

    pass