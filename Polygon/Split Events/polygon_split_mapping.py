import pandas as pd
from pandas.core import frame

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

# Example: Reverse split (e.g., CYCC 1-for-15: split_from=15, split_to=1)
# factor_prices = 15/1 = 15 → historical prices *= 15 (higher)
# factor_vol = 1/15 ≈ 0.0667 → historical vol *= 0.0667 (lower)

def map_splits(
    split_info: pd.DataFrame(),
    historical_data: pd.DataFrame()
    ) -> pd.DataFrame:
    """
    Args:
        split_info - roughly 4100 rows
        historical_data - roughly 20M rows 
    
    We will also be iterating through series of stock data on grouped by adjusted_ticker
    this is probably around 10-15k individual series based on unique tickers

    split_info.head()
                ticker        date  split_from  split_to
        0   ITOT  2016-07-25         1.0       2.0
        1   DOGZ  2023-11-07        20.0       1.0
        2    ITM  2018-10-26         2.0       1.0
        3   DLPN  2020-11-27         5.0       1.0
        4   DLPN  2024-10-16         2.0       1.0
    """
    # Confirm the split info will be sorted by date in descending order, older dates first
    split_info = split_info.sort_values(by='date', ascending=False)

    df_copy = historical_data.copy()

    # Initialize adjusted columns by copying original values
    # This preserves original data and creates new adjusted columns
    # These will be adjusted futher in the method
    df_copy['adjusted_open'] = df_copy['open'].copy()
    df_copy['adjusted_high'] = df_copy['high'].copy()
    df_copy['adjusted_low'] = df_copy['low'].copy()
    df_copy['adjusted_close'] = df_copy['close'].copy()
    df_copy['adjusted_volume'] = df_copy['volume'].copy()

    # Group the OHLCV data in the historical dataframe into series by each ticker/(companies) common "adjusted_ticker"
    grouped_data = df_copy.groupby('adjusted_ticker')

    # Iterate through each row of the splits dataframe using itertuples (faster than iterrows and we dont need to modify the splits dataframe)
    for row in split_info.itertuples():
        # access the attributes of that row (tuple)
        split_ticker = row.ticker
        split_date = row.date
        split_from = row.split_from
        split_to = row.split_to

        # Calculate the adjustment ratios
        # General formula (same for forward and reverse):
        adjustment_factor_prices = split_from / split_to
        adjustment_factor_vol = split_to / split_from

        # For each series 
        for _, series in grouped_data:
            # Check if this series contains the ticker we're looking for 
            if split_ticker in series['ticker'].values:
                # Create mask on the series (already filtered to this adjusted_ticker)
                # No need to check adjusted_ticker since we're already in that group
                mask = (series['ticker'] == split_ticker) & (series['date'] < split_date)
                
                # Apply adjustments directly using the mask
                # The mask has indices from df_copy (since series is a view), so pandas aligns correctly
                df_copy.loc[mask, 'adjusted_open'] *= adjustment_factor_prices
                df_copy.loc[mask, 'adjusted_high'] *= adjustment_factor_prices
                df_copy.loc[mask, 'adjusted_low'] *= adjustment_factor_prices
                df_copy.loc[mask, 'adjusted_close'] *= adjustment_factor_prices
                df_copy.loc[mask, 'adjusted_volume'] *= adjustment_factor_vol


    return df_copy


