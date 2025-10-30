import pandas as pd

#EXAMPLE WALKTHROUGH
# OUR MAPPING:
# 'META': [('META', '2022-06-09'), ('FB', '2012-05-18')]
# 'FB': [('FB', '2025-06-26')]  # Some ETF

# For row with ticker='FB', date='2020-01-01':
# - Check META mapping: FB appears with date '2012-05-18' <= '2020-01-01' ✓
# - Check FB mapping: FB appears with date '2025-06-26' <= '2020-01-01' ✗
# - Result: Use 'META' (FB belonged to Meta in 2020)

def map_symbols(
    reverse_mapping: {},
    historical_data: pd.DataFrame
    ) -> pd.DataFrame:
    """ 
        Takes a symbol mapping dictionary and dataframe and returns the dataframe with a mapped symbols column
    """

    mapped_data = pd.DataFrame()

    for index, row in historical_data.iterows():
        rows_symbol = row['symbol']
        rows_date = row['date']

        # Call helper function to get the actively traded symbol for this row based on the 
        # reverse lookup dictionary
        current_ticker = get_current_ticker_for_historical_date(rows_symbol, rows_date, reverse_mapping)

        mapped_data.at[index, 'current_ticker'] = current_ticker
    
    return mapped_data 


def get_current_ticker_for_historical_date(
    rows_symbol, 
    rows_date, 
    reverse_mapping
    ) -> str:
    """
    """

    # Default the current_ticker to original symbol if we do not find anything
    current_ticker = rows_symbol

    for maping_key, maping_list in reverse_mapping.values():
        for item in maping_list:
            # These are from the tuples
            historical_ticker = item[0]
            change_date = item[1]

            # If the ticker of the tuple is equal to the symbol we are checking from the dataframe, and
            # the date of ticker change is less than the date of the current row, update the current_ticker
            # the key of the reverse lookup
            if (historical_ticker == rows_symbol and change_date <= rows_date):
                current_ticker = maping_key

    return current_ticker


