import time

import pandas as pd


def aggregate_publisher_data(df: pd.DataFrame):
    """
    create a single record of truth for each stock
    
    """
    # get the row indicies with max volume for each group
    idx_max_volume = df.groupby(['ts_event', 'symbol'])['volume'].idxmax()

    # get the full rows with max volume 
    primary_exchange = df.loc[idx_max_volume][['ts_event', 'symbol', 'rtype', 'publisher_id', 'open', 'close']]

    # do regular aggregation for high, low and volume
    aggregated = df.groupby(['ts_event', 'symbol']).agg({
        'high': 'max',
        'low': 'min',       
        'volume': 'sum' 
    }).reset_index()

    # merge the primary exchange info with the aggregated data
    result = aggregated.merge(
        primary_exchange,
        on=['ts_event', 'symbol'],
        how='left'
    )

    # Reorder columns
    result = result[['ts_event', 'rtype', 'publisher_id', 'symbol', 'high', 'low', 'open', 'close', 'volume']]

    return result

# create knowledge_date, this code needs to be looked at
def knowledge_date(df: pd.DataFrame):
    """
    Create a date representing when information was actually available for trading decisions
    """
    df = df.sort_values(['instrument_id', 'ts_event'])
    
    # Shift the date forward by one trading day
    # Use the next row's date as the "knowledge date" for current row's data
    df['knowledge_date'] = df.groupby('instrument_id')['ts_event'].shift(-1)
    
    # For the last row of each stock, manually set knowledge_date
    # (though you'll likely drop these rows anyway due to missing future data)
    df['knowledge_date'] = df['knowledge_date'].fillna(df['ts_event'] + pd.Timedelta(days=1))
    
    return df 