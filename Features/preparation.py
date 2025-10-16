import pandas as pd

# things we need to do before adding actual features 

# 1) create a single record of truth for each stock each day, taking the min/max of its
# publishers for low/high, and the open/close from the publlisher with the most volume 
# 
# 2) create a "knowledge_date" for each row, which is the date from the next bar for that 
# stock so that there is no look ahead bias
# 













# create knowledge_date, this code needs to be looked at
def knowledge_date(df):
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