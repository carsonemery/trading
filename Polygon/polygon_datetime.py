import pandas as pd

# Sole purpose of this file and function is providing a method which adds a datetime column to a dataframe that contains unix nanosecond time stamps
# Polygons exact definition: 
############################
# # window_start
# timestamp - integer
# The Unix nanosecond timestamp for the start of the aggregate window.

def add_datetime(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert nanosecond unix timestamp to datetime object.
    Keeps datetime as datetime (not string) for proper comparison with events/splits dates.
    Optionally adds a string-formatted column if needed.
    """
    # Convert to datetime object (keep as datetime, not string) for comparisons
    df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.date
    
    # Optional: Add string format column if we need it for display/export
    # df['date_str'] = pd.to_datetime(df['window_start'], unit='ns').dt.strftime('%Y-%m-%d')
    
    # Reorder columns to put 'date' first
    cols = ['date'] + [col for col in df.columns if col != 'date']
    return df[cols]

