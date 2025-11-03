import pandas as pd

def add_datetime(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert nanosecond unix timestamp to pandas datetime object.
    Keeps as pandas datetime64[ns] (normalized to date) for pandas operations 
    and proper comparison with events/splits dates.
    """
    # Convert to pandas datetime and normalize to date (removes time component)
    # Keeping as pandas datetime (not Python date) for pandas operations like .dt.year
    df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.normalize()
    
    # Reorder columns to put 'date' first
    cols = ['date'] + [col for col in df.columns if col != 'date']
    return df[cols]