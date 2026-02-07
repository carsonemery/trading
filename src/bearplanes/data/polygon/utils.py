"""Polygon-specific utility functions."""
import pandas as pd


def add_datetime(
    df: pd.DataFrame,
    column_name: str
) -> pd.DataFrame:
    """
    Convert nanosecond unix timestamp to pandas datetime object.
    
    Keeps as pandas datetime64[ns] (normalized to date) for pandas operations 
    and proper comparison with events/splits dates.
    
    Args:
        df: DataFrame with timestamp column
        column_name: Name of column containing nanosecond unix timestamps
    
    Returns:
        DataFrame with added 'date' column in first position
    """
    # Convert to pandas datetime and normalize to date (removes time component)
    # Keeping as pandas datetime (not Python date) for pandas operations like .dt.year
    df['date'] = pd.to_datetime(df[column_name], unit='ns').dt.normalize()
    
    # Reorder columns to put 'date' first
    cols = ['date'] + [col for col in df.columns if col != 'date']
    return df[cols]

