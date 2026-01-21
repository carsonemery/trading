import pandas as pd
from typing import Optional

def adjust_with_cumfacshr(
    df: pd.DataFrame,
    adjust_factor: str = 'dlycumfacshr',
    product_fields: Optional[list[str]] = None, 
    division_fields: Optional[list[str]] = None
    ) -> pd.DataFrame:
    """
    Adjusts prices and volumes for splits using the dlycumfacshr field.

    Args:
        adjust_factor: The factor/variable that we use to perform the adjustment, such as the cumulative adjustment factor from crsp (dlycumfacshr)
        product_fields: includes volume, and shares outstanding
        division_fields: includes high, low, close, (price)

    Returns a dataframe with original columns PLUS new adjusted columns:
    - adj_open, adj_high, adj_low, adj_close (split-adjusted prices)
    - adj_volume, adj_shrout (split-adjusted volume/shares)
    """
    df = df.copy()
    
    # Fill any missing cumulative factors with 1.0 (no adjustment), we have mostly confirmed missing values can be filled this way
    df[f'{adjust_factor}'] = df[f'{adjust_factor}'].fillna(1.0)
    
    # To get split-adjusted prices (adjusted to most recent date):
    # Divide prices by the cumulative factor
    for item in (division_fields or []):
        df[f'adj_{item}'] = df[f'{item}'] / df[f'{adjust_factor}']        

    # For volume/shares - multiply (opposite direction)
    # When shares split 2-for-1, historical volume should be doubled to be comparable
    for item in (product_fields or []):
        df[f'adj_{item}'] = df[f'{item}'] * df[f'{adjust_factor}']
        
    return df