import numpy as np
import pandas as pd

"""
Volume based feature calculations 
Both historical and offset 
"""

def volume_ratio_rolling(
    df: pd.DataFrame, 
    lookback: int) -> pd.DataFrame:
    """Calculate rolling volume ratio feature.
    
    Computes the volume ratio by dividing the current day's volume 
    by the mean volume over the specified lookback window. This helps 
    identify periods of unusually high or low trading activity.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        lookback: Number of days to look back for the rolling mean calculation.
            Must be a positive integer.
    
    Returns:
        DataFrame with an added column 'volume_ratio_{lookback}_days_rolling' 
        containing the calculated volume ratios.
    
    Example:
        >>> df = pd.DataFrame({'volume': [100, 150, 200, 180, 220]})
        >>> df = volume_ratio_rolling(df, lookback=3)
        >>> # Creates column 'volume_ratio_3_days_rolling'
    """

    df[f'volume_ratio_{lookback}_days_rolling'] = df['volume'] / df['volume'].rolling(window=lookback).mean()

    return df

def volume_ratio_rolling_offset(
    df: pd.DataFrame,
    lookback: int,
    offset: int
    ) -> pd.DataFrame:
    """Calculate rolling volume ratios for offset periods


    """

    df[f'volume_ratio_{offset}_offset_{lookback}_lookback'] = df['volume'].shift(offset) / df['volume'].shift(offset).rolling(lookback).mean()
    # should I really do covariance ? what does it add?
    
    return df

def volume_percentiles(
    df: pd.DataFrame, 
    lookback: int,
    ) -> pd.DataFrame:
    """Calculate rolling volume percentile rank feature.
    
    Computes the percentile rank of the current day's volume relative 
    to the volume over the specified lookback window. Returns values 
    between 0 and 1, where 1 indicates the highest volume in the period.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        lookback: Number of days to include in the rolling window for 
            percentile calculation. Must be a positive integer.
    
    Returns:
        DataFrame with an added column 'volume_percentile_{lookback}_days_rolling' 
        containing the percentile ranks (values between 0 and 1).
    
    Example:
        >>> df = pd.DataFrame({'volume': [100, 150, 200, 180, 220]})
        >>> df = volume_percentiles(df, lookback=5)
        >>> # Creates column 'volume_percentile_5_days_rolling'
        >>> # A value of 0.8 means current volume is in 80th percentile
    """

    df[f'volume_percentile_{lookback}_days_rolling'] = df['volume'].rolling(window=lookback).rank(pct=True)

    return df

def volume_trends(
    df: pd.DataFrame,
    short_lookback: int,
    long_lookback: int
    ) -> pd.DataFrame:

    """Calculuate volume trends for the current day
    
    volume trends are 
    Args:

    """

    df[f'volume_trend_direction_{short_lookback}_avg_over_{long_lookback}_avg'] = df['volume'].rolling(window=short_lookback).mean() / df['volume'].rolling(window=long_lookback).mean() 

    return df

def volume_offset_stats(
    df: pd.Dataframe,
    offset: int,
    lookback: int
    ) -> pd.DataFrame:

    """ Calculates the average and standard deviation for 


    """ 

    df[f'{offset}_offset_window_mean_{lookback}_days'] = df['volume'].shift(offset).rolling(lookback)
    df[f'{offset}_offset_window_std_{lookback}_days'] = df['volume'].shift(offset).ro

def volume_deviation_frequency_offset(
    df: pd.DataFrame,
    lookback: int,
    offset: int
    ) -> pd.DataFrame:
    """ Counts the number of times volume reached different standard deviation thresholds for the
    lookback period in the offset
    deviations: 1,2,3,4,5,6

    """



    return df

def returns_on_volume_deviations(
    df: pd.DataFrame,
    lookback: int, 
    offset: int
    ) -> pd.DataFrame:
    """ Calculate the return on the days of outlier deviations in volume in the offset period

    """

    return df


