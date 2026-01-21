from IPython.core.magic import on_off
import pandas as pd
import numpy as np

""" Bolinger band features """


def bb_bands(
    df: pd.DataFrame,
    offset: int,
    lookback: int,
    std: int = 2
    ) -> pd.DataFrame:
    """ Responsible for calculating the upper, middle and lower bands of the bolinger band indicator.

    Default bands are 2 standard deviations above and below the SMA

    """

    # Calculate the middle band using a standard simple moving average
    df[f'bb_SMA_{offset}_offset_{lookback}_lookback'] = df['close'].shift(offset).rolling(lookback).mean()

    # Calculate the upper band using a standard deviation std times the SMA (defaults to standard 2x std deviation)
    df[f"bb_upperband_{offset}_offset_{lookback}_lookback"] = df[f'bb_SMA_{offset}_offset_{lookback}_lookback'] + (
        df['close'].shift(offset).rolling(lookback).std() * std)

    # Calculate the lower band the same way
    df[f"bb_lowerband_{offset}_offset_{lookback}_lookback"] = df[f'bb_SMA_{offset}_offset_{lookback}_lookback'] - (
        df['close'].shift(offset).rolling(lookback).std() * std)
    
    return df

def bb_width_offset(
    df: pd.DataFrame,
    offset: int,
    lookback: int,
    bb_period: int,
    std: int = 2
    ) -> pd.DataFrame:
    """ Calculates bb width in a given lookback period using a given offset.
    
    Width is normalized by the middle band to make it comparable across price levels.

    Args:
        offset: Days back to start the measurement window
        lookback: Number of days to average the width over
        bb_period: Period for calculating the Bollinger Bands (e.g., 20 for 20-day BB)
        std: Standard deviation multiplier for BB calculation
    
    Examples:
        bb_width_pct_20d_now = mean((upper - lower) / middle) over last 20 days
        bb_width_pct_20d_20d_ago = mean((upper - lower) / middle) for days -40 to -20
        bb_width_pct_20d_40d_ago = mean((upper - lower) / middle) for days -60 to -40
        bb_width_pct_20d_60d_ago = mean((upper - lower) / middle) for days -80 to -60
        bb_width_pct_20d_80d_ago = mean((upper - lower) / middle) for days -100 to -80

    """

    # Calculate primary bollinger bands with no offset
    df = bb_bands(df, offset=0, lookback=bb_period, std=std)

    # Calculate normalized width (as percentage of middle band)
    upper_col = f"bb_upperband_0_offset_{bb_period}_lookback"
    lower_col = f"bb_lowerband_0_offset_{bb_period}_lookback"
    middle_col = f'bb_SMA_0_offset_{bb_period}_lookback'
    
    width_pct = (df[upper_col] - df[lower_col]) / df[middle_col]
    
    # Apply offset and calculate rolling mean over lookback period
    feature_name = f'bb_width_pct_{bb_period}bb_{lookback}d_{offset}d_ago'
    df[feature_name] = width_pct.shift(offset).rolling(lookback).mean()

    return df

def bb_price_position(
    df: pd.DataFrame,
    offset: int,
    lookback: int,
    bb_period: int,
    std: int = 2
    ) -> pd.DataFrame:
    """ Calculates a mean ratio of where price traded within the Bollinger Bands over a given lookback period.
    
    The position is calculated as (close - lower) / (upper - lower), which gives:
    - 0.0 = at lower band
    - 0.5 = at middle band
    - 1.0 = at upper band
    - > 1.0 = above upper band
    - < 0.0 = below lower band
    
    Args:
        offset: Days back to start the measurement window
        lookback: Number of days to average the position over
        bb_period: Period for calculating the Bollinger Bands (e.g., 20 for 20-day BB)
        std: Standard deviation multiplier for BB calculation

    Examples:
        bb_position_20d_now = mean((close - lower) / (upper - lower)) over last 20 days
        bb_position_20d_20d_ago = mean((close - lower) / (upper - lower)) for days -40 to -20
        bb_position_20d_40d_ago = mean((close - lower) / (upper - lower)) for days -60 to -40

    """

    # Calculate bollinger bands with no offset
    df = bb_bands(df, offset=0, lookback=bb_period, std=std)

    # Calculate position within bands
    upper_col = f"bb_upperband_0_offset_{bb_period}_lookback"
    lower_col = f"bb_lowerband_0_offset_{bb_period}_lookback"
    
    position = (df['close'] - df[lower_col]) / (df[upper_col] - df[lower_col])
    
    # Apply offset and calculate rolling mean over lookback period
    feature_name = f'bb_position_{bb_period}bb_{lookback}d_{offset}d_ago'
    df[feature_name] = position.shift(offset).rolling(lookback).mean()

    return df

def bb_sequential_trend(
    df: pd.DataFrame,
    short_lookback: int,
    long_lookback: int,
    period: int,
    bb_period: int,
    std: int = 2
    ) -> pd.DataFrame:
    """ Calculates the sequential period to period trend in BB width.
    
    Compares BB width between two sequential time periods to detect volatility
    regime changes. Values > 1.0 indicate expanding volatility, < 1.0 contracting.
    
    Args:
        short_lookback: Days back for the more recent period (e.g., 0 for "now")
        long_lookback: Days back for the older comparison period (e.g., 20)
        period: Number of days to average width over for each period
        bb_period: Period for calculating the Bollinger Bands
        std: Standard deviation multiplier

    Example Usage:
        bb_trend_now_to_20d = bb_width_20d_now / bb_width_20d_20d_ago
        bb_trend_20d_to_40d = bb_width_20d_20d_ago / bb_width_20d_40d_ago
        bb_trend_40d_to_60d = bb_width_20d_40d_ago / bb_width_20d_60d_ago
        bb_trend_60d_to_80d = bb_width_20d_60d_ago / bb_width_20d_80d_ago
        
    Returns:
        DataFrame with trend ratio where:
        > 1.0 = volatility expanding (recent > past)
        < 1.0 = volatility contracting (recent < past)

    """
    
    # Calculate BB widths for both periods using bb_width_offset
    df = bb_width_offset(df, offset=short_lookback, lookback=period, bb_period=bb_period, std=std)
    df = bb_width_offset(df, offset=long_lookback, lookback=period, bb_period=bb_period, std=std)
    
    # Get column names for the two width features
    recent_col = f'bb_width_pct_{bb_period}bb_{period}d_{short_lookback}d_ago'
    past_col = f'bb_width_pct_{bb_period}bb_{period}d_{long_lookback}d_ago'
    
    # Calculate trend ratio (recent / past)
    feature_name = f'bb_trend_{short_lookback}d_to_{long_lookback}d_{period}d_period'
    df[feature_name] = df[recent_col] / df[past_col]
    
    return df

def price_pct_in_lower_bound(
    df: pd.DataFrame,
    offset: int,
    lookback: int,
    bb_period: int
    ) -> pd.DataFrame:
    """ Calculates the percentage of time that price was trading above the lower band yet below the middle band
    """

    df = bb_bands(df, offset=0, lookback=lookback)

    upper_col = f"bb_upperband_0_offset_{bb_period}_lookback"
    lower_col = f"bb_lowerband_0_offset_{bb_period}_lookback"
    middle_col = f'bb_SMA_0_offset_{bb_period}_lookback'

    def count_occurances():
        """ For each day check and count where price is relative to the middle and lower band """
        # Calculate a mid price for the day (high + low) / 2
        avg_price = (df['high'] + df['low']) /2
        
        

        return 

    df['feature'] = df.shift(offset).rolling(lookback).apply(count_occurances, raw = False)

    return df

def price_pct_blw_lower():

    pass


def price_pct_in_upper_bound():



    pass

def price_pct_abv_upper():

    pass

