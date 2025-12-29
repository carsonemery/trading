import pandas as pd
import numpy as np

""" Using an offset in all functions within this atr file so will exclude from **most** method names
"""

def atr(
    df: pd.DataFrame,
    offset: int,
    lookback: int
    ) -> pd.DataFrame:
    """Calculate Average True Range (ATR) for historical periods.
    
    ATR measures volatility by calculating the average of true ranges over a 
    specified lookback period. True Range accounts for gaps by considering the 
    previous close, making it more comprehensive than simple high-low range.
    
    True Range is the maximum of:
    - Current High - Current Low
    - |Current High - Previous Close|
    - |Current Low - Previous Close|
    
    Args:
        df: DataFrame containing 'high', 'low', and 'close' columns.
        offset: How many days in the past to calculate the ATR for.
            For example, offset=14 calculates ATR as it was 14 days ago.
        lookback: Number of days to average true ranges over.
            Traditional ATR uses 14 days, but can be adjusted.
    
    Returns:
        DataFrame with added column 'rolling_ATR_{offset}_offset_{lookback}_lookback'
        containing the ATR values (in absolute price units).
    
    Examples:
        >>> # ATR for last 14 days ending today (offset=0)
        >>> df = atr(df, offset=0, lookback=14)
        
        >>> # ATR for 14 days ending 14 days ago [days -28 to -14]
        >>> df = atr(df, offset=14, lookback=14)
        
        >>> # ATR for 14 days ending 28 days ago [days -42 to -28]
        >>> df = atr(df, offset=28, lookback=14)
    
    Notes:
        - First row will be NaN (needs previous close for calculation)
    """
    # Calculate True Range (element-wise maximum of three conditions)
    df['_temp_true_range'] = np.maximum.reduce([
        df['high'] - df['low'],
        abs(df['high'] - df['close'].shift(1)),
        abs(df['low'] - df['close'].shift(1))
    ])
    
    # Calculate ATR at the specified offset period
    df[f"rolling_ATR_{offset}_offset_{lookback}_lookback"] = (
        df['_temp_true_range'].shift(offset).rolling(lookback).mean()
    )

    # Drop the temporary true_range column
    df.drop('_temp_true_range', axis=1, inplace=True)

    return df

def atr_pct_price(
    df: pd.DataFrame,
    offset: int, 
    lookback: int
    ) -> pd.DataFrame:
    """Calculate ATR as a percentage of price (Normalized ATR).
    
    Normalizes ATR by dividing it by the moving average price over the same 
    period. This allows comparison of volatility across stocks at different 
    price levels and identifies relative volatility changes over time.
    
    Args:
        df: DataFrame containing 'high', 'low', and 'close' columns.
        offset: How many days in the past to calculate the normalized ATR for.
            For example, offset=14 calculates it as it was 14 days ago.
        lookback: Number of days to average true ranges and close prices over.
            Both ATR and average close use the same window for consistency.
    
    Returns:
        DataFrame with added column 'atr_pct_price_{offset}_offset_{lookback}_lookback'
        containing the normalized ATR values (as decimals, e.g., 0.05 = 5%).
    
    Examples:
        >>> df = atr_pct_price(df, offset=0, lookback=14)
        >>> # A value of 0.02 means ATR is 2% of average price (low volatility)
        >>> # A value of 0.10 means ATR is 10% of average price (high volatility)
    
    """
    # Calculate True Range (element-wise maximum of three conditions)
    df['_temp_true_range'] = np.maximum.reduce([
        df['high'] - df['low'],
        abs(df['high'] - df['close'].shift(1)),
        abs(df['low'] - df['close'].shift(1))
    ])
    
    # Calculate ATR at the specified offset period
    atr_values = df['_temp_true_range'].shift(offset).rolling(lookback).mean()
    
    # Calculate average close price over the same period for normalization
    avg_close = df['close'].shift(offset).rolling(lookback).mean()
    
    # Calculate normalized ATR (ATR as percentage of average price)
    df[f'atr_pct_price_{offset}_offset_{lookback}_lookback'] = atr_values / avg_close

    # Drop the temporary true_range column
    df.drop('_temp_true_range', axis=1, inplace=True)

    return df

def atr_compression_expansion(
    df: pd.DataFrame,
    offset: int,
    lookback: int
    ) -> pd.DataFrame:
    """Calculate ATR compression/expansion ratio between current and historical periods.
    
    Measures whether volatility (ATR) is expanding or compressing by comparing 
    the current period's ATR to a historical period's ATR. Values above 1 indicate 
    expanding volatility (breakout behavior), while values below 1 indicate 
    compressing volatility (consolidation/accumulation).
    
    Args:
        df: DataFrame containing 'high', 'low', and 'close' columns.
        offset: How many days back to compare against.
            For example, offset=14 compares current ATR to ATR from 14 days ago.
        lookback: Number of days to calculate ATR over for both periods.
            For example, lookback=14 uses 14-day ATR for both current and historical.
    
    Returns:
        DataFrame with added column 'atr_period_to_period_ratio_{offset}_offset_{lookback}_lookback'
        containing the compression/expansion ratio.
    
    Examples:
        >>> df = atr_compression_expansion(df, offset=14, lookback=14)
        >>> # Ratio of 1.5 = ATR is 50% higher now than 14 days ago (expanding)
        >>> # Ratio of 0.6 = ATR is 40% lower now than 14 days ago (compressing)
        >>> # Ratio of 1.0 = ATR unchanged (stable volatility)

    """
    # Calculate True Range (element-wise maximum of three conditions)
    df['_temp_true_range'] = np.maximum.reduce([
        df['high'] - df['low'],
        abs(df['high'] - df['close'].shift(1)),
        abs(df['low'] - df['close'].shift(1))
    ])
    
    # Calculate current ATR (last 'lookback' days ending today)
    atr_now = df['_temp_true_range'].rolling(lookback).mean()

    # Calculate historical ATR ('lookback' days ending 'offset' days ago)
    atr_in_offset = df['_temp_true_range'].shift(offset).rolling(lookback).mean()

    # Calculate compression/expansion ratio
    df[f"atr_period_to_period_ratio_{offset}_offset_{lookback}_lookback"] = atr_now / atr_in_offset

    # Drop the temporary true_range column
    df.drop('_temp_true_range', axis=1, inplace=True)

    return df

def atr_trend_sequential_offsets(
    df: pd.DataFrame,
    offset: int,
    lookback: int,
    num_of_samples: int
    ) -> pd.DataFrame:
    """Calculate sequential ATR trend ratios over multiple historical periods.
    
    Creates multiple features showing how ATR evolved from period to period 
    going backwards in time. This helps identify sustained volatility trends 
    (e.g., continuous compression during accumulation, or sequential expansion 
    during a breakout).
    
    Args:
        df: DataFrame containing 'high', 'low', and 'close' columns.
        offset: The time step between each period comparison (in days).
            For example, offset=14 compares 14-day periods to each other.
        lookback: Number of days to calculate ATR over for each period.
            Typically matches offset for non-overlapping periods.
        num_of_samples: How many sequential period comparisons to create.
            For example, num_of_samples=5 creates 5 trend ratio features.
    
    Returns:
        DataFrame with added columns (one per sample):
        - 'atr_ratio_trend_0d_to_{offset}d'
        - 'atr_ratio_trend_{offset}d_to_{2*offset}d'
        - 'atr_ratio_trend_{2*offset}d_to_{3*offset}d'
        - ... (continues for num_of_samples)
        
        Each column contains the ratio of adjacent period ATRs.
    
    Examples:
        >>> # Create 5 sequential 14-day ATR trend ratios
        >>> df = atr_trend_sequential_offsets(df, offset=14, lookback=14, num_of_samples=5)
        >>> # This creates:
        >>> # - atr_ratio_trend_0d_to_14d    = ATR(now) / ATR(14d ago)
        >>> # - atr_ratio_trend_14d_to_28d   = ATR(14d ago) / ATR(28d ago)
        >>> # - atr_ratio_trend_28d_to_42d   = ATR(28d ago) / ATR(42d ago)
        >>> # - atr_ratio_trend_42d_to_56d   = ATR(42d ago) / ATR(56d ago)
        >>> # - atr_ratio_trend_56d_to_70d   = ATR(56d ago) / ATR(70d ago)
    
    """
    # Calculate True Range (element-wise maximum of three conditions)
    df['_temp_true_range'] = np.maximum.reduce([
        df['high'] - df['low'],
        abs(df['high'] - df['close'].shift(1)),
        abs(df['low'] - df['close'].shift(1))
    ])

    for i in range(0, num_of_samples):
        # More recent period
        leading_offset = offset * i
        atr_leading = df['_temp_true_range'].shift(leading_offset).rolling(lookback).mean()

        # Older period (one step further back)
        lagging_offset = offset * (i + 1)
        atr_lagging = df['_temp_true_range'].shift(lagging_offset).rolling(lookback).mean()

        # Calculate ratio: more recent / older (shows if expanding or compressing)
        df[f"atr_ratio_trend_{leading_offset}d_to_{lagging_offset}d"] = atr_leading / atr_lagging

    # Drop the temporary true_range column
    df.drop('_temp_true_range', axis=1, inplace=True)

    return df
