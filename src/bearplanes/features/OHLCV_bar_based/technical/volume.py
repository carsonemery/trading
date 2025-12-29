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

        >>> Could call 5-10 times, with something like 5,10,15,20,25,30,40,50,70,90,120
    """

    # Shift(1) ensures current day is NOT included in the rolling mean calculation
    df[f'volume_ratio_{lookback}_days_rolling'] = df['volume'] / df['volume'].shift(1).rolling(window=lookback).mean()

    return df

def volume_ratio_rolling_offset(
    df: pd.DataFrame,
    lookback: int,
    offset: int
    ) -> pd.DataFrame:
    """Calculate rolling volume ratios for historical offset periods.
    
    Provides historical context by calculating volume ratios at a specific 
    point in the past. This gives the model additional features about past 
    volume behavior patterns.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        lookback: Number of days to look back for the rolling mean calculation.
        offset: How many days in the past to calculate the ratio for.
            For example, offset=30 calculates the volume ratio as it was 30 days ago.
    
    Returns:
        DataFrame with an added column 'volume_ratio_{lookback}_lookback_{offset}_offset'
        containing the historical volume ratios.
    
    Example:
        >>> # Calculate what the 20-day volume ratio was 30 days ago
        >>> df = volume_ratio_rolling_offset(df, lookback=20, offset=30)
        >>> # This provides historical context as a feature
    """

    # Shift to offset day, then calculate ratio using previous lookback days
    df[f'volume_ratio_{lookback}_lookback_{offset}_offset'] = (
        df['volume'].shift(offset) / 
        df['volume'].shift(offset + 1).rolling(window=lookback).mean()
    )
    
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

    # Compare current volume to previous lookback days
    # Calculate what percentile current volume represents vs historical window
    def percentile_vs_history(series):
        if len(series) < 2:
            return np.nan
        current = series.iloc[-1]
        historical = series.iloc[:-1]
        return (historical < current).sum() / len(historical)
    
    df[f'volume_percentile_{lookback}_days_rolling'] = (
        df['volume'].rolling(window=lookback + 1).apply(percentile_vs_history, raw=False)
    )

    return df

def volume_percentiles_offset(
    df: pd.DataFrame,
    lookback: int,
    offset: int
    ) -> pd.DataFrame:
    """Calculate rolling volume percentile rank for historical offset periods.
    
    Provides historical context by calculating volume percentile ranks at a 
    specific point in the past. This gives the model additional features about 
    past volume behavior patterns.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        lookback: Number of days to include in the rolling window for 
            percentile calculation. Must be a positive integer.
        offset: How many days in the past to calculate the percentile for.
            For example, offset=30 calculates the volume percentile as it was 30 days ago.
    
    Returns:
        DataFrame with an added column 'volume_percentile_{lookback}_lookback_{offset}_offset' 
        containing the historical percentile ranks (values between 0 and 1).
    
    Example:
        >>> # Calculate what the 20-day volume percentile was 30 days ago
        >>> df = volume_percentiles_offset(df, lookback=20, offset=30)
        >>> # A value of 0.8 means volume was in 80th percentile 30 days ago
    """

    # Compare volume at offset to previous lookback days
    # Calculate what percentile that offset volume represented vs its historical window
    def percentile_vs_history(series):
        if len(series) < 2:
            return np.nan
        current = series.iloc[-1]
        historical = series.iloc[:-1]
        return (historical < current).sum() / len(historical)
    
    # Shift to offset day, then calculate percentile using previous lookback days
    df[f'volume_percentile_{lookback}_lookback_{offset}_offset'] = (
        df['volume'].shift(offset).rolling(window=lookback + 1).apply(percentile_vs_history, raw=False)
    )

    return df

def volume_trends(
    df: pd.DataFrame,
    short_lookback: int,
    long_lookback: int
    ) -> pd.DataFrame:

    """Calculate volume trend by comparing short-term to long-term average volume.
    
    Computes the ratio of short-term rolling average volume to long-term rolling 
    average volume. This helps identify whether volume is trending up or down.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        short_lookback: Number of days for the short-term average (e.g., 5, 10, 20).
        long_lookback: Number of days for the long-term average (e.g., 50, 100, 200).
            Must be greater than short_lookback for meaningful results.
    
    Returns:
        DataFrame with an added column 'volume_trend_direction_{short}_day_avg_over_{long}_day_avg'
        containing the ratio values where:
        - Ratio > 1: Volume trending UP (recent volume higher than historical average)
        - Ratio < 1: Volume trending DOWN (recent volume lower than historical average)
        - Ratio ≈ 1: Volume stable (no significant trend)
    
    Example:
        >>> df = volume_trends(df, short_lookback=10, long_lookback=50)
        >>> # A value of 1.3 means 10-day avg volume is 30% higher than 50-day avg
        >>> # A value of 0.7 means 10-day avg volume is 30% lower than 50-day avg
    """

    df[f'volume_trend_direction_{short_lookback}_day_avg_over_{long_lookback}_day_avg'] = df['volume'].rolling(window=short_lookback).mean() / df['volume'].rolling(window=long_lookback).mean() 

    return df

def volume_trends_offset(
    df: pd.DataFrame,
    short_lookback: int,
    long_lookback: int,
    offset: int
    ) -> pd.DataFrame:

    """Calculate historical volume trend by comparing short-term to long-term averages at an offset period.
    
    Provides historical context by calculating the volume trend ratio as it was
    at a specific point in the past. This gives the model additional features
    about past volume trend behavior.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        short_lookback: Number of days for the short-term average (e.g., 5, 10, 20).
        long_lookback: Number of days for the long-term average (e.g., 50, 100, 200).
            Must be greater than short_lookback for meaningful results.
        offset: How many days in the past to calculate the trend ratio for.
            For example, offset=30 calculates the trend ratio as it was 30 days ago.
    
    Returns:
        DataFrame with an added column 'volume_trend_{short}_day_avg_over_{long}_day_avg_{offset}_offset'
        containing the historical trend ratios where:
        - Ratio > 1: Volume was trending UP at that historical point
        - Ratio < 1: Volume was trending DOWN at that historical point
        - Ratio ≈ 1: Volume was stable at that historical point
    
    Example:
        >>> # Calculate what the 10-day vs 50-day volume trend was 30 days ago
        >>> df = volume_trends_offset(df, short_lookback=10, long_lookback=50, offset=30)
        >>> # This provides historical trend context as a feature
    """

    df[f'volume_trend_{short_lookback}_day_avg_over_{long_lookback}_day_avg_{offset}_offset'] = (
        df['volume'].shift(offset).rolling(window=short_lookback).mean() / 
        df['volume'].shift(offset).rolling(window=long_lookback).mean()
    )
    
    return df

def volume_offset_stats(
    df: pd.DataFrame,
    offset: int,
    lookback: int
    ) -> pd.DataFrame:
    """Calculate volume coefficient of variation for historical offset periods.
    
    Measures the relative volatility/stability of volume during a specific 
    historical period. The coefficient of variation (CV = std/mean) is a 
    normalized measure that allows comparison across different stocks and 
    time periods.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        offset: How many days in the past to start the measurement window.
            For example, offset=30 starts measuring from 30 days ago.
        lookback: Number of days to include in the measurement window.
            For example, lookback=20 measures a 20-day period starting at the offset.
    
    Returns:
        DataFrame with an added column 'volume_cv_{lookback}_days_{offset}_offset'
        containing the coefficient of variation values where:
        - Low CV (e.g., 0.1-0.3): Stable, consistent volume during that period
        - High CV (e.g., 0.8-1.5+): Erratic, volatile volume with large spikes
    
    Example:
        >>> # Measure volume stability from 30-50 days ago (20-day window starting 30 days back)
        >>> df = volume_offset_stats(df, offset=30, lookback=20)
        >>> # A CV of 0.2 means volume was relatively stable during days -50 to -30
        >>> # A CV of 1.5 means volume had large erratic spikes during that period
    """
    
    shifted_volume = df['volume'].shift(offset)
    rolling_mean = shifted_volume.rolling(lookback).mean()
    rolling_std = shifted_volume.rolling(lookback).std()
    
    df[f'volume_cv_{lookback}_days_{offset}_offset'] = rolling_std / rolling_mean
    
    return df

def volume_deviation_frequency_offset(
    df: pd.DataFrame,
    lookback: int,
    offset: int
    ) -> pd.DataFrame:
    """Count volume spike frequency at different standard deviation thresholds for historical periods.
    
    Calculates how many times volume exceeded 2, 3, 4, 5, and 6 standard deviations 
    above the mean during a historical lookback window. This helps characterize the 
    frequency and intensity of volume spikes in the past, which can be predictive of 
    future behavior patterns.
    
    Args:
        df: DataFrame containing a 'volume' column with trading volume data.
        lookback: Number of days to analyze for volume spike frequency.
            For example, lookback=20 analyzes a 20-day historical period.
        offset: How many days in the past to start the lookback window.
            For example, offset=30 with lookback=20 analyzes days -50 to -30.
    
    Returns:
        DataFrame with added columns for each threshold:
        - 'num_vol_deviations_above_2_threshold_{lookback}_lookback_{offset}_offset'
        - 'num_vol_deviations_above_3_threshold_{lookback}_lookback_{offset}_offset'
        - 'num_vol_deviations_above_4_threshold_{lookback}_lookback_{offset}_offset'
        - 'num_vol_deviations_above_5_threshold_{lookback}_lookback_{offset}_offset'
        - 'num_vol_deviations_above_6_threshold_{lookback}_lookback_{offset}_offset'
        
        Each column contains integer counts of days exceeding that threshold.
    
    Example:
        >>> # Count volume spikes from 30-50 days ago (20-day window, 30 days back)
        >>> df = volume_deviation_frequency_offset(df, lookback=20, offset=30)
        >>> # A value of 3 in the '2_threshold' column means there were 3 days 
        >>> # with volume >2 standard deviations above mean during that period
        
    Notes:
        - Z-scores are calculated within each rolling window (not globally)
        - Higher thresholds (5-6 SD) may have zero counts in many windows
        - Useful for detecting accumulation patterns vs news-driven volatility
    """
    # Helper function that returns a count of times standard deviation was above a threshold for a certain
    # lookback and offset 
    def count_deviation(vol_series, threshold):
        """Returns count of days where volume z-score exceeded threshold"""
        mean = vol_series.mean()
        std = vol_series.std()
        
        # Handle edge case where all volumes are identical
        if std == 0 or pd.isna(std):
            return 0
        
        z_scores = (vol_series - mean) / std
        return (z_scores > threshold).sum()
 
    for i in range (2, 7):
        df[f'num_vol_deviations_above_{i}_threshold_{lookback}_lookback_{offset}_offset'] = (
            df['volume'].shift(offset).rolling(lookback).apply(count_deviation, args=(i,), raw=False)
        )

    return df

def mean_return_on_volume_deviation_days(
    df: pd.DataFrame,
    lookback: int, 
    offset: int
    ) -> pd.DataFrame:
    """Calculate average intrabar returns on volume spike days at different intensity thresholds.
    
    Computes the mean open-to-close return for days where volume exceeded specific 
    standard deviation thresholds during a historical lookback period. This identifies 
    whether volume spikes were accompanied by positive price movement (breakout behavior) 
    or minimal movement (accumulation behavior).
    
    Args:
        df: DataFrame containing 'open', 'close', and 'volume' columns.
        lookback: Number of days to analyze for volume spike behavior.
            For example, lookback=20 analyzes a 20-day historical period.
        offset: How many days in the past to start the lookback window.
            For example, offset=30 with lookback=20 analyzes days -50 to -30.
    
    Returns:
        DataFrame with added columns for each threshold bucket:
        - 'mean_return_on_2_3_sd_vol_days_{lookback}_lookback_{offset}_offset'
        - 'mean_return_on_3_5_sd_vol_days_{lookback}_lookback_{offset}_offset'
        - 'mean_return_on_5_plus_sd_vol_days_{lookback}_lookback_{offset}_offset'
        
        Each column contains the mean return (as a decimal) on days exceeding 
        that volume threshold, or NaN if no days met the threshold.
    
    Example:
        >>> df = mean_return_on_volume_deviation_days(df, lookback=20, offset=30)
        >>> # A value of 0.002 in '2_3_sd' column means volume spike days (2-3 SD)
        >>> # had an average +0.2% intraday return during that historical period
        >>> # A value of -0.01 means those days averaged -1% (selling pressure)
        
    """
    # Calculate temporary intra-bar open to close return column
    df['_temp_open_to_close'] = (df['close'] - df['open']) / df['open']

    def calc_mean_return(window_data, low, high):
        """Helper function to calculate mean return on volume spike days"""
        volumes = window_data['volume']
        returns = window_data['_temp_open_to_close']
        
        # Calculate volume statistics for this window
        mean_vol = volumes.mean()
        std_vol = volumes.std()

        # Handle edge case: no volume variation means no spikes possible
        if std_vol == 0 or pd.isna(std_vol):
            return np.nan

        # Calculate z-scores for volume
        z_scores = (volumes - mean_vol) / std_vol

        # Identify days where volume fell within our threshold range
        target_observations = (z_scores >= low) & (z_scores < high)

        # Extract returns for those specific days
        spike_day_returns = returns[target_observations]

        # Return mean of those returns (NaN if no days matched)
        return spike_day_returns.mean()
    
    # Define volume spike intensity buckets
    buckets = {
        '2_3_sd': (2, 3),        # Moderate volume spikes
        '3_5_sd': (3, 5),        # Significant volume spikes
        '5_plus_sd': (5, np.inf) # Extreme volume events
    }

    for label, (low, high) in buckets.items():
        df[f'mean_return_on_{label}_vol_days_{lookback}_lookback_{offset}_offset'] = (
            df[['volume', '_temp_open_to_close']].shift(offset).rolling(lookback).apply(
                lambda window: calc_mean_return(window, low, high), raw=False
            )
        )

    # Clean up temporary column
    df.drop('_temp_open_to_close', axis=1, inplace=True)

    return df

def mean_relative_range_on_vol_deviation_days(
    df: pd.DataFrame,
    lookback: int,
    offset: int
    ) -> pd.DataFrame:
    """Calculate average price range on volume spike days at different intensity thresholds.
    
    Computes the mean intraday range (normalized by close price) for days where 
    volume exceeded specific standard deviation thresholds during a historical 
    lookback period. This identifies whether volume spikes were accompanied by 
    high volatility (wide ranges) or tight price action (narrow ranges).
    
    Args:
        df: DataFrame containing 'high', 'low', 'close', and 'volume' columns.
        lookback: Number of days to analyze for volume spike behavior.
            For example, lookback=20 analyzes a 20-day historical period.
        offset: How many days in the past to start the lookback window.
            For example, offset=30 with lookback=20 analyzes days -50 to -30.
    
    Returns:
        DataFrame with added columns for each threshold bucket:
        - 'mean_rel_range_on_2_3_sd_vol_days_{lookback}_lookback_{offset}_offset'
        - 'mean_rel_range_on_3_5_sd_vol_days_{lookback}_lookback_{offset}_offset'
        - 'mean_rel_range_on_5_plus_sd_vol_days_{lookback}_lookback_{offset}_offset'
        
        Each column contains the mean normalized range on days exceeding that 
        volume threshold, or NaN if no days met the threshold.
    
    Example:
        >>> df = mean_relative_range_on_vol_deviation_days(df, lookback=20, offset=30)
        >>> # A value of 0.02 in '2_3_sd' column means volume spike days (2-3 SD)
        >>> # had an average intraday range of 2% relative to close price
        >>> # A value of 0.08 suggests high volatility on those spike days
        

    """
    # Calculate temporary intraday range column (normalized by close)
    df['_temp_intraday_range'] = (df['high'] - df['low']) / df['close']

    def calc_mean_range(window_data, low, high):
        """Helper function to calculate mean range on volume spike days"""
        volumes = window_data['volume']
        ranges = window_data['_temp_intraday_range']
        
        # Calculate volume statistics for this window
        mean_vol = volumes.mean()
        std_vol = volumes.std()

        # Handle edge case: no volume variation means no spikes possible
        if std_vol == 0 or pd.isna(std_vol):
            return np.nan

        # Calculate z-scores for volume
        z_scores = (volumes - mean_vol) / std_vol

        # Identify days where volume fell within our threshold range
        target_observations = (z_scores >= low) & (z_scores < high)

        # Extract ranges for those specific days
        spike_day_ranges = ranges[target_observations]

        # Return mean of those ranges (NaN if no days matched)
        return spike_day_ranges.mean()
    
    # Define volume spike intensity buckets
    buckets = {
        '2_3_sd': (2, 3),        # Moderate volume spikes
        '3_5_sd': (3, 5),        # Significant volume spikes
        '5_plus_sd': (5, np.inf) # Extreme volume events
    }

    for label, (low, high) in buckets.items():
        df[f'mean_rel_range_on_{label}_vol_days_{lookback}_lookback_{offset}_offset'] = (
            df[['volume', '_temp_intraday_range']].shift(offset).rolling(lookback).apply(
                lambda window: calc_mean_range(window, low, high), raw=False
            )
        )

    # Clean up temporary column
    df.drop('_temp_intraday_range', axis=1, inplace=True)

    return df
