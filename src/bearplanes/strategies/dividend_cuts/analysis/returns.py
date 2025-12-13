import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv
import os
import numpy as np
from scipy.stats import pearsonr



def plot_monte_carlo_paths(df_merged: pd.DataFrame, window_days: int, 
                          output_dir: Path = None) -> None:
    """
    Create Monte Carlo style visualization of stock price paths after dividend cuts.
    
    Approach:
    1. Identify all flagged events (is_reduction_50pct == True)
    2. For each event, extract price path from day 0 to day +window_days
    3. Normalize all paths to 100 on the flag date (day 0)
    4. Plot each stock as a thin semi-transparent line
    5. Add bold line showing the average path across all stocks
    6. Add shaded region for +/- 1 std deviation
    
    Parameters:
    ----------
    df_merged : pd.DataFrame
        Merged CRSP data with dividend flags (output from merge.py)
    window_days : int
        Number of days after flag to show (default 60)
    output_dir : Path
        Directory to save the plot (if None, just display)
        
    Returns:
    -------
    None (saves/displays plot)
    
    Implementation Notes:
    - Group by (permno, declare_date) to identify unique events
    - For each event, get subsequent rows up to +window_days
    - Handle missing data (some stocks may have gaps or delist)
    - Use adj_close for price to handle splits/dividends properly
    """
    print(f"Creating Monte Carlo price paths visualization...")
    
    # Set font to Garamond for all text
    plt.rcParams['font.family'] = 'Garamond'
    
    # Get flagged events
    flagged = df_merged[df_merged['is_reduction_50pct'] == True].copy()
    print(f"  Found {len(flagged)} flagged event dates")
    
    # Get unique events (permno, declare_date pairs)
    events = flagged[['permno', 'declare_date', 'dlycaldt']].drop_duplicates()
    print(f"  Unique events: {len(events)}")
    
    # Store all normalized paths
    all_paths = []
    
    # For each event, extract the price path
    for idx, event in events.iterrows():
        permno = event['permno']
        flag_date = event['dlycaldt']  # This is the trading date when flag appears
        
        # Get all data for this permno
        stock_data = df_merged[df_merged['permno'] == permno].sort_values('dlycaldt')
        
        # Find the flag date and get subsequent data
        flag_idx = stock_data[stock_data['dlycaldt'] == flag_date].index
        if len(flag_idx) == 0:
            continue
            
        flag_idx = flag_idx[0]
        flag_loc = stock_data.index.get_loc(flag_idx)
        
        # Get window_days after flag (including flag date)
        window_data = stock_data.iloc[flag_loc:flag_loc + window_days + 1]
        
        if len(window_data) < 2:  # Need at least 2 points
            continue
            
        # Normalize to 100 on flag date
        prices = window_data['adj_close'].values
        flag_price = prices[0]
        
        if flag_price <= 0 or pd.isna(flag_price):
            continue
            
        normalized = (prices / flag_price) * 100
        
        # Create day index (0, 1, 2, ...)
        days = list(range(len(normalized)))
        
        all_paths.append({
            'permno': permno,
            'declare_date': event['declare_date'],
            'days': days,
            'prices': normalized
        })
    
    print(f"  Successfully extracted {len(all_paths)} valid paths")
    
    if len(all_paths) == 0:
        print("  ERROR: No valid paths to plot!")
        return
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Calculate average path using numpy for reliability
    max_days = max(len(p['days']) for p in all_paths)
    
    # Create matrix: rows = stocks, columns = days
    price_matrix = np.full((len(all_paths), max_days), np.nan)
    
    for i, path in enumerate(all_paths):
        for j, (day, price) in enumerate(zip(path['days'], path['prices'])):
            if j < max_days:
                price_matrix[i, j] = price
    
    # Calculate mean across stocks for each day (ignoring NaN)
    mean_path = np.nanmean(price_matrix, axis=0)
    days_range = np.arange(max_days)
    
    print(f"  Average path calculated for {len(mean_path)} days")
    print(f"  Mean values: Day 0={mean_path[0]:.2f}, Day 30={mean_path[30]:.2f}, Day {len(mean_path)-1}={mean_path[-1]:.2f}")
    print(f"  Number of stocks at Day 0: {np.sum(~np.isnan(price_matrix[:, 0]))}")
    print(f"  Number of stocks at Day 30: {np.sum(~np.isnan(price_matrix[:, 30]))}")
    print(f"  Number of stocks at Day {max_days-1}: {np.sum(~np.isnan(price_matrix[:, -1]))}")
    
    # Plot individual paths FIRST (darker, more visible)
    for path in all_paths:
        ax.plot(path['days'], path['prices'], 
               color='steelblue', alpha=0.25, linewidth=0.7, zorder=1)
    
    # Plot mean path LAST (on top, thick and bright red)
    ax.plot(days_range, mean_path, 
           color='red', linewidth=4, label='Average Path', zorder=10, solid_capstyle='round')
    
    # Formatting
    ax.axhline(y=100, color='black', linestyle='--', linewidth=1.5, alpha=0.7, 
              label='Flag Date (Day 0)', zorder=5)
    ax.set_xlabel('Days After Dividend Cut Flag', fontsize=12, fontname='Garamond')
    ax.set_ylabel('Normalized Price (Day 0 = 100)', fontsize=12, fontname='Garamond')
    ax.set_title(f'Stock Price Paths After 50% Dividend Cuts\n({len(all_paths)} events)', 
                fontsize=14, fontweight='bold', fontname='Garamond')
    ax.legend(loc='best', fontsize=11, framealpha=0.9, prop={'family': 'Garamond'})
    ax.grid(True, alpha=0.3)
    
    # Set tick labels to Garamond
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontname('Garamond')
    
    plt.tight_layout()
    
    # Save or display
    if output_dir:
        output_file = output_dir / 'monte_carlo_paths.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"  Saved plot to: {output_file}")
    else:
        plt.show()
    
    plt.close()

def analyze_return_distribution(df_merged: pd.DataFrame, 
                                forward_days: list = [15, 30, 60],
                                output_dir: Path = None) -> pd.DataFrame:
    """
    Calculate and visualize distribution of forward returns after dividend cuts.
    
    Approach:
    1. For each flagged event, calculate forward returns at 15, 30, 60 days
    2. Forward return = (price_t+N / price_t) - 1
    3. Create histogram for each time horizon
    4. Display summary statistics (mean, median, std, percentiles)
    5. Calculate win rate (% of positive returns)
    
    Parameters:
    ----------
    df_merged : pd.DataFrame
        Merged CRSP data with dividend flags
    forward_days : list
        List of forward-looking windows to analyze (default [15, 30, 60])
    output_dir : Path
        Directory to save plots (if None, just display)
        
    Returns:
    -------
    pd.DataFrame
        Summary statistics for each time horizon
        Columns: ['horizon', 'count', 'mean', 'median', 'std', 'min', 'max', 
                  'pct_10', 'pct_25', 'pct_75', 'pct_90', 'win_rate']
    
    Implementation Notes:
    - Use adj_close to avoid split/dividend distortions
    - Handle cases where stock delists before forward window ends
    - Create subplot with 3 histograms (one per time horizon)
    - Add vertical lines for mean and median on each histogram
    """
    print(f"\nCalculating forward returns distribution...")
    
    # Set font to Garamond
    plt.rcParams['font.family'] = 'Garamond'
    
    # Get flagged events
    flagged = df_merged[df_merged['is_reduction_50pct'] == True].copy()
    events = flagged[['permno', 'declare_date', 'dlycaldt']].drop_duplicates()
    print(f"  Analyzing {len(events)} flagged events")
    
    # Store returns for each horizon
    returns_by_horizon = {days: [] for days in forward_days}
    
    # Calculate forward returns for each event
    for idx, event in events.iterrows():
        permno = event['permno']
        flag_date = event['dlycaldt']
        
        # Get all data for this permno
        stock_data = df_merged[df_merged['permno'] == permno].sort_values('dlycaldt')
        
        # Find the flag date
        flag_idx = stock_data[stock_data['dlycaldt'] == flag_date].index
        if len(flag_idx) == 0:
            continue
            
        flag_idx = flag_idx[0]
        flag_loc = stock_data.index.get_loc(flag_idx)
        flag_price = stock_data.iloc[flag_loc]['adj_close']
        
        if pd.isna(flag_price) or flag_price <= 0:
            continue
        
        # Calculate returns at each horizon
        for days in forward_days:
            future_loc = flag_loc + days
            if future_loc < len(stock_data):
                future_price = stock_data.iloc[future_loc]['adj_close']
                if not pd.isna(future_price) and future_price > 0:
                    forward_return = (future_price / flag_price) - 1
                    returns_by_horizon[days].append(forward_return)
    
    # Calculate summary statistics
    summary_data = []
    for days in forward_days:
        returns = np.array(returns_by_horizon[days])
        if len(returns) > 0:
            summary_data.append({
                'horizon': f'{days}d',
                'count': len(returns),
                'mean': returns.mean(),
                'median': np.median(returns),
                'std': returns.std(),
                'min': returns.min(),
                'max': returns.max(),
                'pct_10': np.percentile(returns, 10),
                'pct_25': np.percentile(returns, 25),
                'pct_75': np.percentile(returns, 75),
                'pct_90': np.percentile(returns, 90),
                'win_rate': (returns > 0).sum() / len(returns)
            })
    
    summary_df = pd.DataFrame(summary_data)
    
    print(f"\nSummary Statistics:")
    print(summary_df.to_string(index=False))
    
    # Create visualization
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    
    for i, days in enumerate(forward_days):
        ax = axes[i]
        returns = np.array(returns_by_horizon[days]) * 100  # Convert to percentage
        
        if len(returns) == 0:
            continue
        
        # Create histogram
        n, bins, patches = ax.hist(returns, bins=40, color='steelblue', 
                                   alpha=0.7, edgecolor='black', linewidth=0.5)
        
        # Add vertical lines for mean and median
        mean_val = returns.mean()
        median_val = np.median(returns)
        
        ax.axvline(mean_val, color='red', linestyle='--', linewidth=2.5, 
                  label=f'Mean: {mean_val:.1f}%', zorder=10)
        ax.axvline(median_val, color='darkgreen', linestyle='--', linewidth=2.5, 
                  label=f'Median: {median_val:.1f}%', zorder=10)
        ax.axvline(0, color='black', linestyle='-', linewidth=1.5, alpha=0.5, zorder=5)
        
        # Labels and formatting
        ax.set_xlabel('Forward Return (%)', fontsize=11, fontname='Garamond')
        ax.set_ylabel('Frequency', fontsize=11, fontname='Garamond')
        ax.set_title(f'{days}-Day Forward Returns\n(n={len(returns)})', 
                    fontsize=12, fontweight='bold', fontname='Garamond')
        ax.legend(loc='best', fontsize=9, prop={'family': 'Garamond'}, framealpha=0.9)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Set tick labels to Garamond
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_fontname('Garamond')
    
    plt.suptitle('Distribution of Forward Returns After 50% Dividend Cuts', 
                fontsize=14, fontweight='bold', fontname='Garamond', y=1.02)
    plt.tight_layout()
    
    # Save or display
    if output_dir:
        output_file = output_dir / 'return_distribution.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\n  Saved plot to: {output_file}")
    else:
        plt.show()
    
    plt.close()
    
    return summary_df

def backtest_short_strategy(df_merged: pd.DataFrame, 
                            hold_days: int = 30,
                            output_dir: Path = None) -> dict:
    """
    Backtest a simple short strategy: short on flag date, cover after hold_days.
    
    Strategy Rules:
    1. On flag date (is_reduction_50pct == True), short the stock at adj_close
    2. Hold for hold_days trading days
    3. Cover at adj_close after hold_days
    4. Return per trade = -(price_exit / price_entry - 1)
    5. Equal weight all trades (no position sizing)
    6. Track cumulative P&L over time
    
    Parameters:
    ----------
    df_merged : pd.DataFrame
        Merged CRSP data with dividend flags
    hold_days : int
        Number of days to hold short position (default 30)
    output_dir : Path
        Directory to save plots (if None, just display)
        
    Returns:
    -------
    dict
        Performance metrics:
        {
            'total_return': float,           # Cumulative return
            'num_trades': int,                # Number of trades taken
            'win_rate': float,                # % of profitable shorts
            'avg_return': float,              # Mean return per trade
            'median_return': float,           # Median return per trade
            'sharpe_ratio': float,            # Annualized Sharpe (252 days)
            'max_drawdown': float,            # Max peak-to-trough decline
            'trades': pd.DataFrame            # Detailed trade log
        }
    
    Visualizations:
    - Cumulative return over time (line chart)
    - Underwater chart (drawdown visualization)
    - Distribution of trade returns (histogram)
    
    Implementation Notes:
    - Group by unique events (permno, declare_date)
    - For each event, find entry and exit prices
    - Calculate individual trade returns
    - Aggregate into cumulative equity curve
    - Handle overlapping positions (multiple shorts active simultaneously)
    """
    pass

def analyze_52week_positioning(df_merged: pd.DataFrame,
                               lookback_days: int = 252,
                               output_dir: Path = None) -> pd.DataFrame:
    """
    Analyze where stocks were trading relative to 52-week range on flag date.
    
    Approach:
    1. For each flagged event, look back lookback_days (default 252 = ~1 year)
    2. Find high and low over that window
    3. Calculate position: (price - low) / (high - low)
    4. 0.0 = at 52-week low, 1.0 = at 52-week high, 0.5 = midpoint
    5. Create histogram of positioning across all events
    6. Test hypothesis: were cuts occurring when stocks already beaten down?
    
    Parameters:
    ----------
    df_merged : pd.DataFrame
        Merged CRSP data with dividend flags
    lookback_days : int
        Number of days to look back for high/low (default 252)
    output_dir : Path
        Directory to save plots (if None, just display)
        
    Returns:
    -------
    pd.DataFrame
        One row per flagged event with columns:
        ['permno', 'declare_date', 'ticker', 'price', 'high_252', 'low_252',
         'position_in_range', 'reduction_pct']
    
    Visualizations:
    - Histogram of position_in_range values
    - Summary stats (mean, median positioning)
    - Potentially: forward returns by quartile of positioning
    
    Implementation Notes:
    - Use adj_close to handle splits properly
    - Need at least lookback_days of history prior to flag
    - Some events may not have full history (recent IPOs)
    """
    print(f"\nAnalyzing 52-week positioning on dividend cut dates...")
    
    # Set font to Garamond
    plt.rcParams['font.family'] = 'Garamond'
    
    # Get flagged events
    flagged = df_merged[df_merged['is_reduction_50pct'] == True].copy()
    events = flagged[['permno', 'declare_date', 'dlycaldt', 'ticker', 'reduction_pct']].drop_duplicates()
    print(f"  Analyzing {len(events)} flagged events")
    
    # Store positioning data
    positioning_data = []
    
    # Calculate positioning for each event
    for idx, event in events.iterrows():
        permno = event['permno']
        flag_date = event['dlycaldt']
        
        # Get all data for this permno
        stock_data = df_merged[df_merged['permno'] == permno].sort_values('dlycaldt')
        
        # Find the flag date
        flag_idx = stock_data[stock_data['dlycaldt'] == flag_date].index
        if len(flag_idx) == 0:
            continue
            
        flag_idx = flag_idx[0]
        flag_loc = stock_data.index.get_loc(flag_idx)
        
        # Need at least lookback_days of history before flag
        if flag_loc < lookback_days:
            continue
        
        # Get lookback window (not including flag date)
        lookback_data = stock_data.iloc[flag_loc - lookback_days:flag_loc]
        
        if len(lookback_data) < lookback_days * 0.8:  # Need at least 80% of data
            continue
        
        # Calculate high and low over lookback period
        high_252 = lookback_data['adj_close'].max()
        low_252 = lookback_data['adj_close'].min()
        current_price = stock_data.iloc[flag_loc]['adj_close']
        
        if pd.isna(high_252) or pd.isna(low_252) or pd.isna(current_price):
            continue
        
        if high_252 <= low_252:  # Sanity check
            continue
        
        # Calculate position in range: 0 = at low, 1 = at high
        position_in_range = (current_price - low_252) / (high_252 - low_252)
        
        positioning_data.append({
            'permno': permno,
            'declare_date': event['declare_date'],
            'ticker': event['ticker'],
            'price': current_price,
            'high_252': high_252,
            'low_252': low_252,
            'position_in_range': position_in_range,
            'reduction_pct': event['reduction_pct']
        })
    
    # Create DataFrame
    result_df = pd.DataFrame(positioning_data)
    print(f"  Successfully calculated positioning for {len(result_df)} events")
    
    if len(result_df) == 0:
        print("  ERROR: No events with sufficient history!")
        return result_df
    
    # Calculate summary statistics
    mean_pos = result_df['position_in_range'].mean()
    median_pos = result_df['position_in_range'].median()
    
    print(f"\n  Mean positioning: {mean_pos:.3f} (0=low, 1=high)")
    print(f"  Median positioning: {median_pos:.3f}")
    print(f"  % Near lows (<0.25): {(result_df['position_in_range'] < 0.25).sum() / len(result_df) * 100:.1f}%")
    print(f"  % Near highs (>0.75): {(result_df['position_in_range'] > 0.75).sum() / len(result_df) * 100:.1f}%")
    
    # Create visualization
    fig, ax = plt.subplots(figsize=(10, 7))
    
    # Histogram
    n, bins, patches = ax.hist(result_df['position_in_range'], bins=30, 
                              color='steelblue', alpha=0.7, edgecolor='black', linewidth=0.8)
    
    # Add vertical lines for mean and median
    ax.axvline(mean_pos, color='red', linestyle='--', linewidth=2.5, 
              label=f'Mean: {mean_pos:.3f}', zorder=10)
    ax.axvline(median_pos, color='darkgreen', linestyle='--', linewidth=2.5, 
              label=f'Median: {median_pos:.3f}', zorder=10)
    
    # Add reference lines for quartiles
    ax.axvline(0.25, color='gray', linestyle=':', linewidth=1.5, alpha=0.5, 
              label='25th / 75th percentile')
    ax.axvline(0.75, color='gray', linestyle=':', linewidth=1.5, alpha=0.5)
    ax.axvline(0.5, color='black', linestyle='-', linewidth=1, alpha=0.3)
    
    # Shading for interpretation zones
    ax.axvspan(0, 0.25, alpha=0.1, color='red', label='Near 52-week low')
    ax.axvspan(0.75, 1.0, alpha=0.1, color='green', label='Near 52-week high')
    
    # Labels and formatting
    ax.set_xlabel('Position in 52-Week Range (0=Low, 1=High)', fontsize=12, fontname='Garamond')
    ax.set_ylabel('Frequency', fontsize=12, fontname='Garamond')
    ax.set_title(f'Stock Positioning at Time of Dividend Cut\n(n={len(result_df)} events)', 
                fontsize=14, fontweight='bold', fontname='Garamond')
    ax.legend(loc='best', fontsize=9, prop={'family': 'Garamond'}, framealpha=0.9)
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_xlim(0, 1)
    
    # Set tick labels to Garamond
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontname('Garamond')
    
    # Add interpretation text
    if mean_pos < 0.4:
        interpretation = "Stocks were near 52-week lows\nwhen cutting dividends"
    elif mean_pos > 0.6:
        interpretation = "Stocks were near 52-week highs\nwhen cutting dividends"
    else:
        interpretation = "Stocks were mid-range\nwhen cutting dividends"
    
    textstr = f'{interpretation}\n\nMean: {mean_pos:.3f}\nMedian: {median_pos:.3f}'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
    ax.text(0.98, 0.97, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', horizontalalignment='right', 
            bbox=props, fontname='Garamond')
    
    plt.tight_layout()
    
    # Save or display
    if output_dir:
        output_file = output_dir / '52week_positioning.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\n  Saved plot to: {output_file}")
    else:
        plt.show()
    
    plt.close()
    
    return result_df


def plot_cut_severity_vs_returns(df_merged: pd.DataFrame,
                                 forward_days: int = 30,
                                 output_dir: Path = None) -> pd.DataFrame:
    """
    Scatter plot: dividend cut severity vs forward returns.
    Tests hypothesis: do bigger cuts lead to worse stock performance?
    
    Approach:
    1. For each flagged event, extract reduction_pct (e.g., -50%, -75%)
    2. Calculate forward return at forward_days (default 30)
    3. Create scatter plot with reduction_pct on x-axis, return on y-axis
    4. Add regression line with R² value
    5. Calculate correlation coefficient
    
    Parameters:
    ----------
    df_merged : pd.DataFrame
        Merged CRSP data with dividend flags
    forward_days : int
        Forward window for return calculation (default 30)
    output_dir : Path
        Directory to save plot (if None, just display)
        
    Returns:
    -------
    pd.DataFrame
        One row per event with columns:
        ['permno', 'declare_date', 'ticker', 'reduction_pct', 
         'forward_return', 'price_before', 'price_after']
    
    Visualizations:
    - Scatter plot with each dot = 1 event
    - Regression line overlaid
    - Correlation coefficient and p-value annotated
    
    Statistical Analysis:
    - Pearson correlation: reduction_pct vs forward_return
    - Test significance of relationship
    - Show if bigger cuts predict worse outcomes
    
    Implementation Notes:
    - reduction_pct is already negative (e.g., -0.50 for 50% cut)
    - Some stocks may not survive forward_days (delisting)
    - Consider coloring points by year or market cap
    """
    print(f"\nAnalyzing cut severity vs forward returns...")
    
    # Set font to Garamond
    plt.rcParams['font.family'] = 'Garamond'
    
    # Get flagged events
    flagged = df_merged[df_merged['is_reduction_50pct'] == True].copy()
    events = flagged[['permno', 'declare_date', 'dlycaldt', 'ticker', 'reduction_pct']].drop_duplicates()
    print(f"  Analyzing {len(events)} flagged events")
    
    # Store data for each event
    event_data = []
    
    # Calculate forward returns for each event
    for idx, event in events.iterrows():
        permno = event['permno']
        flag_date = event['dlycaldt']
        reduction_pct = event['reduction_pct']
        
        if pd.isna(reduction_pct):
            continue
        
        # Get all data for this permno
        stock_data = df_merged[df_merged['permno'] == permno].sort_values('dlycaldt')
        
        # Find the flag date
        flag_idx = stock_data[stock_data['dlycaldt'] == flag_date].index
        if len(flag_idx) == 0:
            continue
            
        flag_idx = flag_idx[0]
        flag_loc = stock_data.index.get_loc(flag_idx)
        flag_price = stock_data.iloc[flag_loc]['adj_close']
        
        if pd.isna(flag_price) or flag_price <= 0:
            continue
        
        # Calculate forward return
        future_loc = flag_loc + forward_days
        if future_loc < len(stock_data):
            future_price = stock_data.iloc[future_loc]['adj_close']
            if not pd.isna(future_price) and future_price > 0:
                forward_return = (future_price / flag_price) - 1
                
                event_data.append({
                    'permno': permno,
                    'declare_date': event['declare_date'],
                    'ticker': event['ticker'],
                    'reduction_pct': reduction_pct,
                    'forward_return': forward_return,
                    'price_before': flag_price,
                    'price_after': future_price
                })
    
    # Create DataFrame
    result_df = pd.DataFrame(event_data)
    print(f"  Successfully matched {len(result_df)} events with forward returns")
    
    if len(result_df) < 2:
        print("  ERROR: Not enough data points for analysis!")
        return result_df
    
    # Calculate correlation
    correlation, p_value = pearsonr(result_df['reduction_pct'], result_df['forward_return'])
    
    print(f"\n  Correlation: {correlation:.3f}")
    print(f"  P-value: {p_value:.4f}")
    print(f"  Significant: {'Yes' if p_value < 0.05 else 'No'}")
    
    # Create scatter plot
    fig, ax = plt.subplots(figsize=(10, 7))
    
    # reduction_pct is ALREADY in percentage form (e.g., -67.6 means -67.6%)
    # forward_return is in decimal form (e.g., 0.15 means 15%), so multiply by 100
    x = result_df['reduction_pct'].values  # Already percentages, don't multiply!
    y = result_df['forward_return'].values * 100  # Convert decimals to percentages
    
    # Scatter plot
    ax.scatter(x, y, alpha=0.6, s=50, color='steelblue', edgecolors='black', linewidth=0.5)
    
    # Add regression line
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    x_line = np.linspace(x.min(), x.max(), 100)
    ax.plot(x_line, p(x_line), color='red', linewidth=2.5, label='Regression Line', zorder=10)
    
    # Calculate R-squared
    y_pred = p(x)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r_squared = 1 - (ss_res / ss_tot)
    
    # Add reference lines
    ax.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    ax.axvline(x=-50, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    
    # Add text annotation with statistics
    textstr = f'Correlation: {correlation:.3f}\nR²: {r_squared:.3f}\nP-value: {p_value:.4f}\nn = {len(result_df)}'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
    ax.text(0.05, 0.95, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props, fontname='Garamond')
    
    # Labels and formatting
    ax.set_xlabel('Dividend Cut Severity (%)', fontsize=12, fontname='Garamond')
    ax.set_ylabel(f'{forward_days}-Day Forward Return (%)', fontsize=12, fontname='Garamond')
    ax.set_title(f'Dividend Cut Severity vs Stock Performance\n({forward_days}-day forward returns)', 
                fontsize=14, fontweight='bold', fontname='Garamond')
    ax.legend(loc='best', fontsize=10, prop={'family': 'Garamond'}, framealpha=0.9)
    ax.grid(True, alpha=0.3)
    
    # Set tick labels to Garamond
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontname('Garamond')
    
    plt.tight_layout()
    
    # Save or display
    if output_dir:
        output_file = output_dir / 'cut_severity_vs_returns.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\n  Saved plot to: {output_file}")
    else:
        plt.show()
    
    plt.close()
    
    return result_df


def main():
    """
    Main function to run all analyses and generate outputs.
    
    Loads merged CRSP data with dividend flags and runs all analysis functions.
    Saves all outputs to Strategies/dividend_cuts/analysis/output/
    """
    load_dotenv()
    
    # Setup paths
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent.parent / "Data"
    if os.getenv("DATA_DIR"):
        data_dir = Path(os.getenv("DATA_DIR"))
    
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Load merged data
    input_file = data_dir / "crsp" / "crsp_with_dividend_flags.parquet"
    print(f"Loading merged data from: {input_file}")
    df_merged = pd.read_parquet(input_file)
    print(f"Loaded {len(df_merged):,} rows")
    print(f"Flagged events: {df_merged['is_reduction_50pct'].sum():,}")
    
    # print("\n" + "="*80)
    # print("ANALYSIS 1: Monte Carlo Price Paths")
    # print("="*80)
    # plot_monte_carlo_paths(df_merged, window_days=60, output_dir=output_dir)
    
    # print("\n" + "="*80)
    # print("ANALYSIS 2: Return Distribution")
    # print("="*80)
    # summary_stats = analyze_return_distribution(df_merged, output_dir=output_dir)
    
    # print("\n" + "="*80)
    # print("ANALYSIS 3: Short Strategy Backtest")
    # print("="*80)
    # # TODO: backtest_short_strategy(df_merged, hold_days=30, output_dir=output_dir)
    
    print("\n" + "="*80)
    print("ANALYSIS 4: 52-Week Positioning")
    print("="*80)
    positioning_data = analyze_52week_positioning(df_merged, output_dir=output_dir)
    
    # print("\n" + "="*80)
    # print("ANALYSIS 5: Cut Severity vs Returns")
    # print("="*80)
    # severity_data = plot_cut_severity_vs_returns(df_merged, output_dir=output_dir)
    
    print("\n" + "="*80)
    print("ALL ANALYSES COMPLETE")
    print(f"Outputs saved to: {output_dir}")
    print("="*80)


if __name__ == "__main__":
    main()
