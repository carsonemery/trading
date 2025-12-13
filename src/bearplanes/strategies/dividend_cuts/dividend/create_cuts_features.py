import os
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv


## Creates dividend cut flags ##
## Also various diagnostic / analysis functions ##


def prepare_dividend_data(
    df: pd.DataFrame,
    df_crsp: pd.DataFrame) -> pd.DataFrame:
    """
    1. Filter to ordinary cash dividends (Y + CD only)
    2. Convert columns to datetime
    3. Sort by permno, ex_date (payment sequence, not announcement sequence)
    4. Adjust the dividend_amt by for splits and other share based events using the dlycumfacshr from crsp data
    Returns cleaned and sorted dataframe
    """
    print(f"Original records: {len(df):,}")
    print(f"Original permnos: {df['permno'].nunique():,}")
    
    # Filter to ordinary cash dividends only
    df_clean = df[
        (df['is_ordinary_div'] == 'Y') & 
        (df['dist_type'] == 'CD')
    ].copy()

    # Convert date columns to datetime
    df_clean['declare_date'] = pd.to_datetime(df_clean['declare_date'])
    df_clean['record_date'] = pd.to_datetime(df_clean['record_date'])
    df_clean['ex_date'] = pd.to_datetime(df_clean['ex_date'])
    df_clean['payment_date'] = pd.to_datetime(df_clean['payment_date'])
    
    # Prepare CRSP lookup table (permno, date) -> dlycumfacshr
    print("\nPreparing CRSP lookup table...")
    crsp_lookup = df_crsp[['permno', 'dlycaldt', 'dlycumfacshr']].copy()
    crsp_lookup['dlycaldt'] = pd.to_datetime(crsp_lookup['dlycaldt'])
    crsp_lookup['dlycumfacshr'] = crsp_lookup['dlycumfacshr'].fillna(1.0)
    crsp_lookup = crsp_lookup.dropna(subset=['permno', 'dlycaldt'])
    
    # Create a dictionary for fast lookup: (permno, date) -> dlycumfacshr
    crsp_dict = crsp_lookup.set_index(['permno', 'dlycaldt'])['dlycumfacshr'].to_dict()
    print(f"  CRSP lookup entries: {len(crsp_dict):,}")
    
    # For each dividend, look up dlycumfacshr from CRSP
    # Priority: ex_date -> record_date -> declare_date -> payment_date -> 1.0
    print("\nLooking up split adjustment factors for each dividend...")
    
    def get_dlycumfacshr_and_source(row):
        """Look up dlycumfacshr for a dividend row, trying all dates in priority order.
        Returns (dlycumfacshr, source_date_type)"""
        permno = int(row['permno'])
        
        # Try ex_date first
        if pd.notna(row['ex_date']):
            key = (permno, row['ex_date'])
            if key in crsp_dict:
                return crsp_dict[key], 'ex'
        
        # Try record_date
        if pd.notna(row['record_date']):
            key = (permno, row['record_date'])
            if key in crsp_dict:
                return crsp_dict[key], 'record'
        
        # Try declare_date
        if pd.notna(row['declare_date']):
            key = (permno, row['declare_date'])
            if key in crsp_dict:
                return crsp_dict[key], 'declare'
        
        # Try payment_date
        if pd.notna(row['payment_date']):
            key = (permno, row['payment_date'])
            if key in crsp_dict:
                return crsp_dict[key], 'payment'
        
        # Default: no adjustment
        return 1.0, 'none'
    
    # Apply the lookup function to get dlycumfacshr for each dividend
    df_clean[['dlycumfacshr', 'dlycumfacshr_source']] = df_clean.apply(
        get_dlycumfacshr_and_source, axis=1, result_type='expand'
    )
    
    # Store original dividend amount before adjustment
    df_clean['dividend_amt_original'] = df_clean['dividend_amt']
    
    # Adjust dividend amount for splits: divide by dlycumfacshr
    df_clean['dividend_amt'] = df_clean['dividend_amt'] / df_clean['dlycumfacshr']
    
    # Sort by permno and ex_date
    df_clean = df_clean.sort_values(['permno', 'ex_date']).reset_index(drop=True)
    
    # Report results
    splits_adjusted = (df_clean['dlycumfacshr'] != 1.0).sum()
    print(f"\nAfter filtering to Y+CD and adjusting for splits:")
    print(f"  Records: {len(df_clean):,} ({100*len(df_clean)/len(df):.1f}% retained)")
    print(f"  Permnos: {df_clean['permno'].nunique():,}")
    print(f"  Dividends adjusted for splits: {splits_adjusted:,} ({100*splits_adjusted/len(df_clean):.1f}%)")
    print(f"\nSplit adjustment source breakdown:")
    print(df_clean['dlycumfacshr_source'].value_counts())
    
    # Merge in ticker from CRSP for manual inspection (take the most common ticker for each permno)
    print("\nMerging ticker from CRSP...")
    ticker_lookup = df_crsp.groupby('permno')['ticker'].agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else None).to_dict()
    df_clean['ticker'] = df_clean['permno'].map(ticker_lookup)
    
    # Keep dlycumfacshr and other diagnostic columns for manual inspection
    # (Don't drop them like we did before)
    
    return df_clean

def merge_split_factors(
    df_div: pd.DataFrame,
    df_price: pd.DataFrame) -> pd.DataFrame:
    """
    Merge dlycumfacshr (cumulative split adjustment factor) and ticker from CRSP daily stock file
    onto dividend data for declare_date, record_date, and ex_date.
    
    This allows us to detect if a split occurred that might explain dividend amount changes.
    
    Parameters:
    - df_div: Dividend dataframe with permno, declare_date, record_date, ex_date
    - df_price: CRSP daily stock dataframe with permno, dlycaldt, dlycumfacshr, ticker
    
    Returns:
    - pd.DataFrame: Dividend data with dlycumfacshr_declare, dlycumfacshr_record, dlycumfacshr_ex, and ticker columns added
    """
    print("\nMerging split adjustment factors and ticker from CRSP...")
    
    # Make a copy and convert date columns to datetime
    df_div = df_div.copy()
    df_div['declare_date'] = pd.to_datetime(df_div['declare_date'])
    df_div['record_date'] = pd.to_datetime(df_div['record_date'])
    df_div['ex_date'] = pd.to_datetime(df_div['ex_date'])
    
    # Prepare price data - keep only what we need
    price_factors = df_price[['permno', 'dlycaldt', 'dlycumfacshr', 'ticker']].copy()
    price_factors['dlycaldt'] = pd.to_datetime(price_factors['dlycaldt'])
    
    # Prepare split factors only (without ticker) for declare and record dates
    price_factors_no_ticker = df_price[['permno', 'dlycaldt', 'dlycumfacshr']].copy()
    price_factors_no_ticker['dlycaldt'] = pd.to_datetime(price_factors_no_ticker['dlycaldt'])
    
    # Merge on declare_date
    df_merged = df_div.merge(
        price_factors_no_ticker,
        left_on=['permno', 'declare_date'],
        right_on=['permno', 'dlycaldt'],
        how='left',
        suffixes=('', '_declare')
    )
    df_merged.rename(columns={'dlycumfacshr': 'dlycumfacshr_declare'}, inplace=True)
    df_merged.drop(columns=['dlycaldt'], inplace=True)
    
    # Merge on record_date
    df_merged = df_merged.merge(
        price_factors_no_ticker,
        left_on=['permno', 'record_date'],
        right_on=['permno', 'dlycaldt'],
        how='left',
        suffixes=('', '_record')
    )
    df_merged.rename(columns={'dlycumfacshr': 'dlycumfacshr_record'}, inplace=True)
    df_merged.drop(columns=['dlycaldt'], inplace=True)
    
    # Merge on ex_date (with ticker)
    df_merged = df_merged.merge(
        price_factors,
        left_on=['permno', 'ex_date'],
        right_on=['permno', 'dlycaldt'],
        how='left',
        suffixes=('', '_ex')
    )
    df_merged.rename(columns={'dlycumfacshr': 'dlycumfacshr_ex'}, inplace=True)
    df_merged.drop(columns=['dlycaldt'], inplace=True)
    
    # Report merge success
    declare_match_pct = 100 * df_merged['dlycumfacshr_declare'].notna().sum() / len(df_merged)
    record_match_pct = 100 * df_merged['dlycumfacshr_record'].notna().sum() / len(df_merged)
    ex_match_pct = 100 * df_merged['dlycumfacshr_ex'].notna().sum() / len(df_merged)
    ticker_match_pct = 100 * df_merged['ticker'].notna().sum() / len(df_merged)
    
    print(f"  Declare date matches: {declare_match_pct:.1f}%")
    print(f"  Record date matches: {record_match_pct:.1f}%")
    print(f"  Ex date matches: {ex_match_pct:.1f}%")
    print(f"  Ticker match: {ticker_match_pct:.1f}%")
    
    return df_merged

def identify_continuous_series(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify continuous dividend series within each permno based on:
    1. Time gaps between payments (using freq_type thresholds)
    2. Changes in freq_type (policy change)
    
    Adds columns:
    - days_since_prev: Days since previous payment
    - series_id: Unique ID for each continuous series within a permno
    
    Series breaks occur when:
    - Gap exceeds threshold for the freq_type (M=45, Q=120, S=210, A=450 days)
    - freq_type changes between payments
    """
    # Define max gaps for each frequency type
    FREQ_THRESHOLDS = {
        'M': 40,    # Monthly: ~30 days, allow up to 40
        'Q': 110,   # Quarterly: ~90 days, allow up to 110
        'S': 200,   # Semi-annual: ~180 days, allow up to 200
        'A': 400,   # Annual: ~365 days, allow up to 400
    }
    DEFAULT_THRESHOLD = 400  # For unknown/other frequencies
    
    df = df.copy()
    
    def assign_series_ids(group):
        """Assign series IDs within a permno group."""
        group = group.copy()
        group['days_since_prev'] = group['ex_date'].diff().dt.days
        group['series_id'] = 0
        
        series_counter = 0
        for i in range(1, len(group)):
            days_gap = group.iloc[i]['days_since_prev']
            curr_freq = group.iloc[i]['freq_type']
            prev_freq = group.iloc[i-1]['freq_type']
            
            # Get threshold for previous frequency (what we expect)
            threshold = FREQ_THRESHOLDS.get(prev_freq, DEFAULT_THRESHOLD)
            
            # Series breaks if:
            # 1. Gap is too large for the frequency
            # 2. Frequency type changed
            # 3. Gap is NaN (shouldn't happen but be safe)
            if pd.isna(days_gap) or days_gap > threshold or curr_freq != prev_freq:
                series_counter += 1
            
            group.iloc[i, group.columns.get_loc('series_id')] = series_counter
        
        return group
    
    df = df.groupby('permno', group_keys=False).apply(assign_series_ids)
    
    return df

def flag_reductions(
    df: pd.DataFrame, 
    threshold: float, 
    min_payments: int) -> pd.DataFrame:
    """
    Flag dividend reductions at a given threshold level.
    Only flags after min_payments in a CONTINUOUS series.
    
    Adds columns:
    - reduction_pct: Percentage change from previous dividend to current
    - is_reduction_{threshold}pct: Boolean flag for reductions >= threshold
    - payment_number: Payment number within each continuous series
    
    Parameters:
    - df: DataFrame with dividends (must be sorted by permno, ex_date)
    - threshold: Reduction threshold as decimal (e.g., 0.25 for 25%)
    - min_payments: Minimum payments in a series before flagging reductions
    
    Returns:
    - DataFrame with reduction flags added
    """
    df = df.copy()
    
    print(f"\n{'='*80}")
    print(f"FLAGGING REDUCTIONS >= {threshold*100:.0f}%")
    print(f"{'='*80}")
    
    # Filter to rows with declare_date (needed for analysis)
    rows_before = len(df)
    df = df[df['declare_date'].notna()].copy()
    print(f"Filtered to rows with declare_date: {len(df):,} / {rows_before:,} ({100*len(df)/rows_before:.1f}%)")
    
    # Identify continuous series
    print("Identifying continuous dividend series...")
    df = identify_continuous_series(df)
    num_series = df.groupby('permno')['series_id'].max().sum() + len(df['permno'].unique())
    print(f"  Found {num_series:,} continuous series across {df['permno'].nunique():,} permnos")
    
    # Calculate previous dividend amount within each continuous series
    df['prev_dividend'] = df.groupby(['permno', 'series_id'])['dividend_amt'].shift(1)
    
    # Calculate percentage change
    df['reduction_pct'] = ((df['dividend_amt'] - df['prev_dividend']) / df['prev_dividend'] * 100)
    
    # Number each payment within each continuous series
    df['payment_number'] = df.groupby(['permno', 'series_id']).cumcount() + 1
    
    # Flag reductions: 
    # - Must have previous dividend (not first payment in series)
    # - Must have >= min_payments in this series
    # - Reduction must be >= threshold (negative percentage)
    threshold_pct = -threshold * 100  # Convert to negative percentage
    
    flag_column = f'is_reduction_{int(threshold*100)}pct'
    df[flag_column] = (
        (df['prev_dividend'].notna()) &  # Not first payment in series
        (df['payment_number'] > min_payments) &  # After min_payments in this series
        (df['reduction_pct'] <= threshold_pct)  # Reduction >= threshold
    )
    
    # Stats
    num_reductions = df[flag_column].sum()
    num_permnos_with_cuts = df[df[flag_column]]['permno'].nunique()
    
    print(f"\nResults:")
    print(f"  Total reduction events flagged: {num_reductions:,}")
    print(f"  Permnos with cuts: {num_permnos_with_cuts:,}")
    print(f"  % of dividends that are cuts: {100*num_reductions/len(df):.2f}%")
    
    # Show distribution of reduction percentages for flagged events
    if num_reductions > 0:
        flagged_reductions = df[df[flag_column]]['reduction_pct']
        print(f"\nReduction % distribution (for flagged events):")
        print(f"  Mean: {flagged_reductions.mean():.1f}%")
        print(f"  Median: {flagged_reductions.median():.1f}%")
        print(f"  Min: {flagged_reductions.min():.1f}%")
        print(f"  Max: {flagged_reductions.max():.1f}%")
    
    print(f"{'='*80}\n")
    
    return df

def analyze_flagged_cuts(
    df: pd.DataFrame,
    flag_column: str,
    num_examples: int,
    start_year: int,
    analysis_type: str) -> None:
    """
    Analyze dividend data with context around specific events.
    
    Parameters:
    - df: DataFrame with dividend data
    - flag_column: Name of the flag column (for cuts analysis)
    - num_examples: Number of example permnos to show
    - start_year: Only show data from this year onwards
    - analysis_type: 'cuts' = context around flagged cuts, 
    """

    # Filter to recent data
    df['year'] = pd.to_datetime(df['declare_date']).dt.year
    df_recent = df[df['year'] >= start_year].copy()
    
    print(f"\nTotal records from {start_year}+: {len(df_recent):,}")
    

    # Get flagged cuts
    events_recent = df_recent[df_recent[flag_column] == True].copy()
    print(f"Total cuts flagged: {len(events_recent):,}")
    print(f"Unique permnos with cuts: {events_recent['permno'].nunique():,}")
    
    if len(events_recent) == 0:
        print("\nNo cuts found!")
        return
    
    # Stats
    print(f"\nReduction % stats:")
    print(events_recent['reduction_pct'].describe())
    print(f"\nCuts by year:")
    print(events_recent['year'].value_counts().sort_index())
    event_type = "cuts"

    
    # Show examples with context
    print(f"\n{'='*80}")
    print(f"EXAMPLES - {num_examples} {event_type} with context (5 rows before/after):")
    print(f"{'='*80}")
    
    permnos_with_cuts = events_recent['permno'].unique()[:num_examples]
    
    for i, permno in enumerate(permnos_with_cuts, 1):
        # Use FULL history to get context
        permno_full_hist = df[df['permno'] == permno].copy()
        
        # Find first event in recent period
        if analysis_type == 'cuts':
            events_for_permno = df_recent[(df_recent['permno'] == permno) & (df_recent[flag_column] == True)]
        else:
            events_for_permno = df_recent[(df_recent['permno'] == permno) & (df_recent['shares_adj_factor'] != 0)]
        
        if len(events_for_permno) == 0:
            continue
        
        first_event_date = events_for_permno['declare_date'].iloc[0]
        
        # Find position in FULL history
        event_pos = permno_full_hist[permno_full_hist['declare_date'] == first_event_date].index[0]
        event_pos_iloc = permno_full_hist.index.get_loc(event_pos)
        
        # Get context (5 before, 5 after)
        start = max(0, event_pos_iloc - 5)
        end = min(len(permno_full_hist), event_pos_iloc + 6)
        context = permno_full_hist.iloc[start:end].copy()
        
        context['declare_date'] = pd.to_datetime(context['declare_date']).dt.strftime('%Y-%m-%d')
        context['record_date'] = pd.to_datetime(context['record_date']).dt.strftime('%Y-%m-%d')
        context['ex_date'] = pd.to_datetime(context['ex_date']).dt.strftime('%Y-%m-%d')
        
        print(f"\n--- Example {i}: Permno {permno} (Ticker: {context['ticker'].iloc[0] if 'ticker' in context.columns else 'N/A'}) ---")
        cols_to_show = ['declare_date', 'ex_date', 'dividend_amt']
        
        # Add diagnostic columns if they exist
        if 'dividend_amt_original' in context.columns:
            cols_to_show.append('dividend_amt_original')
        if 'dlycumfacshr' in context.columns:
            cols_to_show.append('dlycumfacshr')
        if 'dlycumfacshr_source' in context.columns:
            cols_to_show.append('dlycumfacshr_source')
        
        cols_to_show.extend(['prev_dividend', 'reduction_pct', 'days_since_prev', 'freq_type', 'series_id', 'payment_number', flag_column])
        
        print(context[cols_to_show].to_string(index=False))

    
    print(f"\n{'='*80}")

def diagnostic_div(
    df: pd.DataFrame
    ):
    """
    Diagnostic to understand each permno's dividend history.
    
    For each permno, calculates what % of its records are Y+CD (clean) vs not.
    Shows distribution of these percentages across all permnos.
    """
    print("="*80)
    print("DIVIDEND HISTORY DIAGNOSTIC (BY PERMNO)")
    print("="*80)
    
    # Sort by permno and ex_date
    df = df.sort_values(['permno', 'ex_date']).copy()
    
    print(f"Total dividend records: {len(df):,}")
    print(f"Unique permnos: {df['permno'].nunique():,}")
    
    # Mark each row as clean (Y + CD) or not
    df['is_clean_row'] = (df['is_ordinary_div'] == 'Y') & (df['dist_type'] == 'CD')
    
    # For each permno, calculate % of rows that are clean
    permno_stats = df.groupby('permno').agg({
        'is_clean_row': lambda x: 100 * x.sum() / len(x),  # % of rows that are Y+CD
        'ex_date': 'count',  # Total number of dividends
        'dividend_amt': 'mean'  # Average dividend amount
    }).rename(columns={
        'is_clean_row': 'pct_clean', 
        'ex_date': 'num_dividends', 
        'dividend_amt': 'avg_dividend'
    })
    
    # Categorize permnos by cleanliness
    permno_stats['category'] = pd.cut(
        permno_stats['pct_clean'], 
        bins=[-0.1, 0, 50, 90, 99.9, 100.1],
        labels=['0% clean', '1-50% clean', '51-90% clean', '91-99% clean', '100% clean']
    )
    
    print(f"\n{'='*80}")
    print("PERMNO CLEANLINESS DISTRIBUTION:")
    print(f"{'='*80}")
    print(permno_stats['category'].value_counts().sort_index())
    print(f"\nAs percentages:")
    print(100 * permno_stats['category'].value_counts(normalize=True).sort_index())
    
    # Show detailed stats
    print(f"\n{'='*80}")
    print("DETAILED STATS (% CLEAN PER PERMNO):")
    print(f"{'='*80}")
    print(permno_stats['pct_clean'].describe())
    
    # Count how many permnos are 100% clean vs not
    fully_clean = (permno_stats['pct_clean'] == 100).sum()
    partially_clean = (permno_stats['pct_clean'] > 90) & (permno_stats['pct_clean'] < 100)
    partially_clean_count = partially_clean.sum()
    no_clean = (permno_stats['pct_clean'] == 0).sum()
    
    print(f"\n100% clean permnos: {fully_clean:,} ({100*fully_clean/len(permno_stats):.1f}%)")
    print(f"Partially clean (90-99%): {partially_clean_count:,} ({100*partially_clean_count/len(permno_stats):.1f}%)")
    print(f"0% clean permnos: {no_clean:,} ({100*no_clean/len(permno_stats):.1f}%)")
    
    # Check records (rows) by cleanliness
    total_rows = len(df)
    clean_rows = df['is_clean_row'].sum()
    
    print(f"\n{'='*80}")
    print("TOTAL ROWS (RECORDS) BREAKDOWN:")
    print(f"{'='*80}")
    print(f"Clean rows (Y+CD): {clean_rows:,} ({100*clean_rows/total_rows:.1f}%)")
    print(f"Non-clean rows: {total_rows - clean_rows:,} ({100*(total_rows-clean_rows)/total_rows:.1f}%)")
    
    # Show what's in the non-clean rows
    non_clean_df = df[~df['is_clean_row']].copy()
    if len(non_clean_df) > 0:
        print(f"\n{'='*80}")
        print("WHAT'S IN NON-CLEAN ROWS:")
        print(f"{'='*80}")
        print("is_ordinary_div values:")
        print(non_clean_df['is_ordinary_div'].value_counts())
        print("\ndist_type values:")
        print(non_clean_df['dist_type'].value_counts())
    
    # Show examples from different categories
    print(f"\n{'='*80}")
    print("EXAMPLES FROM EACH CATEGORY:")
    print(f"{'='*80}")
    
    for cat in ['100% clean', '91-99% clean', '51-90% clean']:
        cat_permnos = permno_stats[permno_stats['category'] == cat]
        if len(cat_permnos) > 0:
            example_permno = cat_permnos.index[0]
            pct = cat_permnos.loc[example_permno, 'pct_clean']
            print(f"\n{cat} - Permno {example_permno} ({pct:.1f}% clean):")
            print(df[df['permno'] == example_permno][['ex_date', 'dividend_amt', 'is_ordinary_div', 'dist_type', 'freq_type']].head(10))
    
    print(f"\n{'='*80}")
    
    # Additional analysis: What happens if we drop non-clean rows?
    print(f"\n{'='*80}")
    print("IMPACT OF DROPPING NON-CLEAN ROWS:")
    print(f"{'='*80}")
    
    # Simulate dropping non-clean rows
    clean_only_df = df[df['is_clean_row']].copy()
    
    print(f"\nBefore filtering:")
    print(f"  Total records: {len(df):,}")
    print(f"  Unique permnos: {df['permno'].nunique():,}")
    
    print(f"\nAfter dropping non-clean rows:")
    print(f"  Total records: {len(clean_only_df):,} ({100*len(clean_only_df)/len(df):.1f}% retained)")
    print(f"  Unique permnos: {clean_only_df['permno'].nunique():,} (lost {df['permno'].nunique() - clean_only_df['permno'].nunique()})")
    
    # Check average dividends per permno before/after
    avg_before = df.groupby('permno').size().mean()
    avg_after = clean_only_df.groupby('permno').size().mean()
    
    print(f"\nAverage dividends per permno:")
    print(f"  Before: {avg_before:.1f}")
    print(f"  After: {avg_after:.1f} ({100*avg_after/avg_before:.1f}% retained)")
    
    # For the "mostly clean" permnos (91-99%), what are they losing?
    mostly_clean_permnos = permno_stats[(permno_stats['pct_clean'] >= 91) & (permno_stats['pct_clean'] < 100)].index
    mostly_clean_non_clean_rows = df[(df['permno'].isin(mostly_clean_permnos)) & (~df['is_clean_row'])]
    
    if len(mostly_clean_non_clean_rows) > 0:
        print(f"\n{'='*80}")
        print("WHAT 91-99% CLEAN PERMNOS ARE LOSING:")
        print("="*80)
        print(f"Total non-clean rows from 91-99% clean permnos: {len(mostly_clean_non_clean_rows):,}")
        print("\nDist types being dropped:")
        print(mostly_clean_non_clean_rows['dist_type'].value_counts())
        print("\nis_ordinary_div being dropped:")
        print(mostly_clean_non_clean_rows['is_ordinary_div'].value_counts())
    
    print(f"\n{'='*80}")
    print("RECOMMENDATION:")
    print("="*80)
    print("DROP ROWS (not permnos) - you keep 93.7% of data")
    print("The dropped rows are mostly splits, special divs, etc (not regular dividends)")
    print("This approach retains data from 91-99% clean permnos")
    print("="*80)
    
    return df, permno_stats, clean_only_df

def main():
    load_dotenv()

    # Get path relative to this script file
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent.parent / "Data"
    
    # Or use environment variable override if set
    if os.getenv("DATA_DIR"):
        data_dir = Path(os.getenv("DATA_DIR"))
        
    # Load dividend data
    div_input_file = data_dir / "crsp_distribution_events" / "stkdistributions_combined_typed.parquet"
    df_div = pd.read_parquet(div_input_file)

    
    # Load CRSP daily stock data (for split adjustments)
    price_input_file = data_dir / "crsp" / "crsp_dsf_cleaned.parquet"
    df_price = pd.read_parquet(price_input_file)
    
    # Step 1: Prepare dividend data (filter to Y+CD, adjust for splits, add ticker, sort)
    df_clean = prepare_dividend_data(df_div, df_price)
    
    # Step 2: Flag reductions
    threshold = 0.50
    min_payments = 8
    df_flagged = flag_reductions(df_clean, threshold, min_payments)

    # # Step 3: Save flagged dividend data
    # output_file = data_dir / "crsp_distribution_events" / f"dividends_with_{int(threshold*100)}pct_cut_flags.parquet"
    # df_flagged.to_parquet(output_file, index=False)
    # print(df_flagged)
    # print(f"\nSaved flagged dividend data to: {output_file}")
    # print(f"Shape: {df_flagged.shape}")
    # print(f"Columns: {list(df_flagged.columns)}")
    
    # Step 4: Analyze the flagged cuts
    analyze_flagged_cuts(df_flagged, flag_column=f'is_reduction_{int(threshold*100)}pct', 
                        num_examples=25, start_year=2019, analysis_type='cuts')


if __name__ == "__main__":
    main()