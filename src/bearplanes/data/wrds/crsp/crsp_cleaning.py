import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

## prepares crsp data for use ##

# Columns to keep from CRSP daily data
COLUMNS_TO_KEEP = [
    'permno',           # Unique security ID
    'permco',           # Unique company ID
    'hdrcusip',         # Header CUSIP (8 chars)
    'cusip',            # Full CUSIP
    'ticker',           # Ticker symbol
    'shrout',           # Shares outstanding
    'siccd',            # SIC code
    'dlycaldt',         # Daily calendar date
    'sharetype',        # Share type (filter: NS and '')
    'securitytype',     # Security type (filter: EQTY)
    'securitysubtype',  # Sub type (filter: COM)
    'usincflg',         # US incorporation flag
    'primaryexch',      # Primary exchange
    'conditionaltype',  # Conditional type (filter: RW)
    'tradingstatusflg', # Trading status
    'dlycap',           # Daily market cap
    'dlycapflg',        # Daily cap flag
    'dlydistretflg',    # Distribution return flag
    'dlyvol',           # Daily volume
    'dlyopen',          # Daily open
    'dlyhigh',          # Daily high
    'dlylow',           # Daily low
    'dlyclose',         # Daily close
    'dlycumfacshr',     # Cumulative share adjustment factor (splits/stock dividends only)
]

# Note: We deliberately exclude dlycumfacpr (full price adjustment factor) because we want
# to see raw price movements from cash dividends. For a dividend cuts strategy, we need
# to observe actual price drops when dividends are paid to detect when they're cut/eliminated.
# We only adjust for dlycumfacshr to handle mechanical share changes (splits, reverse splits).

# Optimized dtypes for memory efficiency
DTYPE_CONVERSIONS = {
    # Integer columns - downcast to smaller types
    'permno': 'int32',
    'permco': 'int32',
    'shrout': 'Int32',          # Shares outstanding (nullable - has 760 NAs)
    'siccd': 'int16',           # SIC codes are small
    
    # Float columns - keep float64 for precision in financial calculations
    'dlycap': 'float64',        # Market cap (has some NAs, but float handles them)
    'dlyvol': 'float32',        # Volume - safe as float32 (has some NAs, float handles)
    'dlyopen': 'float64',       # OHLC data - keep precision for return calculations
    'dlyhigh': 'float64',
    'dlylow': 'float64',
    'dlyclose': 'float64',
    'dlycumfacshr': 'float64',  # Share adjustment factor - CRITICAL for splits
}

# Columns that should be categorical (limited unique values)
CATEGORICAL_COLUMNS = [
    'sharetype',
    'securitytype',
    'securitysubtype',
    'usincflg',
    'primaryexch',
    'conditionaltype',
    'tradingstatusflg',
    'dlycapflg',
    'dlydistretflg',
]

# String columns to keep as strings (high cardinality)
STRING_COLUMNS = [
    'hdrcusip',
    'cusip',
    'ticker',
    'dlycaldt',
]


def load_and_optimize_crsp_year(
    file_path,
    year) -> pd.DataFrame:
    """
    Load a single CRSP parquet file and optimize its memory usage.
    We drop a number of columns that we won't need to look at / use and then convert datatypes.
    
    Parameters:
    -----------
    file_path : str or Path
        Path to the CRSP parquet file
        
    Returns:
    --------
    pd.DataFrame
        Optimized DataFrame with selected columns and dtypes
    """    
    # Load the dataframes with only the columns we need
    df = pd.read_parquet(file_path, columns=COLUMNS_TO_KEEP)
    
    # Print original size of dataframe
    print(f"  Original size for {year}: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Convert dtypes for optimization
    for col, dtype in DTYPE_CONVERSIONS.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    
    # Convert to categorical
    for col in CATEGORICAL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    # Print size afterwards
    print(f"  Optimized size for {year}: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    return df

def load_all_crsp_data(
    data_dir, 
    start_year, 
    end_year):
    """
    Load all CRSP daily data files and concatenate them.
    
    Parameters:
    -----------
    data_dir : str or Path
        Directory containing CRSP parquet files
    start_year : int
        First year to load (inclusive)
    end_year : int
        Last year to load (inclusive)
        
    Returns:
    --------
    pd.DataFrame
        Combined DataFrame with all years
    """
    data_dir = Path(data_dir)
    df_list = []
    
    # start a loop from our first year through our last, in each loop, call our function 
    # to load the yearly data frames and optimize the types.
    for year in range(start_year, end_year + 1):
        file_path = data_dir / f"crsp_dsf_{year}.parquet"
        # Call to get our optimized dataframe
        df_year = load_and_optimize_crsp_year(file_path, year)
        # Append it to a list
        df_list.append(df_year)
        
    # Concatenate all dataframes
    df_combined = pd.concat(df_list, ignore_index=True)
    
    return df_combined

def filter_noise(
    df: pd.DataFrame,
    min_price: float = 0.1,
    min_volume: float = 1000.0) -> pd.DataFrame:
    """
    Filter out penny stocks and ultra-low-volume stocks that would produce noisy signals.
    
    Removes permnos where the stock has NEVER (across entire history):
    - Traded above min_price of $0.10 on any single day, AND
    - Had volume above min_volume 1,000 on any single day
    
    This is a very lenient filter - we only exclude stocks that fail BOTH conditions
    across their entire history. If a stock ever had one day above these thresholds,
    it passes through.

    """
    print(f"Original shape: {df.shape}")
    print(f"Unique permnos before filtering: {df['permno'].nunique()}")
    
    # For each permno, get the maximum price and volume ever observed
    permno_stats = df.groupby('permno').agg({
        'dlyhigh': 'max',    # Highest price ever reached
        'dlyvol': 'max'      # Highest volume ever reached
    }).reset_index()
    
    # Identify permnos that pass our filters
    # A permno is KEPT if it ever had:
    # - A day with price > min_price, OR
    # - A day with volume > min_volume
    # (We only exclude if it NEVER met either threshold)
    valid_permnos = permno_stats[
        (permno_stats['dlyhigh'] > min_price) | 
        (permno_stats['dlyvol'] > min_volume)
    ]['permno']
    
    # Filter the original dataframe
    df_filtered = df[df['permno'].isin(valid_permnos)].copy()
    
    print(f"Filtered shape: {df_filtered.shape}")
    print(f"Unique permnos after filtering: {df_filtered['permno'].nunique()}")
    print(f"Removed {df['permno'].nunique() - df_filtered['permno'].nunique()} permnos")
    print(f"Removed {len(df) - len(df_filtered):,} rows ({100 * (len(df) - len(df_filtered)) / len(df):.2f}%)")
    
    return df_filtered

def filter_security_type(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out permno series where the stock was NEVER EQTY/COM across its entire history.
    
    If a permno ever had securitytype='EQTY' and securitysubtype='COM' on any day,
    we keep ALL rows for that permno (even rows where it might have different types).
    """
    print(f"Original shape: {df.shape}")
    print(f"Unique permnos before filtering: {df['permno'].nunique()}")
    
    # Find permnos that were EVER EQTY and COM
    permnos_with_eqty_com = df[
        (df['securitytype'] == 'EQTY') & 
        (df['securitysubtype'] == 'COM')
    ]['permno'].unique()
    
    # Keep all rows for those permnos
    df_filtered = df[df['permno'].isin(permnos_with_eqty_com)].copy()

    print(f"Filtered shape: {df_filtered.shape}")
    print(f"Unique permnos after filtering: {df_filtered['permno'].nunique()}")
    print(f"Removed {df['permno'].nunique() - df_filtered['permno'].nunique()} permnos")
    
    return df_filtered

def check_security_type(
    df: pd.DataFrame):
    # Check for NA values first
    total_rows = len(df)
    na_count = df['securitytype'].isna().sum()
    print(f"Total rows: {total_rows:,}")
    print(f"Rows with NA securitytype: {na_count:,} ({100 * na_count / total_rows:.2f}%)")
    print(f"Rows with valid securitytype: {total_rows - na_count:,} ({100 * (total_rows - na_count) / total_rows:.2f}%)")
    print()
    
    # See what combinations of security types exist per permno
    type_combos = df.groupby('permno')['securitytype'].apply(
        lambda x: tuple(sorted(x.dropna().unique()))
    )
    
    print("Security type combinations (sorted unique types per permno):")
    print(type_combos.value_counts())
    print(f"\nTotal unique combinations: {type_combos.nunique()}")

def check_security_subtype(
    df: pd.DataFrame):
    # Check for NA values first
    total_rows = len(df)
    na_count = df['securitysubtype'].isna().sum()
    print(f"Total rows: {total_rows:,}")
    print(f"Rows with NA securitysubtype: {na_count:,} ({100 * na_count / total_rows:.2f}%)")
    print(f"Rows with valid securitysubtype: {total_rows - na_count:,} ({100 * (total_rows - na_count) / total_rows:.2f}%)")
    print()
    
    # See what combinations of security subtypes exist per permno
    subtype_combos = df.groupby('permno')['securitysubtype'].apply(
        lambda x: tuple(sorted(x.dropna().unique()))
    )
    
    print("Security subtype combinations (sorted unique subtypes per permno):")
    print(subtype_combos.value_counts())
    print(f"\nTotal unique combinations: {subtype_combos.nunique()}")

def check_p_exchange(
    df: pd.DataFrame):
    # Check for NA values first
    total_rows = len(df)
    na_count = df['primaryexch'].isna().sum()
    print(f"Total rows: {total_rows:,}")
    print(f"Rows with NA primaryexch: {na_count:,} ({100 * na_count / total_rows:.2f}%)")
    print(f"Rows with valid primaryexch: {total_rows - na_count:,} ({100 * (total_rows - na_count) / total_rows:.2f}%)")
    print()
    
    # See what combinations of exchanges stocks move between
    exchange_combos = df.groupby('permno')['primaryexch'].apply(
        lambda x: tuple(sorted(x.dropna().unique()))
    )
    
    print("Exchange combinations (sorted unique exchanges per permno):")
    print(exchange_combos.value_counts())
    print(f"\nTotal unique combinations: {exchange_combos.nunique()}")

def sort_by_permno_date(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort dataframe by permno and date for time-series operations.
    """
    df = df.sort_values(['permno', 'dlycaldt']).reset_index(drop=True)
    return df

def adjust_price(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjusts prices and volumes for splits using the dlycumfacshr field.
    
    Returns a dataframe with original columns PLUS new adjusted columns:
    - adj_open, adj_high, adj_low, adj_close (split-adjusted prices)
    - adj_volume, adj_shrout (split-adjusted volume/shares)
    """
    df = df.copy()
    
    # Fill any missing cumulative factors with 1.0 (no adjustment)
    df['dlycumfacshr'] = df['dlycumfacshr'].fillna(1.0)
    
    # To get split-adjusted prices (adjusted to most recent date):
    # Divide prices by the cumulative factor
    df['adj_open'] = df['dlyopen'] / df['dlycumfacshr']
    df['adj_high'] = df['dlyhigh'] / df['dlycumfacshr']
    df['adj_low'] = df['dlylow'] / df['dlycumfacshr']
    df['adj_close'] = df['dlyclose'] / df['dlycumfacshr']

    # For volume/shares - multiply (opposite direction)
    # When shares split 2-for-1, historical volume should be doubled to be comparable
    df['adj_volume'] = df['dlyvol'] * df['dlycumfacshr']
    df['adj_shrout'] = df['shrout'] * df['dlycumfacshr']
        
    return df

def permno_permco_analysis(
    df: pd.DataFrame):
    """
    Analyze the permno/permco relationship to understand multiple share classes.
    """
    # Load distributions data
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent / "Data" / "crsp_distribution_events"
    distributions_file = data_dir / "stkdistributions_combined_typed.parquet"
    distributions = pd.read_parquet(distributions_file)
    
    # How bad is the multi-class problem?
    companies_with_multiple_permnos = df.groupby('permco')['permno'].nunique()

    print(f"Total companies (permco): {len(companies_with_multiple_permnos):,}")
    print(f"Companies with multiple permnos: {(companies_with_multiple_permnos > 1).sum():,}")
    print(f"Percentage: {(companies_with_multiple_permnos > 1).mean():.1%}")

    # For dividend payers specifically
    divs_permnos = distributions[
        (distributions['is_ordinary_div'] == 'Y') &
        (distributions['dist_type'] == 'CD')
    ]['permno'].unique()

    df_divpayers = df[df['permno'].isin(divs_permnos)]
    divpayer_companies = df_divpayers.groupby('permco')['permno'].nunique()

    print(f"\nAmong dividend payers:")
    print(f"Total companies: {len(divpayer_companies):,}")
    print(f"Companies with multiple share classes: {(divpayer_companies > 1).sum():,}")
    print(f"Percentage: {(divpayer_companies > 1).mean():.1%}")

def test_share_adjustments(
    df: pd.DataFrame): 
    """
    Verify split adjustments are working correctly by examining known split events.
    
    Tests forward and reverse splits:
    - TSLA: Aug 21-29, 2022 (3-for-1 forward split on Aug 25)
    - AMZN: Jun 2-9, 2022 (20-for-1 forward split on Jun 6)
    - GOOGL: Jul 16-20, 2022 (20-for-1 forward split on Jul 18)
    - AMC: Aug 21-29, 2023 (1-for-10 reverse split on Aug 25)
    - AGRI: Dec 4-6, 2024 (1-for-100 reverse split on Dec 5)
    - RNAZ: Dec 3-5, 2024 (1-for-33 reverse split on Dec 4)
    - APVO: Dec 3-5, 2024 (1-for-37 reverse split on Dec 4)
    """
    
    # Debug: Check what we have
    print("DEBUG INFO:")
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {df.columns.tolist()}")
    print(f"dlycaldt type: {df['dlycaldt'].dtype}")
    print(f"dlycaldt sample: {df['dlycaldt'].head(3).tolist()}")
    print(f"ticker sample: {df['ticker'].head(10).tolist()}")
    print(f"Non-null tickers: {df['ticker'].notna().sum()} / {len(df)}")
    print(f"Unique tickers (first 20): {df['ticker'].dropna().unique()[:20].tolist()}")
    print()
    
    # Define test cases: (ticker, start_date, end_date, description)
    # Dates in YYYY-MM-DD format to match dlycaldt
    test_cases = [
        ('TSLA', '2022-08-21', '2022-08-29', 'TSLA 3-for-1 forward split (Aug 25, 2022)'),
        ('AMZN', '2022-06-02', '2022-06-09', 'AMZN 20-for-1 forward split (Jun 6, 2022)'),
        ('GOOGL', '2022-07-16', '2022-07-20', 'GOOGL 20-for-1 forward split (Jul 18, 2022)'),
        ('AMC', '2023-08-21', '2023-08-29', 'AMC 1-for-10 reverse split (Aug 25, 2023)'),
        ('AGRI', '2024-12-04', '2024-12-06', 'AGRI 1-for-100 reverse split (Dec 5, 2024)'),
        ('RNAZ', '2024-12-03', '2024-12-05', 'RNAZ 1-for-33 reverse split (Dec 4, 2024)'),
        ('APVO', '2024-12-03', '2024-12-05', 'APVO 1-for-37 reverse split (Dec 4, 2024)'),
    ]
    
    # Columns to display
    display_cols = [
        'dlycaldt', 'ticker', 
        'dlyopen', 'dlyhigh', 'dlylow', 'dlyclose', 'dlyvol',
        'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume',
        'dlycumfacshr'
    ]
    
    print("SPLIT ADJUSTMENT VERIFICATION")
    
    for ticker, start_date, end_date, description in test_cases:
        print(f"\n{description}")
        
        # Filter for this ticker and date range
        mask = (
            (df['ticker'] == ticker) & 
            (df['dlycaldt'] >= start_date) & 
            (df['dlycaldt'] <= end_date)
        )
        
        test_df = df[mask][display_cols].copy()
        
        if len(test_df) == 0:
            print(f"  No data found for {ticker} in this date range")
            # Debug: Check if ticker exists at all
            ticker_exists = (df['ticker'] == ticker).sum()
            print(f"  DEBUG: Total rows with ticker {ticker}: {ticker_exists}")
            continue
        
        # Sort by date
        test_df = test_df.sort_values('dlycaldt')
        
        # Display with nice formatting
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.float_format', lambda x: f'{x:.2f}')
        
        print(test_df.to_string(index=False))
        
        # Show the factor change around the split
        if len(test_df) > 1:
            factors = test_df['dlycumfacshr'].unique()
            if len(factors) > 1:
                print(f"\n  Factor change detected: {factors}")
    
    print("\n" + "="*80)
    print("Verification complete. Check that:")
    print("  - Forward splits: adj_close stays constant, raw prices drop")
    print("  - Reverse splits: adj_close stays constant, raw prices increase")
    print("  - dlycumfacshr changes on the split date")
    print("="*80)

def main():
    
    # Get path relative to this script file (works regardless of where script is run from)
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent / "Data" / "crsp"
    
    # Or use environment variable override if set
    if os.getenv("DATA_DIR"):
        data_dir = Path(os.getenv("DATA_DIR"))

    # load combined dataframe with crsp data
    file_path = data_dir / "crsp_dsf_combined.parquet"
    df = pd.read_parquet(file_path)

    # 1) remove untradable securities 
    df_noise_filtered = filter_noise(df)

    # 2) filter security type (loosely)
    filtered_security = filter_security_type(df_noise_filtered)

    # 3) adjust price 
    price_adjusted = adjust_price(filtered_security)

    # 4) sort 
    df_prepared = sort_by_permno_date(price_adjusted)

    # Save parquet again
    output_path  = data_dir / "crsp_dsf_cleaned.parquet"
    df_prepared.to_parquet(output_path)

    print(f"\nCleaned CRSP data saved to: {output_path}")
    print(f"Final shape: {df_prepared.shape}")
    print(f"Date range: {df_prepared['dlycaldt'].min()} to {df_prepared['dlycaldt'].max()}")

if __name__ == "__main__":
    main()
