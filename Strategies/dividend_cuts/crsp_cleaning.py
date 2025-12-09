import os
import pandas as pd
from pathlib import Path
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
    'dlynumtrd',        # Daily number of trades
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
    'shrout': 'int32',          # Shares outstanding (in thousands)
    'siccd': 'int16',           # SIC codes are small
    'dlynumtrd': 'int32',       # Number of trades
    
    # Float columns - keep float64 for precision in financial calculations
    'dlycap': 'float64',        # Market cap
    'dlyvol': 'float32',        # Volume - safe as float32 (whole numbers)
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
    file_path) -> pd.DataFrame:
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
    print(f"  Original size: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Convert dtypes for optimization
    for col, dtype in DTYPE_CONVERSIONS.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    
    # Convert to categorical
    for col in CATEGORICAL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    # Print size afterwards
    print(f"  Optimized size: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
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
        df_year = load_and_optimize_crsp_year(file_path)
        # Append it to a list
        df_list.append(df_year)
        
    # Concatenate all dataframes
    df_combined = pd.concat(df_list, ignore_index=True)
    
    return df_combined


if __name__ == "__main__":
    
    # Get path relative to this script file (works regardless of where script is run from)
    script_dir = Path(__file__).parent  # Strategies/dividend_cuts/
    data_dir = script_dir.parent.parent / "Data" / "crsp"  # emerytrading/Data/crsp/
    
    # Or use environment variable override if set
    if os.getenv("DATA_DIR"):
        data_dir = Path(os.getenv("DATA_DIR"))
    
    print(f"Loading data from: {data_dir}")
    
    # Load all data
    df = load_all_crsp_data(data_dir, 2010, 2024)
    
    # Save combined dataframe
    output_file = data_dir / "crsp_dsf_combined.parquet"
    df.to_parquet(output_file, index=False)
