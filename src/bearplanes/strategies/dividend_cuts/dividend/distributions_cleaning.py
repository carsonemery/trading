import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

## Renames columns, drops a few columns and changes the types of columns ##

# Columns to keep and rename
COLUMNS_TO_KEEP = {
    # Identifiers
    'permno': 'permno',  # CRSP security identifier
    'disexdt': 'ex_date',  # Ex-Distribution Date - KEY DATE for matching CRSP
    'disseqnbr': 'seq_num',  # Sequence number (multiple distributions on same date)
    
    # Distribution characteristics
    'disordinaryflg': 'is_ordinary_div',  # Is this a regular dividend? (Y/N)
    'distype': 'dist_type',  # Distribution type (CD=Cash Dividend, SD=Stock Dividend, etc)
    'disfreqtype': 'freq_type',  # Frequency (M=Monthly, Q=Quarterly, etc)
    'disdetailtype': 'detail_type',  # Detail type
    
    # Payment info
    'dispaymenttype': 'payment_currency',  # Currency of payment
    'disorigcurtype': 'orig_currency',  # Original currency
    'disdivamt': 'dividend_amt',  # Dividend amount per share
    
    # Price/share adjustment factors
    'disfacpr': 'price_adj_factor',  # Factor to adjust price
    'disfacshr': 'shares_adj_factor',  # Factor to adjust shares
    
    # Additional dates
    'disdeclaredt': 'declare_date',  # When dividend was announced
    'disrecorddt': 'record_date',  # Who gets the dividend
    'dispaydt': 'payment_date',  # When dividend is actually paid
    
    # Related securities
    'dispermno': 'received_permno',  # PERMNO of security received (for stock dividends)
    'dispermco': 'issuer_permco',  # PERMCO of issuer providing payment
}

# Optimized dtypes for memory efficiency
DTYPE_CONVERSIONS = {
    # Integer columns
    'permno': 'int32',
    'seq_num': 'int32',
    'received_permno': 'Int32',  # Nullable - only for stock dividends
    'issuer_permco': 'Int32',    # Nullable
    
    # Float columns - keep precision for financial calculations
    'dividend_amt': 'float64',
    'price_adj_factor': 'float64',
    'shares_adj_factor': 'float64',
}

# Columns that should be categorical
CATEGORICAL_COLUMNS = [
    'is_ordinary_div',
    'dist_type',
    'freq_type',
    'detail_type',
    'payment_currency',
    'orig_currency',
]

# Date columns to convert to datetime
DATE_COLUMNS = [
    'ex_date',
    'declare_date',
    'record_date',
    'payment_date',
]


def optimize_distributions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize distributions dataframe memory usage.
    Select columns, rename them, and convert to optimal dtypes.
    """
    print(f"Original size: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Select and rename columns
    df = df[list(COLUMNS_TO_KEEP.keys())].copy()
    df = df.rename(columns=COLUMNS_TO_KEEP)
    
    # Convert numeric dtypes
    for col, dtype in DTYPE_CONVERSIONS.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    
    # Convert to categorical
    for col in CATEGORICAL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    # Convert date columns to datetime
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    print(f"Optimized size: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    return df


if __name__ == "__main__":
    # Get path relative to this script file
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent / "Data" / "crsp_distribution_events"
    
    # Or use environment variable override if set
    if os.getenv("DATA_DIR"):
        data_dir = Path(os.getenv("DATA_DIR"))
    
    print(f"Loading data from: {data_dir}")
    
    # Load combined file
    input_file = data_dir / "stkdistributions_combined.parquet"
    df = pd.read_parquet(input_file)
    
    # Optimize
    df = optimize_distributions(df)
    
    # Save optimized version
    output_file = data_dir / "stkdistributions_combined_typed.parquet"
    df.to_parquet(output_file, index=False)
    
    print(f"Saved to: {output_file}")
