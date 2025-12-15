## merges into crsp data on declaration date ##
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv


def merge(
    df_dist: pd.DataFrame,
    df_crsp: pd.DataFrame) -> pd.DataFrame:
    """
    Merges dividend distribution data into CRSP daily stock data.
    
    For each CRSP trading day, adds dividend information if there was a dividend event on that date.
    Uses declare_date from dividends as the primary merge key (falls back to ex_date if declare_date is missing).
    
    Parameters:
    - df_dist: Dividend data with reduction flags
    - df_crsp: CRSP daily stock data
    
    Returns:
    - CRSP data with dividend columns added (NaN for days without dividend events)
    """
    print(f"\n{'='*80}")
    print("MERGING DIVIDEND DATA INTO CRSP")
    print(f"{'='*80}")
    print(f"CRSP rows: {len(df_crsp):,}")
    print(f"CRSP permnos: {df_crsp['permno'].nunique():,}")
    print(f"Dividend rows: {len(df_dist):,}")
    print(f"Dividend permnos: {df_dist['permno'].nunique():,}")
    
    # Prepare dividend data for merge
    df_dist_merge = df_dist.copy()
    
    # Create merge_date: use declare_date if available, else ex_date
    df_dist_merge['merge_date'] = df_dist_merge['declare_date'].fillna(df_dist_merge['ex_date'])
    
    # Ensure both date columns are datetime
    df_dist_merge['merge_date'] = pd.to_datetime(df_dist_merge['merge_date'])
    df_crsp_merge = df_crsp.copy()
    df_crsp_merge['dlycaldt'] = pd.to_datetime(df_crsp_merge['dlycaldt'])
    
    # Select columns to merge from dividend data
    div_cols_to_merge = [
        'permno', 'merge_date',
        'declare_date', 'ex_date', 
        'dividend_amt', 'prev_dividend', 'reduction_pct',
        'series_id', 'payment_number', 'is_reduction_50pct',
        'freq_type'
    ]
    
    df_dist_subset = df_dist_merge[div_cols_to_merge].copy()
    
    # Handle duplicates: if multiple dividends on same date for same permno, keep the flagged one
    duplicates = df_dist_subset[df_dist_subset.duplicated(subset=['permno', 'merge_date'], keep=False)]
    if len(duplicates) > 0:
        print(f"\nWarning: {len(duplicates)} duplicate (permno, date) pairs in dividend data")
        print("Keeping rows with is_reduction_50pct=True when duplicates exist")
        # Sort so flagged cuts come first, then drop duplicates
        df_dist_subset = df_dist_subset.sort_values('is_reduction_50pct', ascending=False)
        df_dist_subset = df_dist_subset.drop_duplicates(subset=['permno', 'merge_date'], keep='first')
    
    print(f"\nDividend events to merge: {len(df_dist_subset):,}")
    print(f"  Events with is_reduction_50pct=True: {df_dist_subset['is_reduction_50pct'].sum():,}")
    
    # Left join: keep all CRSP rows, add dividend info where available
    df_merged = df_crsp_merge.merge(
        df_dist_subset,
        left_on=['permno', 'dlycaldt'],
        right_on=['permno', 'merge_date'],
        how='left',
        suffixes=('', '_div')
    )
    
    # Drop the merge_date column (redundant with dlycaldt)
    if 'merge_date' in df_merged.columns:
        df_merged = df_merged.drop(columns=['merge_date'])
    
    print(f"\nMerge complete:")
    print(f"  Total rows: {len(df_merged):,}")
    print(f"  Rows with dividend events: {df_merged['dividend_amt'].notna().sum():,}")
    print(f"  Rows with reduction flags: {df_merged['is_reduction_50pct'].fillna(False).sum():,}")
    print(f"{'='*80}\n")
    
    return df_merged


def main():
    load_dotenv()

    ## load files ##
    # Get path relative to this script file
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent.parent / "Data"
    
    # Or use environment variable override if set
    if os.getenv("DATA_DIR"):
        data_dir = Path(os.getenv("DATA_DIR"))
        
    # Load dividend data
    div_input_file = data_dir / "crsp_distribution_events" / "dividends_with_50pct_cut_flags.parquet"
    df_dist = pd.read_parquet(div_input_file)
    
    # Load CRSP daily stock data (for split adjustments)
    price_input_file = data_dir / "crsp" / "crsp_dsf_cleaned.parquet"
    df_crsp = pd.read_parquet(price_input_file)

    ## Run and Save Merge ##
    df_merged = merge(df_dist, df_crsp)

    # Save
    output_file = data_dir / "crsp" / "crsp_with_dividend_flags.parquet"
    print(f"\nSaving merged data to: {output_file}")
    df_merged.to_parquet(output_file, index=False)
    print(f"  Saved {len(df_merged):,} rows")
    print(f"  Columns: {list(df_merged.columns)}")
    
    # Quick stats
    print(f"\n{'='*80}")
    print("MERGE SUMMARY")
    print(f"{'='*80}")
    print(f"Total rows: {len(df_merged):,}")
    print(f"Rows with dividend events: {df_merged['dividend_amt'].notna().sum():,}")
    print(f"Rows with 50% reduction flags: {df_merged['is_reduction_50pct'].fillna(False).sum():,}")
    print(f"Date range: {df_merged['dlycaldt'].min()} to {df_merged['dlycaldt'].max()}")
    print(f"{'='*80}\n") 
    
if __name__ == "__main__":
    main()



    