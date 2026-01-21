import pandas as pd

def optimize_datatypes(
    df: pd.DataFrame,
    COLUMNS_TO_KEEP: {},
    DTYPE_CONVERSIONS: {},
    CATEGORICAL_COLUMNS: [],
    DATE_COLUMNS: []
    ) -> pd.DataFrame:
    """
    Optimize distributions dataframe memory usage.
    Select columns, rename them, and convert to optimal dtypes.

    Args:
        df
        COLUMNS_TO_KEEP
        DTYPE_CONVERSIONS
        CATEGORICAL_COLUMNS
        DATE_COLUMNS
    """
    print(f"Original size: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Select and rename columns
    df = df[list(COLUMNS_TO_KEEP.keys())].copy()
    # Renames the columns we are keeping using a dictionary datastructure
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

def optimize_datatypes_v2():
    """ just want this to auto detect and optimize the datatypes of a given dataframe """
    pass