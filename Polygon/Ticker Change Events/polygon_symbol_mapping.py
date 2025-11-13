import pandas as pd
import numpy as np
from tqdm import tqdm

#EXAMPLE WALKTHROUGH
# OUR MAPPING:
# 'META': [('META', '2022-06-09'), ('FB', '2012-05-18')]
# 'FB': [('FB', '2025-06-26')]  # Some ETF

# For row with ticker='FB', date='2020-01-01':
# - Check META mapping: FB appears with date '2012-05-18' <= '2020-01-01' ✓
# - Check FB mapping: FB appears with date '2025-06-26' <= '2020-01-01' ✗
# - Result: Use 'META' (FB belonged to Meta in 2020)

def prepare_mapping_dataframe(
    reverse_mapping: {},
    start_date: str
    ) -> pd.DataFrame:
    """ Converts the reverse_mapping dictionary into a dataframe

    Format Before (Examples):

    KVP:    
        Mapping: ('DRD', [('DRD', '2012-01-03'), ('DROOY', '2007-08-20'), ('DROOD', '2007-07-23')])
    Type: 
        Mapping: (<class 'str'>, <class 'list'>)

    KVP:
        Mapping: ('PWRD', [('PWRD', '2025-02-03'), ('NETZ', '2022-02-03')])
    Type:
        Mapping: (<class 'str'>, <class 'list'>)


    Format After:

    Long format (what we want)
    current_ticker | historical_ticker | change_date
    DRD           | DRD               | 2012-01-03
    DRD           | DROOY             | 2007-08-20
    DRD           | DROOD             | 2007-07-23
    PWRD          | PWRD              | 2025-02-03
    PWRD          | NETZ              | 2022-02-03

    """

    # Convert start date to Timestamp if provided 
    if start_date is not None:
        start_date = pd.to_datetime(start_date).normalize()

    # Create a list of dictionary objects to store the information we will convert into a dataframe
    # this is faster than concating/appending each row to the dataframe in the loop
    list_of_ticker_changes = []

    # Add progress bar for processing ticker mappings
    for current_ticker, history_list_of_tuples in tqdm(reverse_mapping.items(), desc="Preparing mapping dataframe", unit="ticker"):
        
        # Loop through each tuple in the list for this ticker
        for historical_ticker, change_date in history_list_of_tuples:

            # Convert date to Timestamp
            if not isinstance(change_date, pd.Timestamp):
                change_date = pd.to_datetime(change_date).normalize()

            # Filter out dates (and thereby name change events) that happened
            # before our historical OHLCV data begins (decreases the number of operations
            # we have to do later in map_symbols)
            if start_date is not None and change_date < start_date:
                continue # continue will skip the tuple

            # Add to our list 
            list_of_ticker_changes.append({
                'current_ticker': current_ticker,
                'historical_ticker': historical_ticker,
                'change_date': change_date
            })
        
    # Convert the list 
    mapping_df = pd.DataFrame(list_of_ticker_changes)

    return mapping_df

def map_symbols(
    reverse_mapping_dict: {},
    historical_data: pd.DataFrame,
    start_date: str
    ) -> pd.DataFrame:
    """ 
        Takes a symbol mapping dataframe and OHLCV dataframe and returns the dataframe with a mapped symbols column titled 
        'adjusted_ticker'.
    """

    # Copy the original dataframe to preserve all columns
    print("Creating copy of dataframe")
    historical_data = historical_data.copy()

    # convert mapping dictionary to Dataframe
    mapping_df = prepare_mapping_dataframe(reverse_mapping_dict, start_date)

    # Sort historical_data by ticker and date for merge_asof
    # CRITICAL: merge_asof requires the join key (date) to be sorted within each group (ticker)
    print("Sorting historical data for merge_asof...")
    with tqdm(total=1, desc="Sorting historical data", unit="operation") as pbar:
        historical_data_sorted = historical_data.sort_values(['ticker', 'date'], kind='stable').reset_index(drop=True)
        pbar.update(1)
    
    # Also ensure mapping_df is sorted correctly
    # It should already be sorted from prepare_mapping_dataframe, but let's be explicit
    print("Sorting mapping dataframe...")
    with tqdm(total=1, desc="Sorting mapping dataframe", unit="operation") as pbar:
        mapping_df_sorted = mapping_df.sort_values(['historical_ticker', 'change_date'], kind='stable').reset_index(drop=True)
        pbar.update(1)

    # Call our testing function
    print("Call diagnose")
    diagnose_data(historical_data_sorted)

    # Perform the merge_asof operation
    # NOTE: merge_asof is a single atomic pandas operation, so we can't show progress during execution
    # It's optimized C code that runs all at once. We'll just show a message.
    print(f"Performing merge_asof on {len(historical_data_sorted):,} rows (this may take a while)...")
    mapped_historical_data  = pd.merge_asof(
        # Left - the ~23M rows of OHLCV data
        historical_data_sorted,
        # Right - ~ 90k rows of ticker changes
        mapping_df_sorted,

        # Join condition - Find matching dates 
        left_on = 'date',
        right_on='change_date',

        # Grouping - only match within the same ticker
        left_by='ticker',
        right_by='historical_ticker',

        # Direction - find the most recent change on or before this date
        direction='backward'
    )
    print("Merge_asof completed!")

    # Handle tickers with no mapping 
    # I believe this is safe to do for now, because we filled tickers that did not return a mapping with the date 
    # 2025-11-08, which is earlier than any date in the OHCLV dataframe so the merge_asof should have some nas we can fill
    # with the original ticker name
    print("Filling missing mappings and cleaning up...")
    mapped_historical_data['adjusted_ticker'] = mapped_historical_data['current_ticker'].fillna(mapped_historical_data['ticker'])

    # Clean up temp columns
    mapped_historical_data = mapped_historical_data.drop(columns=['historical_ticker', 'change_date', 'current_ticker'])

    return mapped_historical_data

def diagnose_data(
    historical_df, 
    mapping_df=None, 
    ticker_col='ticker', 
    date_col='date'):
    """
    Comprehensive diagnosis function to identify sorting issues that could break merge_asof.
    
    Args:
        historical_df: The main historical OHLCV dataframe
        mapping_df: Optional mapping dataframe to also diagnose
        ticker_col: Name of the ticker column (default 'ticker')
        date_col: Name of the date column (default 'date')
    """
    print("\n" + "="*80)
    print("DATA DIAGNOSIS FOR MERGE_ASOF")
    print("="*80)
    
    # ========== BASIC INFO ==========
    print(f"\n[1] BASIC INFORMATION")
    print(f"    Shape: {historical_df.shape}")
    print(f"    Date dtype: {historical_df[date_col].dtype}")
    print(f"    Ticker dtype: {historical_df[ticker_col].dtype}")
    print(f"    Date range: {historical_df[date_col].min()} to {historical_df[date_col].max()}")
    print(f"    NaT/NaN dates: {historical_df[date_col].isna().sum():,}")
    print(f"    Unique tickers: {historical_df[ticker_col].nunique():,}")
    
    # ========== CHECK SORTING WITHIN TICKER GROUPS ==========
    print(f"\n[2] CHECKING SORT ORDER WITHIN TICKER GROUPS")
    print(f"    (merge_asof requires dates to be sorted within each ticker group)")
    
    # ===== NESTED FUNCTION EXPLANATION =====
    # This is a NESTED FUNCTION (function defined inside another function).
    # It's like a helper function that only exists within diagnose_data().
    # 
    # Why nested? It needs access to 'date_col' from the outer function.
    # This is called a "closure" - the inner function "closes over" outer variables.
    def check_monotonic(group):
        """
        Check if dates are non-decreasing (allows duplicates).
        
        This function will be PASSED to .apply() below.
        Python will call it once for each ticker group.
        
        Args:
            group: A pandas DataFrame containing all rows for ONE ticker
        
        Returns:
            True if dates are sorted (non-decreasing), False otherwise
        """
        # Extract just the date column as a numpy array
        # .values converts pandas Series to numpy array (faster operations)
        dates = group[date_col].values
        
        # np.diff() calculates differences between consecutive elements
        # Example: [2020-01-01, 2020-01-02, 2020-01-05] 
        #         -> [1 day, 3 days] (differences)
        diffs = np.diff(dates)
        
        # Check if ALL differences are >= 0 (non-negative)
        # This means dates are non-decreasing (sorted forward in time)
        # pd.Timedelta(0) represents zero time difference
        return np.all(diffs >= pd.Timedelta(0))
    
    # ===== FUNCTION PASSING / DELEGATE PATTERN =====
    # This is the KEY CONCEPT: We're passing a FUNCTION as an argument!
    #
    # .groupby(ticker_col) splits the dataframe into groups (one per ticker)
    #   Example: If we have tickers ['AAPL', 'MSFT', 'AAPL', 'MSFT']
    #            groupby creates: Group1 (all AAPL rows), Group2 (all MSFT rows)
    #
    # .apply(function) calls that function ONCE for each group
    #   It passes each group as the first argument to the function
    #
    # group_keys=False means: don't include the group name (ticker) in the result index
    #
    # LAMBDA EXPLANATION:
    #   lambda x: check_monotonic(x)
    #   This is an ANONYMOUS FUNCTION (function without a name)
    #   It's equivalent to:
    #       def temp_function(x):
    #           return check_monotonic(x)
    #   
    #   Why use lambda? It's shorter for simple operations.
    #   'x' is the parameter (each group from groupby)
    #   The lambda just calls check_monotonic() with that group
    #
    # RESULT: sorted_check is a pandas Series where:
    #   - Index = ticker names
    #   - Values = True/False (is that ticker's dates sorted?)
    sorted_check = historical_df.groupby(ticker_col, group_keys=False).apply(
        lambda x: check_monotonic(x)  # Lambda passes each group to check_monotonic
    )
    
    # .all() returns True only if ALL values in the Series are True
    # If even one ticker has unsorted dates, this will be False
    all_sorted = sorted_check.all()
    print(f"    ✓ All ticker groups properly sorted: {all_sorted}")
    
    if not all_sorted:
        # BOOLEAN INDEXING EXPLANATION:
        # ~sorted_check creates a boolean Series (True/False for each ticker)
        # The ~ operator means "NOT" (inverts True to False, False to True)
        # sorted_check[~sorted_check] filters to only keep rows where value is False
        # .index gets the ticker names
        # .tolist() converts to a Python list
        unsorted_tickers = sorted_check[~sorted_check].index.tolist()
        print(f"    ✗ Unsorted ticker groups: {len(unsorted_tickers):,} / {historical_df[ticker_col].nunique():,}")
        
        if len(unsorted_tickers) > 0:
            print(f"    First 10 unsorted tickers: {unsorted_tickers[:10]}")
            
            # Show detailed example of first unsorted ticker
            example_ticker = unsorted_tickers[0]
            example_data = historical_df[historical_df[ticker_col] == example_ticker].sort_values(date_col)
            print(f"\n    Example - {example_ticker} (first 20 rows):")
            print(f"    {'Ticker':<10} {'Date':<12} {'Date Diff':<15}")
            print(f"    {'-'*10} {'-'*12} {'-'*15}")
            prev_date = None
            for idx, row in example_data.head(20).iterrows():
                date = row[date_col]
                diff_str = ""
                if prev_date is not None:
                    diff = date - prev_date
                    if diff < pd.Timedelta(0):
                        diff_str = f"⚠️ {diff} (BACKWARDS!)"
                    else:
                        diff_str = str(diff)
                print(f"    {row[ticker_col]:<10} {str(date.date()):<12} {diff_str:<15}")
                prev_date = date
    
    # ========== CHECK FOR DUPLICATE DATES ==========
    print(f"\n[3] CHECKING FOR DUPLICATE DATES WITHIN TICKER GROUPS")
    
    # GROUPBY WITH MULTIPLE COLUMNS:
    # groupby([ticker_col, date_col]) groups by BOTH ticker AND date
    # This creates groups like: (AAPL, 2020-01-01), (AAPL, 2020-01-02), etc.
    # .size() counts how many rows are in each group
    # Result: Series with (ticker, date) as index, count as value
    duplicate_dates = historical_df.groupby([ticker_col, date_col]).size()
    
    # CHAINED OPERATIONS EXPLANATION:
    # 1. duplicate_dates[duplicate_dates > 1] - filter to counts > 1 (duplicates)
    # 2. .reset_index() - converts index to columns, creates new default index
    # 3. [ticker_col] - select just the ticker column
    # 4. .unique() - get unique ticker values (removes duplicates from list)
    tickers_with_duplicates = duplicate_dates[duplicate_dates > 1].reset_index()[ticker_col].unique()
    
    if len(tickers_with_duplicates) > 0:
        print(f"    ⚠️  Tickers with duplicate dates: {len(tickers_with_duplicates):,}")
        print(f"    Total duplicate date entries: {(duplicate_dates > 1).sum():,}")
        
        # Show example
        example_ticker = tickers_with_duplicates[0]
        example_dups = historical_df[historical_df[ticker_col] == example_ticker]
        dup_dates = example_dups[date_col].value_counts()
        dup_dates = dup_dates[dup_dates > 1]
        print(f"    Example - {example_ticker} has {len(dup_dates)} dates with duplicates")
        if len(dup_dates) > 0:
            example_date = dup_dates.index[0]
            print(f"    Date {example_date.date()} appears {dup_dates.iloc[0]} times")
    else:
        print(f"    ✓ No duplicate dates found")
    
    # ========== CHECK FOR NON-CONTIGUOUS DATE RANGES (TICKER REUSE) ==========
    print(f"\n[4] CHECKING FOR NON-CONTIGUOUS DATE RANGES (potential ticker reuse)")
    print(f"    (Looking for tickers that appear, disappear, then reappear)")
    
    def find_gaps(group, gap_threshold_days):
        """
        Find gaps in date sequences that exceed a threshold.
        
        This is a NESTED FUNCTION (defined inside another function).
        It has access to variables from the outer function (like 'date_col').
        
        Args:
            group: A pandas DataFrame containing rows for one ticker
            gap_threshold_days: Number of days that defines a "large gap"
        
        Returns:
            List of Timedelta objects representing gaps larger than threshold
        """
        # Get the date column and sort it
        dates = group[date_col].sort_values()
        
        # If there's less than 2 dates, there can't be any gaps
        if len(dates) < 2:
            return []
        
        # Calculate differences between consecutive dates
        # .diff() calculates: date[i] - date[i-1] for each date
        # The first value will be NaN (no previous date), so we drop it
        diffs = dates.diff().dropna()
        
        # Filter to only keep gaps larger than our threshold
        # This creates a boolean mask, then filters the Series
        large_gaps = diffs[diffs > pd.Timedelta(days=gap_threshold_days)]
        
        # Convert pandas Series to Python list and return
        return large_gaps.tolist()
    
    # Define multiple gap thresholds to check
    gap_thresholds = [
        (365, ">365 days (1 year)"),
        (120, ">120 days (~4 months)"),
        (60, ">60 days (~2 months)"),
        (20, ">20 days")
    ]
    
    # Check each threshold
    for threshold_days, threshold_label in gap_thresholds:
        # ===== FUNCTOOLS.PARTIAL EXPLANATION =====
        # This is a technique called "partial function application"
        # It creates a NEW function by "locking in" some parameters
        #
        # Problem: find_gaps() needs TWO parameters: (group, gap_threshold_days)
        # But .apply() only passes ONE parameter: (group)
        #
        # Solution: Use partial() to create a function that already has
        #           gap_threshold_days set, so it only needs 'group'
        #
        # Example:
        #   Original: find_gaps(group, 365)
        #   After partial: find_gaps_with_threshold(group)  # 365 is already set!
        #
        # This is like creating a specialized version of the function
        from functools import partial
        find_gaps_with_threshold = partial(find_gaps, gap_threshold_days=threshold_days)
        
        # ===== FUNCTION PASSING TO APPLY =====
        # .groupby() splits dataframe into groups (one per ticker)
        # .apply() calls find_gaps_with_threshold() for EACH group
        # group_keys=True means: keep ticker name in the result index
        #
        # RESULT: gaps_by_ticker is a Series where:
        #   - Index = ticker names
        #   - Values = lists of gap durations (or empty list if no gaps)
        gaps_by_ticker = historical_df.groupby(ticker_col, group_keys=True).apply(find_gaps_with_threshold)
        
        # ===== LIST COMPREHENSION EXPLANATION =====
        # This is a concise way to create a list by filtering/transforming
        #
        # SYNTAX: [expression for item in iterable if condition]
        #
        # BREAKDOWN:
        #   - gaps_by_ticker.items() - iterates over (ticker, gaps) pairs
        #   - ticker, gaps - unpacks each pair into two variables
        #   - if len(gaps) > 0 - only include tickers that have gaps
        #   - ticker - the value we want in our final list
        #
        # EQUIVALENT TO:
        #   tickers_with_gaps = []
        #   for ticker, gaps in gaps_by_ticker.items():
        #       if len(gaps) > 0:
        #           tickers_with_gaps.append(ticker)
        #
        # List comprehensions are more Pythonic and often faster!
        tickers_with_gaps = [ticker for ticker, gaps in gaps_by_ticker.items() if len(gaps) > 0]
        
        if len(tickers_with_gaps) > 0:
            print(f"    ⚠️  Tickers with gaps {threshold_label}: {len(tickers_with_gaps):,}")
            print(f"    First 10: {tickers_with_gaps[:10]}")
            
            # Show detailed example for the first threshold only (to avoid too much output)
            if threshold_days == 365:
                example_ticker = tickers_with_gaps[0]
                example_data = historical_df[historical_df[ticker_col] == example_ticker].sort_values(date_col)
                date_ranges = []
                current_start = example_data[date_col].iloc[0]
                prev_date = current_start
                
                # Find all date ranges separated by gaps
                for date in example_data[date_col].iloc[1:]:
                    if (date - prev_date) > pd.Timedelta(days=365):
                        date_ranges.append((current_start, prev_date))
                        current_start = date
                    prev_date = date
                date_ranges.append((current_start, prev_date))
                
                print(f"\n    Example - {example_ticker} has {len(date_ranges)} date range(s):")
                for i, (start, end) in enumerate(date_ranges, 1):
                    row_count = len(example_data[(example_data[date_col] >= start) & (example_data[date_col] <= end)])
                    print(f"      Range {i}: {start.date()} to {end.date()} ({row_count} rows)")
        else:
            print(f"    ✓ No gaps {threshold_label} found")
    
    # ========== CHECK MAPPING DF IF PROVIDED ==========
    if mapping_df is not None:
        print(f"\n[5] MAPPING DATAFRAME DIAGNOSIS")
        print(f"    Shape: {mapping_df.shape}")
        
        if 'historical_ticker' in mapping_df.columns and 'change_date' in mapping_df.columns:
            # Check sorting within historical_ticker groups
            mapping_sorted_check = mapping_df.groupby('historical_ticker', group_keys=False).apply(
                lambda x: check_monotonic(x.rename(columns={'change_date': date_col}))
            )
            mapping_all_sorted = mapping_sorted_check.all()
            print(f"    ✓ Mapping dates sorted within historical_ticker groups: {mapping_all_sorted}")
            
            if not mapping_all_sorted:
                unsorted_mapping = mapping_sorted_check[~mapping_sorted_check].index.tolist()
                print(f"    ✗ Unsorted historical_tickers: {len(unsorted_mapping):,}")
                print(f"    First 10: {unsorted_mapping[:10]}")
            
            # Check for duplicate historical_tickers with same change_date
            mapping_dups = mapping_df.groupby(['historical_ticker', 'change_date']).size()
            mapping_dups_count = (mapping_dups > 1).sum()
            if mapping_dups_count > 0:
                print(f"    ⚠️  Duplicate (historical_ticker, change_date) pairs: {mapping_dups_count:,}")
            else:
                print(f"    ✓ No duplicate (historical_ticker, change_date) pairs")
    
    # ========== FINAL RECOMMENDATION ==========
    print(f"\n[6] RECOMMENDATIONS")
    if not all_sorted:
        print(f"    ✗ CRITICAL: Data is not properly sorted. Fix before using merge_asof.")
        print(f"      → Re-sort with: df.sort_values(['{ticker_col}', '{date_col}'], kind='stable').reset_index(drop=True)")
    else:
        print(f"    ✓ Data appears to be sorted correctly")
    
    if len(tickers_with_duplicates) > 0:
        print(f"    ⚠️  Duplicate dates found. This is usually OK for merge_asof, but verify behavior.")
    
    if len(tickers_with_gaps) > 0:
        print(f"    ⚠️  Large gaps found. Ticker reuse may cause mapping issues - verify logic.")
    
    print("="*80 + "\n")


Results




