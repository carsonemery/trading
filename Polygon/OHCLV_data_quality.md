================================================================================
DATA DIAGNOSIS FOR MERGE_ASOF
================================================================================

[1] BASIC INFORMATION
    Shape: (23408074, 9)
    Date dtype: datetime64[ns]
    Ticker dtype: object
    Date range: 2016-01-04 00:00:00 to 2025-10-24 00:00:00
    NaT/NaN dates: 0
    Unique tickers: 23,621

[2] CHECKING SORT ORDER WITHIN TICKER GROUPS
    (merge_asof requires dates to be sorted within each ticker group)
c:\Users\carso\Development\emerytrading\Polygon\Ticker Change Events\polygon_symbol_mapping.py:242: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  sorted_check = historical_df.groupby(ticker_col, group_keys=False).apply(
    ✓ All ticker groups properly sorted: True

[3] CHECKING FOR DUPLICATE DATES WITHIN TICKER GROUPS
    ⚠️  Tickers with duplicate dates: 28
    Total duplicate date entries: 28
    Example - AMUB has 1 dates with duplicates
    Date 2019-08-13 appears 2 times

[4] CHECKING FOR NON-CONTIGUOUS DATE RANGES (potential ticker reuse)
    (Looking for tickers that appear, disappear, then reappear)
c:\Users\carso\Development\emerytrading\Polygon\Ticker Change Events\polygon_symbol_mapping.py:389: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  gaps_by_ticker = historical_df.groupby(ticker_col, group_keys=True).apply(find_gaps_with_threshold)
    ⚠️  Tickers with gaps >365 days (1 year): 667
    First 10: ['AAC', 'ACAC', 'ACACW', 'ACET', 'ACI', 'ACIC', 'ACII', 'ACPr', 'ACPrw', 'ACSG']

    Example - AAC has 2 date range(s):
      Range 1: 2016-01-04 to 2019-10-25 (961 rows)
      Range 2: 2021-03-25 to 2023-11-06 (660 rows)
c:\Users\carso\Development\emerytrading\Polygon\Ticker Change Events\polygon_symbol_mapping.py:389: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  gaps_by_ticker = historical_df.groupby(ticker_col, group_keys=True).apply(find_gaps_with_threshold)
    ⚠️  Tickers with gaps >120 days (~4 months): 961
    First 10: ['AAC', 'AACI', 'AACIU', 'AACIW', 'ACABU', 'ACAC', 'ACACU', 'ACACW', 'ACET', 'ACI']
c:\Users\carso\Development\emerytrading\Polygon\Ticker Change Events\polygon_symbol_mapping.py:389: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  gaps_by_ticker = historical_df.groupby(ticker_col, group_keys=True).apply(find_gaps_with_threshold)
    ⚠️  Tickers with gaps >60 days (~2 months): 1,293
    First 10: ['AAC', 'AACI', 'AACIU', 'AACIW', 'AAM.U', 'AAMC', 'ACABU', 'ACAC', 'ACACU', 'ACACW']
c:\Users\carso\Development\emerytrading\Polygon\Ticker Change Events\polygon_symbol_mapping.py:389: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  gaps_by_ticker = historical_df.groupby(ticker_col, group_keys=True).apply(find_gaps_with_threshold)
    ⚠️  Tickers with gaps >20 days: 2,870
    First 10: ['AAC', 'AACI', 'AACIU', 'AACIW', 'AACT.U', 'AAM.U', 'AAMC', 'ABEOW', 'ABLVW', 'ACABU']

[6] RECOMMENDATIONS
    ✓ Data appears to be sorted correctly
    ⚠️  Duplicate dates found. This is usually OK for merge_asof, but verify behavior.
    ⚠️  Large gaps found. Ticker reuse may cause mapping issues - verify logic.
================================================================================