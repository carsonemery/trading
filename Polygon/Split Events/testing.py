import pandas as pd
import os
import sys
from polygon_split_mapping import map_splits

def main():

    # Pull in OHLCV ticker change mapped data 
    ticker_mapped_parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26_MAPPED.parquet'
    historical_data = pd.read_parquet(ticker_mapped_parquet_path)

    # Pull in split data
    split_parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_SPLITS_2016_2025.parquet'
    split_info = pd.read_parquet(split_parquet_path)

    # Call map_splits to map splits and return a split adjusted dataframe 
    split_adjusted = map_splits(split_info, historical_data)
    print(split_adjusted)

    # Save split_adjusted dataframe to parquet file, this should mark the end of the data OHLCV data acquisition and normalization process (for now)
    adjusted_parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26_MAPPED.csv'
    split_adjusted.to_parquet(adjusted_parquet_path)

if __name__ == "__main__":
    main()