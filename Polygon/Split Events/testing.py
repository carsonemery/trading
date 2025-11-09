import pandas as pd
import os
import sys
from polygon_split_mapping import map_splits

def main():

    # pull in adjusted_ticker mapped OHLCV data 
    csv_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26.csv'
    historical_data = pd.read_csv(csv_path) # If I want to change the range I need to change the file path or delete the file

    # pull in split data
    parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_SPLITS_2016_2025.parquet'
    split_info = pd.read_parquet(parquet_path)


    split_adjusted = map_splits(split_info, historical_data)

    # Save split_adjusted dataframe to parquet file, this should mark the end of the data OHLCV data acquisition and normalization process (for now)

    


if __name__ == "__main__":
    main()