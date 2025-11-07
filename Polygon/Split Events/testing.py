import pandas as pd
# from polygon_split_mapping import 


def main():

    # pull in OHLCV data for a select number of stocks to test
    csv_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\OHLCV_Historical_2016-01-01_to_2025-10-26.csv'
    test_df = pd.read_csv(csv_path) # If I want to change the range I need to change the file path or delete the file

    parquet_path = r'C:\Users\carso\Development\emerytrading\Data\Stocks\Polygon\test_data_SPLITS_2016_2025.parquet'
    test_splits = pd.read_parquet(parquet_path)







if __name__ == "__main__":
    main()