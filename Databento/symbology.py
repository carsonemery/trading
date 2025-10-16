import pandas as pd

def main():
    df = pd.read_csv(r"C:\Users\carso\Development\emerytrading\Data\Stocks\test_data_complete.csv")
    # print(df.head())

    symbol_id_map = df.groupby('symbol')['instrument_id'].nunique()
    multiple_ids = symbol_id_map[symbol_id_map > 1]
    print("Symbols with multiple IDs in sample", len(multiple_ids))


if __name__ == "__main__":
    main()