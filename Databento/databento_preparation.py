import pandas as pd
import time

# things we need to do before adding actual features 

# 1) create a single record of truth for each stock each day, taking the min/max of its
# publishers for low/high, and the open/close from the publlisher with the most volume 
# 
# 2) create a "knowledge_date" for each row, which is the date from the next bar for that 
# stock so that there is no look ahead bias
# 
# Would ideally also add here the corporate actions and split adjustments when we have the data

def aggregate_publisher_data(df: pd.DataFrame):
    """
    create a single record of truth for each stock
    
    """
    # get the row indicies with max volume for each group
    idx_max_volume = df.groupby(['ts_event', 'symbol'])['volume'].idxmax()

    # get the full rows with max volume 
    primary_exchange = df.loc[idx_max_volume][['ts_event', 'symbol', 'rtype', 'publisher_id', 'open', 'close']]

    # do regular aggregation for high, low and volume
    aggregated = df.groupby(['ts_event', 'symbol']).agg({
        'high': 'max',
        'low': 'min',       
        'volume': 'sum' 
    }).reset_index()

    # merge the primary exchange info with the aggregated data
    result = aggregated.merge(
        primary_exchange,
        on=['ts_event', 'symbol'],
        how='left'
    )

    # Reorder columns
    result = result[['ts_event', 'rtype', 'publisher_id', 'symbol', 'high', 'low', 'open', 'close', 'volume']]

    return result

# create knowledge_date, this code needs to be looked at
def knowledge_date(df: pd.DataFrame):
    """
    Create a date representing when information was actually available for trading decisions
    """
    df = df.sort_values(['instrument_id', 'ts_event'])
    
    # Shift the date forward by one trading day
    # Use the next row's date as the "knowledge date" for current row's data
    df['knowledge_date'] = df.groupby('instrument_id')['ts_event'].shift(-1)
    
    # For the last row of each stock, manually set knowledge_date
    # (though you'll likely drop these rows anyway due to missing future data)
    df['knowledge_date'] = df['knowledge_date'].fillna(df['ts_event'] + pd.Timedelta(days=1))
    
    return df 


def main():
    df = pd.read_csv(r"C:\Users\carso\Development\emerytrading\Data\Stocks\test_data_complete.csv")
    print(df)
    row_count = len(df)

    start = time.time()
    result = aggregate_publisher_data(df)
    end = time.time()

    time_taken = end - start # already in seconds
    time_per_row = time_taken / row_count
    time_per_thousand_rows = time_per_row * 1000
    prod_estimate = time_per_thousand_rows * 30000
    prod_in_minutes = prod_estimate / 60

    print(result)
    print(f"Time taken: {time_taken:.4f} seconds for {row_count} rows")
    print(f"\nTime per row: {time_per_row:.6f} seconds")
    print(f"Time for 1,000 rows: {time_per_thousand_rows:.4f} seconds")
    print(f"\nEstimated time for 30M rows: {prod_estimate:.2f} seconds ({prod_in_minutes:.2f} minutes)")
    print()


if __name__ == "__main__":
    main()