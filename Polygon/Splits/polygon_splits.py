from polygon import RESTClient
import os
from dotenv import load_dotenv
from polygon.exceptions import BadResponse
import pandas as pd

load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

# tickers_list = ['META', 'BLL', 'BALL',
#                'FB', 'AI', 'T', 'PTWO',
#                'SBC', 'TWX', 'AOL',
#                'WBD', 'HWP', 'HPQ', 'AAPL']

tickers_list = ['TSLA']

def get_splits(tickers_list: []) -> []:
    """
    """
    # Create a list of splits to store the splits, elements will be dictionaries
    
    splits = []

    for ticker in tickers_list:
        for split in client.list_splits(
            ticker = ticker,
            order = "desc",
            limit = "1000",
            sort = "execution_date",
        ):
            splits.append(split)

    return splits


def main():
    print("working")

    list_of_splits = get_splits(tickers_list)

    print(list_of_splits)

    for split in list_of_splits:
        print(split)


if __name__ == "__main__":
    main()
