from pandas.core.internals.construction import _list_of_dict_to_arrays
from polygon import RESTClient
import os
from dotenv import load_dotenv

load_dotenv()
client = RESTClient(os.getenv("POLYGON_API_KEY"))

tickers_list = ['META', 'TSLA', 'AAPL', 'BALL']

def get_events(tickers_list: []) -> []:
    list_of_events = []

    for ticker in tickers_list:
        events = client.get_ticker_events(ticker)
        list_of_events.append(events)

    return list_of_events
        
def display_events(tickers_list): 
    list_of_events = get_events(tickers_list)
    
    for event in list_of_events:
        print(event)
        print(f"event type: {type(event)}")


def main():
    display_events(tickers_list)
      
if __name__ == "__main__":
      main()






