import os
import wrds
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

# Connect to the Database
connection = wrds.Connection(wrds_username=wrds_username)

# Get the libraries we have available
libraries = connection.list_libraries()

# Get the list of crsp tables we have avaiable
crsps_tables = connection.list_tables(library='crsp')

# Get the tables with dfs in their names
daily_stock_tables = [table for table in crsps_tables if 'dsf' in table ]

print(daily_stock_tables)


# # stock_data = connection.describe_table('crsp', 'wrds_dsf62v2_query')
whats_working = []


for table in daily_stock_tables:
    print(f"Testing table: {table}...")
    try:
        # Try to fetch just 1 row to test permissions
        stock_data = connection.get_table('crsp', table, obs=1)
        
        # If we get here, it worked
        print(f"  SUCCESS: {table}")
        whats_working.append(table)
        
    except Exception as e:
        # Catch specific error and print it
        print(f"  FAILED: {table}")
        # print(f"    Reason: {e}") # Uncomment to see full error details

print("\nSummary of working tables:")
print(whats_working)




print(stock_data)

# # results = connection.get_table('crsp', 'wrds_dsf62v2_query', obs=100)
# results = connection.get_table('crsp', 'dfs', obs=100)

# print(results.head())


connection.close()




