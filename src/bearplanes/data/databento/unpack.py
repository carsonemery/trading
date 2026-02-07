import os

import pandas as pd
from databento import DBNStore

folder_path = r""

# gets all files including json files
def get_files():

    files = []
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path):
            files.append(item_path)
    return files

def process_databento_zstfiles():
    
    # get the list of files
    files = get_files()
    # collect all dataframes in a list
    dataframes = []

    # Process each downloaded file
    for file in files:

        # Skip non-DBN files (like metadata.json)
        if file.endswith('.zst'):
            file_data = DBNStore.from_file(file).to_df()
            file_data = file_data.reset_index()
            dataframes.append(file_data)
        else:
            continue

    # concatenate all dataframes at once 
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else: 
        raise ValueError("something went wrong - no .zst files found")

def convert_to_csv():
    test_data = process_databento_zstfiles()

