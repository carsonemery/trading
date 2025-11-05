import pandas as pd

def add_datetime(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert nanosecond unix timestamp to pandas datetime object.
    Keeps as pandas datetime64[ns] (normalized to date) for pandas operations 
    and proper comparison with events/splits dates.
    """
    # Convert to pandas datetime and normalize to date (removes time component)
    # Keeping as pandas datetime (not Python date) for pandas operations like .dt.year
    df['date'] = pd.to_datetime(df['window_start'], unit='ns').dt.normalize()
    
    # Reorder columns to put 'date' first
    cols = ['date'] + [col for col in df.columns if col != 'date']
    return df[cols]


# async def checkpoint_namechange_data(
#     data_type: str,
#     data: str,
#     filepath: str, 
#     mode: str
#     ) -> None:
#     """
#     Checkpoints data for the name change events endpoint
#     needs aiofiles and json imports

#         Args:
#         data_type: "events" (list of name change event dictionaries), "failed" (list of strings), 
#                   or "processed" (set of strings)
#         data: The data to checkpoint
#         filepath: Base directory path for checkpoint files
#         mode: "append" for JSONL files, "overwrite" for JSON files
#     """

#     # Determine the file path and format based on data_type
#     if data_type == "events":
#         file_path = os.makedirs(os.path.join(filepath, "events.json")
    
#     elif data_type == "failed":
#         file_path = os.path.join(filepath, "failed_tickers.json")

#     elif data_type == "processed":
#         file_path = os.path.join(filepath, "processed_tickers.json")



#       os.makedirs(os.path.dirname(job.path), exist_ok = True)
