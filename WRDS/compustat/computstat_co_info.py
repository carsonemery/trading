from dataclasses import fields
import os
from pandas.core.groupby.groupby import OutputFrameOrSeries
import wrds
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

# Pull in the fields list
from compustat_fields_utility import field_list

# Save to Data/ folder at project root (not relative to cwd)
OUTPUT_DIR = Path(__file__).parent.parent.parent / "Data" / "compustat_fundamentals_"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = 2010
END_YEAR = 2024

# Initialize a connection object using with context manager
with wrds.Connection(wrds_username=wrds_username) as db:

    # Primary Compustat library:
    # comp  -----> Compustat North America
    
    # Key Compustat tables in comp:
    # comp.funda     -----> Fundamentals Annual (yearly financial statements)
    # comp.fundq     -----> Fundamentals Quarterly (quarterly financial statements)
    # comp.company   -----> Company information
    # comp.names     -----> Company names and identifiers

    table = 'company'
    
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"Downloading {year}...")

        query_string = f"""
        SELECT 
        {','.join(field_list)} 
        FROM comp.{table}
        WHERE datadate >= '{year}0101'
            AND datadate < '{year+1}0101'
            AND datafmt = 'STD'
            AND consol = 'C'
            AND indfmt IN ('FS', 'INDL')
        """

        try:
            df = db.raw_sql(query_string)

            # save to parquet file
            output_file = OUTPUT_DIR / f"{table}_info_{year}.parquet"
            df.to_parquet(output_file, compression='snappy', index=False)

            # print file size info
            file_size_mb: float = output_file.stat().st_size / 1024 / 1024
            rows: int = len(df)

            print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")
            
        except Exception as e:
            print(f"\nFailed: {e}")