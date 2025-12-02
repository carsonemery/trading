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

OUTPUT_DIR = Path("data/compustat_fundamentals_q")
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

    table = 'fundq'
    
    for year in range(START_YEAR, END_YEAR):
        print(f"Downloading {year}...")

        sample_query = f"""
        SELECT 
        {','.join(field_list)} 
        FROM comp.{table}
        WHERE YYYYMMDD >= '{year}0101'
            AND YYYYMMDD < '{year+1}0101'
            AND datafmt = 'STD'
            AND consol = 'C'
            AND indfmt = 'INDL'
            AND indfmt = 'FS'
        ORDER BY YYYYMMDD, permno
        """

    try:
        df = db.raw_sql(sample_query)

        # save to parquet file
        output_file = OUTPUT_DIR / f"{table}_{year}.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)

        # print file size info
        file_size_mb: float = output_file.stat().st_size / 1024 / 1024
        rows: int = len(df)

        print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")

        print("\nCompustat Fundamentals")
        print(df)
    except Exception as e:
        print(f"\nFailed: {e}")


