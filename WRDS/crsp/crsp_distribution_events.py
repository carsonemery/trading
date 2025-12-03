import os
import wrds
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from pathlib import Path

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

OUTPUT_DIR = Path(__file__).parent.parent.parent / "Data" / "crsp_distribution_events"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = 2010
END_YEAR = 2024

with wrds.Connection(wrds_username=wrds_username) as db:
    print("Connected to WRDS\n")
    
    table = 'stkdistributions'
    
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"Downloading {year}...")

        query_string = f"""
        SELECT *
        FROM crspq.{table}
        WHERE disexdt >= '{year}-01-01'
            AND disexdt < '{year + 1}-01-01'
        """

        try:
            df = db.raw_sql(query_string)

            # save to parquet file
            output_file = OUTPUT_DIR / f"{table}_{year}.parquet"
            df.to_parquet(output_file, compression='snappy', index=False)

            # print file size info
            file_size_mb: float = output_file.stat().st_size / 1024 / 1024
            rows: int = len(df)

            print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")
            
        except Exception as e:
            print(f"\nFailed: {e}")