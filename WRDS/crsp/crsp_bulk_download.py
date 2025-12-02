import os
import wrds
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from pathlib import Path

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

OUTPUT_DIR = Path("data/crsp")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = 2010
END_YEAR = 2022

with wrds.Connection(wrds_username=wrds_username) as db:
    print("Connected to WRDS\n")
    
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"Downloading {year}...")
        
        # Query for entire year
        full_query = f"""
        SELECT *
        FROM crspq.dsf_v2
        WHERE YYYYMMDD >= '{year}0101'
          AND YYYYMMDD < '{year + 1}0101'
        ORDER BY YYYYMMDD, permno
        """

        try:
            df = db.raw_sql(full_query)
            
            # Save to parquet
            output_file: Path = OUTPUT_DIR / f"crsp_dsf_{year}.parquet"
            df.to_parquet(output_file, compression='snappy', index=False)
            
            # print file size info
            file_size_mb: float = output_file.stat().st_size / 1024 / 1024
            rows: int = len(df)
            
            print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")
            
        except Exception as e:
            print(f"{year}: Error - {e}")

