"""Download CRSP daily stock file data from WRDS."""

from pathlib import Path

import pandas as pd

from bearplanes.data.wrds.client import WRDSClient


def download_crsp_dsf(
    start_year: int,
    end_year: int,
    output_dir: Path
) -> None:
    """Download CRSP daily stock file data.
    
    Args:
        start_year: Starting year (inclusive).
        end_year: Ending year (inclusive).
        output_dir: Directory to save parquet files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with WRDSClient() as db:
        print("Connected to WRDS\n")
        
        for year in range(start_year, end_year + 1):
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
                output_file: Path = output_dir / f"crsp_dsf_{year}.parquet"
                df.to_parquet(output_file, compression='snappy', index=False)
                
                # print file size info
                file_size_mb: float = output_file.stat().st_size / 1024 / 1024
                rows: int = len(df)
                
                print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")
                
            except Exception as e:
                print(f"{year}: Error - {e}")

