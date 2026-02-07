"""Download CRSP stock distribution events (dividends, splits, etc.) from WRDS."""

from pathlib import Path

import pandas as pd

from bearplanes.data.wrds.client import WRDSClient


def download_distributions(
    start_year: int,
    end_year: int,
    output_dir: Path
) -> None:
    """Download CRSP stock distribution events.
    
    Args:
        start_year: Starting year (inclusive).
        end_year: Ending year (inclusive).
        output_dir: Directory to save parquet files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with WRDSClient() as db:
        print("Connected to WRDS\n")
        
        table = 'stkdistributions'
        
        for year in range(start_year, end_year + 1):
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
                output_file = output_dir / f"{table}_{year}.parquet"
                df.to_parquet(output_file, compression='snappy', index=False)

                # print file size info
                file_size_mb: float = output_file.stat().st_size / 1024 / 1024
                rows: int = len(df)

                print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")
                
            except Exception as e:
                print(f"\nFailed: {e}")