"""Download Compustat company information data from WRDS."""

from pathlib import Path

import pandas as pd

from bearplanes.data.wrds.client import WRDSClient
from bearplanes.data.wrds.compustat.fields import fields as field_list


def download_company_info(
    start_year: int,
    end_year: int,
    output_dir: Path,
    fields: list = None
) -> None:
    """Download Compustat company information data.
    
    Args:
        start_year: Starting year (inclusive).
        end_year: Ending year (inclusive).
        output_dir: Directory to save parquet files.
        fields: List of field names to retrieve. If None, uses default field_list.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    fields_to_use = fields or field_list
    
    with WRDSClient() as db:

        # Primary Compustat library:
        # comp  -----> Compustat North America
        
        # Key Compustat tables in comp:
        # comp.company   -----> Company information
        # comp.names     -----> Company names and identifiers

        table = 'company'
        
        for year in range(start_year, end_year + 1):
            print(f"Downloading {year}...")

            query_string = f"""
            SELECT 
            {','.join(fields_to_use)} 
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
                output_file = output_dir / f"{table}_info_{year}.parquet"
                df.to_parquet(output_file, compression='snappy', index=False)

                # print file size info
                file_size_mb: float = output_file.stat().st_size / 1024 / 1024
                rows: int = len(df)

                print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")
                
            except Exception as e:
                print(f"\nFailed: {e}")