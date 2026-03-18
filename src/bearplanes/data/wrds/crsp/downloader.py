"""Download CRSP daily stock file data from WRDS."""

from bearplanes.data.wrds.crsp.query_string_enum import CSRPQueryStrings
from pathlib import Path

from bearplanes.data.wrds.client import WRDSClient

def download_crsp_dsf(
    start_year: int,
    end_year: int,
    output_dir: Path,
    table_name: str
) -> None:
    """Downloads data from the CRSP family of tables a year at a time.
    Uses the CRSPQueryStrings ENUM for extendability
    
    Args:
        start_year: Starting year (inclusive).
        end_year: Ending year (inclusive).
        output_dir: Directory to save parquet files.
        table_name: The table name we are querying from.

    Accepts the following as table_name:
        crspq.dsf_v2 -> daily stock data
        crspq.wrds_dailyindexret -> indicies
        crspq.stkdistributions -> distributions
        crspq.stkdelists -> delistings
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with WRDSClient() as db:
        print("Connected to WRDS\n")
        
        for year in range(start_year, end_year + 1):
            print(f"Downloading {year} from {table_name} to {output_dir}...")
            
            if table_name == "crspq.dsf_v2":
                query_string = CSRPQueryStrings.DAILY_DATA.value.format(year=year, next_year=year + 1)
            elif table_name == "crspq.wrds_dailyindexret":
                query_string = CSRPQueryStrings.INDICIES.value.format(year=year, next_year=year + 1)
            elif table_name == "crspq.stkdistributions":
                query_string = CSRPQueryStrings.DISTRIBUTIONS.value.format(year=year, next_year=year + 1)
            elif table_name == "crspq.stkdelists":
                query_string = CSRPQueryStrings.DELISTINGS.value.format(year=year, next_year=year + 1)
            else:
                raise ValueError(f"Unsupported table_name: {table_name}")

            try:
                df = db.raw_sql(query_string)
                
                output_file: Path = output_dir / f"{table_name}_raw_{year}.parquet"
                df.to_parquet(output_file, compression='snappy', index=False)
            
                # File size info
                file_size_mb: float = output_file.stat().st_size / 1024 / 1024
                print(f"{year}: {file_size_mb:.1f} MB")
                
            except Exception as e:
                print(f"{year}: Error - {e}")

