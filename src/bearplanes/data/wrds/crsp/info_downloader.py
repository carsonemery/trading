"""Download CRSP reference tables (no date partitioning) from WRDS."""

from pathlib import Path

from bearplanes.data.wrds.client import WRDSClient
from bearplanes.data.wrds.crsp.query_string_enum import CSRPQueryStrings


def download_crsp_info(output_dir: Path, table_name: str) -> None:
    """Download a CRSP reference table in a single pull.

    Uses the CSRPQueryStrings enum. No year loop; these tables are not
    date-partitioned.

    Args:
        output_dir: Directory to save parquet file.
        table_name: The table identifier (must match an enum branch below).

    Accepts the following as table_name:
        crspq.stksecurityinfohdr -> stock header / security info
        crspq.wrds_names_query -> names
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with WRDSClient() as db:
        print("Connected to WRDS\n")
        print(f"Downloading {table_name} to {output_dir}...")

        if table_name == "crspq.stksecurityinfohdr":
            query_string = CSRPQueryStrings.SEC_INFO.value
        elif table_name == "crspq.wrds_names_query":
            query_string = CSRPQueryStrings.NAMES.value
        else:
            raise ValueError(f"Unsupported table_name: {table_name}")

        try:
            df = db.raw_sql(query_string)

            output_file = output_dir / f"{table_name}_raw.parquet"
            df.to_parquet(output_file, compression='snappy', index=False)

            file_size_mb = output_file.stat().st_size / 1024 / 1024
            rows = len(df)

            print(f"{rows:,} rows, {file_size_mb:.1f} MB")

        except Exception as e:
            print(f"Error - {e}")
