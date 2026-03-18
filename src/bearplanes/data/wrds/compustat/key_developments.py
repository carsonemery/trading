"""Download Capital IQ Key Developments data from WRDS."""

from pathlib import Path

from bearplanes.data.wrds.client import WRDSClient


def download_ciq_key_developments(
    start_year: int,
    end_year: int,
    output_dir: Path,
) -> None:
    """Download Capital IQ Key Developments data.

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

            query = f"""
            SELECT *
            FROM ciq.wrds_keydev
            WHERE announcedate >= '{year}-01-01'
              AND announcedate < '{year + 1}-01-01'
            ORDER BY announcedate
            """

            try:
                df = db.raw_sql(query)

                output_file = output_dir / f"ciq_keydev_raw_{year}.parquet"
                df.to_parquet(output_file, compression='snappy', index=False)

                file_size_mb = output_file.stat().st_size / 1024 / 1024
                rows = len(df)

                print(f"{year}: {rows:,} rows, {file_size_mb:.1f} MB")

            except Exception as e:
                print(f"{year}: Error - {e}")
