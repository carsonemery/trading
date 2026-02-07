"""Download CRSP-Compustat linking table from WRDS."""

from pathlib import Path

import pandas as pd

from bearplanes.data.wrds.client import WRDSClient


def download_crsp_compustat_link(output_dir: Path = None) -> pd.DataFrame:
    """Download the CRSP-Compustat linking table.
    
    This table maps CRSP PERMNOs to Compustat GVKEYs, allowing you to link
    securities across both databases.
    
    Args:
        output_dir: Directory to save the linking table.
        
    Returns:
        DataFrame with CRSP-Compustat linkages.
    """
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    with WRDSClient() as db:
        # Download the CRSP-Compustat link table
        ccm_link = db.raw_sql("""
            SELECT gvkey, lpermno as permno, lpermco as permco,
                   linktype, linkprim, linkdt, linkenddt, liid as iid
            FROM crsp.ccmxpf_lnkhist
            WHERE linktype IN ('LC', 'LU', 'LS')  -- Primary links only
              AND linkprim IN ('P', 'C', 'J')           -- Primary securities + Joiner/Secondary
        """)

        if output_dir:
            # Save the linking table
            output_file = output_dir / "ccm_link.parquet"
            ccm_link.to_parquet(output_file, compression='snappy', index=False)

            print(f"Downloaded {len(ccm_link):,} linkages")
            print(f"Columns: {', '.join(ccm_link.columns)}")
            return ccm_link
        else:
            return ccm_link
    