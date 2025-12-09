import os
import wrds
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

with wrds.Connection(wrds_username=wrds_username) as db:
    # Download the CRSP-Compustat link table
    ccm_link = db.raw_sql("""
        SELECT gvkey, lpermno as permno, lpermco as permco,
               linktype, linkprim, linkdt, linkenddt
        FROM crsp.ccmxpf_lnkhist
        WHERE linktype IN ('LC', 'LU', 'LS')  -- Primary links only
          AND linkprim IN ('P', 'C')           -- Primary securities only
    """)

    # Save the linking table
    OUTPUT_DIR: Path = Path(__file__).parent.parent / "Data" / "crsp_compustat_link"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    output_file = OUTPUT_DIR / f"ccm_link.parquet"
    ccm_link.to_parquet(output_file, compression='snappy', index=False)

    print(ccm_link)
    print(len(ccm_link))
    print(ccm_link.columns)
