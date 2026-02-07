"""TAQ (Trade and Quote) data query functions."""

from typing import List, Optional

import pandas as pd
import datetime

from bearplanes.data.wrds.client import WRDSClient

def query_taq_quotes(
    date: str,
    symbols: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """Query TAQ consolidated quotes (CQM) data.
    
    Args:
        date: Date in YYYYMMDD format (e.g., '20240104'). **DATE MUST BE IN YYYMMDD FORMAT**
        symbols: List of ticker symbols to filter. If None, returns all symbols.
        limit: Maximum number of rows to return. If None, returns all matching rows.
        
    Returns:
        DataFrame with quote data (date, time, symbol, bid, ask, sizes).
    
    """
    with WRDSClient() as db:
        # Build WHERE clause if symbols provided
        if symbols:
            symbol_list = ",".join(f"'{s}'" for s in symbols)
            where_clause = f"WHERE sym_root IN ({symbol_list})"
        else:
            where_clause = ""
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        query = f"""
        SELECT date, time_m, sym_root, bid, ask, bidsiz, asksiz
        FROM taqmsec.cqm_{date}
        {where_clause}
        {limit_clause}
        """
        
        return db.raw_sql(query)

def query_taq_trades(
    date: str,
    symbols: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """Query TAQ consolidated trades (CTM) data.
    
    Args:
        date: Date in YYYYMMDD format (e.g., '20240104').
        symbols: List of ticker symbols to filter. If None, returns all symbols.
        limit: Maximum number of rows to return. If None, returns all matching rows.
        
    Returns:
        DataFrame with trade data.
    """
    with WRDSClient() as db:
        # Build WHERE clause if symbols provided
        if symbols:
            symbol_list = ",".join(f"'{s}'" for s in symbols)
            where_clause = f"WHERE sym_root IN ({symbol_list})"
        else:
            where_clause = ""
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        query = f"""
        SELECT *
        FROM taqmsec.ctm_{date}
        {where_clause}
        {limit_clause}
        """
        
        return db.raw_sql(query)

def get_trading_dates(year: int) -> List[datetime]:
    """
    Get all trading dates for a given year from CRSP.
    This ensures we only query dates where markets were actually open.

    This method is required as we cannot query by whole year periods
    
    Args:
        year: Year to get trading dates for (e.g., 2024)
    
    Returns:
        List of datetime objects for each trading day
    """
    with WRDSClient() as db:
        query = f"""
        SELECT DISTINCT date
        FROM crsp.dsi
        WHERE EXTRACT(YEAR FROM date) = {year}
        ORDER BY date
        """
        
        dates_df = db.raw_sql(query, date_cols=['date'])
        return dates_df['date'].tolist()

def query_taq_quotes_single_day(
    date: datetime,
    symbols: Optional[List[str]] = None,
    db_connection = None
) -> pd.DataFrame:
    
    table_suffix = date.strftime('%Y%m%d')
    table_name = f'taqmsec.cqm_{table_suffix}'
    
    if symbols:
        symbol_list = ",".join(f"'{s}'" for s in symbols)
        symbol_filter = f"AND sym_root IN ({symbol_list})"
    else:
        symbol_filter = ""
    
    query = f"""
    WITH quotes_with_buckets AS (
        SELECT 
            '{date.strftime('%Y-%m-%d')}'::date as date,
            time_m,
            sym_root,
            COALESCE(sym_suffix, '') as sym_suffix,
            bid,
            ask,
            qu_seqnum,
            
            -- Use TIME comparisons
            CASE 
                WHEN time_m <= '10:30:00'::time THEN '10:30:00'::time
                WHEN time_m <= '11:00:00'::time THEN '11:00:00'::time
                WHEN time_m <= '11:30:00'::time THEN '11:30:00'::time
                WHEN time_m <= '12:00:00'::time THEN '12:00:00'::time
                WHEN time_m <= '12:30:00'::time THEN '12:30:00'::time
                WHEN time_m <= '13:00:00'::time THEN '13:00:00'::time
                WHEN time_m <= '13:30:00'::time THEN '13:30:00'::time
                WHEN time_m <= '14:00:00'::time THEN '14:00:00'::time
                WHEN time_m <= '14:30:00'::time THEN '14:30:00'::time
                WHEN time_m <= '15:00:00'::time THEN '15:00:00'::time
                WHEN time_m <= '15:30:00'::time THEN '15:30:00'::time
                WHEN time_m <= '16:00:00'::time THEN '16:00:00'::time
                ELSE NULL
            END AS time_bucket,
            
            ROW_NUMBER() OVER (
                PARTITION BY 
                    sym_root, 
                    COALESCE(sym_suffix, ''),
                    CASE 
                        WHEN time_m <= '10:30:00'::time THEN '10:30:00'::time
                        WHEN time_m <= '11:00:00'::time THEN '11:00:00'::time
                        WHEN time_m <= '11:30:00'::time THEN '11:30:00'::time
                        WHEN time_m <= '12:00:00'::time THEN '12:00:00'::time
                        WHEN time_m <= '12:30:00'::time THEN '12:30:00'::time
                        WHEN time_m <= '13:00:00'::time THEN '13:00:00'::time
                        WHEN time_m <= '13:30:00'::time THEN '13:30:00'::time
                        WHEN time_m <= '14:00:00'::time THEN '14:00:00'::time
                        WHEN time_m <= '14:30:00'::time THEN '14:30:00'::time
                        WHEN time_m <= '15:00:00'::time THEN '15:00:00'::time
                        WHEN time_m <= '15:30:00'::time THEN '15:30:00'::time
                        WHEN time_m <= '16:00:00'::time THEN '16:00:00'::time
                    END
                ORDER BY time_m DESC, qu_seqnum DESC
            ) AS rn
            
        FROM {table_name}
        WHERE time_m >= '10:00:00'::time
          AND time_m <= '16:00:00'::time
          AND natbbo_ind IN ('1', '2', '4')
          AND qu_cancel IS NULL
          AND bid > 0 AND ask > 0
          AND bid < ask
          {symbol_filter}
    )
    
    SELECT 
        date,
        sym_root,
        sym_suffix,
        time_bucket,
        bid,
        ask,
        (bid + ask) / 2.0 AS midpoint,
        
        CASE time_bucket::text
            WHEN '10:30:00' THEN 1
            WHEN '11:00:00' THEN 2
            WHEN '11:30:00' THEN 3
            WHEN '12:00:00' THEN 4
            WHEN '12:30:00' THEN 5
            WHEN '13:00:00' THEN 6
            WHEN '13:30:00' THEN 7
            WHEN '14:00:00' THEN 8
            WHEN '14:30:00' THEN 9
            WHEN '15:00:00' THEN 10
            WHEN '15:30:00' THEN 11
            WHEN '16:00:00' THEN 12
        END AS period
        
    FROM quotes_with_buckets
    WHERE rn = 1
      AND time_bucket IS NOT NULL
    ORDER BY sym_root, period
    """
    
    if db_connection:
        df = db_connection.raw_sql(query)
    else:
        with WRDSClient() as db:
            df = db.raw_sql(query)
    
    return df

