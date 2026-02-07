"""WRDS (Wharton Research Data Services) connection client.

This module provides a centralized client for connecting to WRDS databases.
"""

import os
from typing import Optional

import pandas as pd
import wrds

from bearplanes.utils.config import get_wrds_credentials


class WRDSClient:
    """WRDS connection client with context manager support.
    
    This class handles all WRDS connection setup, including credentials
    and environment configuration. Use it as a context manager to ensure
    proper connection cleanup.
    
    Examples:
        >>> with WRDSClient() as db:
        ...     df = db.raw_sql("SELECT * FROM comp.fundq LIMIT 10")
        
        >>> # Or manually manage connection
        >>> client = WRDSClient()
        >>> db = client.connect()
        >>> df = db.raw_sql("SELECT * FROM comp.fundq LIMIT 10")
        >>> client.close()
    """
    
    def __init__(self, username: Optional[str] = None):
        """Initialize WRDS client.
        
        Args:
            username: WRDS username. If None, reads from environment via config.
        """
        self._connection = None
        
        # Get credentials
        credentials = get_wrds_credentials()
        self.username = credentials["username"]
        self.pgpass_path = credentials["pgpass_path"]
        
        # Set PGPASSFILE environment variable for passwordless connection
        if self.pgpass_path:
            os.environ['PGPASSFILE'] = self.pgpass_path
    
    def connect(self) -> wrds.Connection:
        """Establish connection to WRDS.
        
        Returns:
            Active WRDS connection object.
            
        Raises:
            Exception: If connection fails.
        """
        if self._connection is None:
            self._connection = wrds.Connection(wrds_username=self.username)
        return self._connection
    
    def close(self):
        """Close the WRDS connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
    
    def __enter__(self):
        """Context manager entry - establish connection."""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()
        return False
    
    def raw_sql(self, query: str, **kwargs) -> pd.DataFrame:
        """Execute raw SQL query and return DataFrame.
        
        Convenience method that ensures connection is established.
        
        Args:
            query: SQL query string.
            **kwargs: Additional arguments passed to wrds.Connection.raw_sql().
            
        Returns:
            Query results as DataFrame.
        """
        db = self.connect()
        return db.raw_sql(query, **kwargs)
    
    def get_table(self, library: str, table: str, **kwargs) -> pd.DataFrame:
        """Get entire table from WRDS.
        
        Convenience method that ensures connection is established.
        
        Args:
            library: WRDS library name (e.g., 'comp', 'crsp').
            table: Table name (e.g., 'fundq', 'dsf').
            **kwargs: Additional arguments passed to wrds.Connection.get_table().
            
        Returns:
            Table data as DataFrame.
        """
        db = self.connect()
        return db.get_table(library=library, table=table, **kwargs)
    
    def list_libraries(self) -> list:
        """List available WRDS libraries.
        
        Returns:
            List of library names.
        """
        db = self.connect()
        return db.list_libraries()
    
    def list_tables(self, library: str) -> list:
        """List tables in a WRDS library.
        
        Args:
            library: WRDS library name.
            
        Returns:
            List of table names.
        """
        db = self.connect()
        return db.list_tables(library=library)
    
    def describe_table(self, library: str, table: str) -> pd.DataFrame:
        """Get column information for a table.
        
        Args:
            library: WRDS library name.
            table: Table name.
            
        Returns:
            DataFrame with column information.
        """
        db = self.connect()
        return db.describe_table(library=library, table=table)

