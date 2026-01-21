"""Centralized path management for the Bearplanes project.

This module provides functions to get standard paths throughout the project,
eliminating hardcoded paths and making the code portable.
"""
from pyarrow.dataset import dataset
import os
from pathlib import Path
from typing import Optional


def get_project_root() -> Path:
    """
    Get the project root directory.
    
    Assumes this file is in src/bearplanes/utils/
    Returns the bearplanes/ directory (project root).
    """
    return Path(__file__).parent.parent.parent.parent


def get_data_dir() -> Path:
    """
    Get the main data directory.
    
    Checks BEARPLANES_DATA_DIR environment variable first,
    then defaults to <project_root>/data/
    
    Returns:
        Path to data directory
    """
    env_data_dir = os.getenv("BEARPLANES_DATA_DIR")
    if env_data_dir:
        return Path(env_data_dir)
    return get_project_root() / "data"


def get_raw_data_dir(source: str, dataset: str) -> Path:
    """
    Get raw data directory for a specific data source.
    
    Args:
        source: Data source name ('polygon', 'databento', 'wrds', etc.)
        dataset: The dataset from the given source. I am organizing by this way currently.
                For wrds datasets might be crsp, crsp_distribution_events etc.
    
    Returns:
        Path to raw data directory for that source
        
    Example:
        >>> get_raw_data_dir('polygon')
        PosixPath('/path/to/bearplanes/data/raw/polygon')
    """
    path = get_data_dir() / "raw" / source.lower() / dataset.lower()
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_processed_data_dir(source: str, dataset: str) -> Path:
    """
    Get processed data directory for a specific dataset.
    
    Args:
        source: Vendor Name or source of data(wrds, polygon, databento etc.)
        dataset: Dataset name ('crsp', 'compustat', etc.)
        @TODO, date: Date (date I might have on the file with some meaning of date acquired, date range of the data)
    
    Returns:
        Path to processed data directory
    """
    path = get_data_dir() / "processed" / source.lower() / dataset.lower()
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_strategy_data_dir(strategy_name: str) -> Path:
    """
    Get data directory for a specific strategy.
    
    Args:
        strategy_name: Name of the strategy ('dividend_cuts', etc.)
    
    Returns:
        Path to strategy data directory
    """
    path = get_data_dir() / "strategies" / strategy_name.lower()
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_notebook_dir() -> Path:
    """Get the notebooks directory."""
    return get_project_root() / "notebooks"


# Legacy compatibility - map old Data/ structure to new data/ structure
def get_legacy_data_dir(subdirs: Optional[list[str]] = None) -> Path:
    """
    Get path to legacy Data/ directory structure.
    
    This is for backward compatibility during migration.
    Eventually this should be deprecated.
    
    Args:
        subdirs: List of subdirectories (e.g., ['Stocks', 'Polygon'])
    
    Returns:
        Path to legacy data location
    """
    base = get_project_root() / "Data"
    if subdirs:
        for subdir in subdirs:
            base = base / subdir
    return base

def get_file_path(file_directory: str, file_name: str):
    """
    Get the full file path to a specific file. 

    Args:
        file_directory: The exact folder/directory of the file
        file_name: The name of the file which should include the file extension
    """
    path = file_directory / file_name
    return path
    