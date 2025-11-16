"""
Path management utilities for the Polygon data processing pipeline.

This module provides centralized path management using pathlib.Path,
making it easy to reference project directories without hardcoded paths.
"""
from pathlib import Path
from typing import Optional

# Get the project root directory
# This file is in Polygon/, so we go up one level to get the project root
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Data directories
DATA_ROOT = PROJECT_ROOT / "Data"
STOCKS_DATA = DATA_ROOT / "Stocks"
POLYGON_DATA = STOCKS_DATA / "Polygon"
DATABENTO_DATA = STOCKS_DATA / "Databento"

# Output/checkpoint directories
OUTPUT_ROOT = PROJECT_ROOT / "output"
CHECKPOINTS_ROOT = OUTPUT_ROOT / "checkpoints"

# Reference data specific paths
REFERENCE_DATA_CHECKPOINTS = CHECKPOINTS_ROOT / "reference_data"
REFERENCE_DATA_SUCCESS = REFERENCE_DATA_CHECKPOINTS / "success"
REFERENCE_DATA_FAILED = REFERENCE_DATA_CHECKPOINTS / "failed"


def get_data_file(filename: str, subdirectory: Optional[str] = None) -> Path:
    """
    Get a path to a data file in the Polygon data directory.
    
    Args:
        filename: Name of the file (e.g., "OHLCV_Historical_2016-01-01_to_2025-10-26.csv")
        subdirectory: Optional subdirectory within Polygon data (e.g., "test-fullpull")
    
    Returns:
        Path object pointing to the file
    
    Example:
        >>> path = get_data_file("OHLCV_Historical_2016-01-01_to_2025-10-26.csv")
        >>> path = get_data_file("2025-10-01.csv.gz", subdirectory="test10-20")
    """
    if subdirectory:
        return POLYGON_DATA / subdirectory / filename
    return POLYGON_DATA / filename


def ensure_dir(path: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Path to the directory
    
    Returns:
        The same path object (for chaining)
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


# Ensure output directories exist
ensure_dir(OUTPUT_ROOT)
ensure_dir(CHECKPOINTS_ROOT)
ensure_dir(REFERENCE_DATA_CHECKPOINTS)
ensure_dir(REFERENCE_DATA_SUCCESS)
ensure_dir(REFERENCE_DATA_FAILED)

