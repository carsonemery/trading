"""Bearplanes - Quantitative Trading System.

A modular quantitative trading system for developing, testing, and deploying
systematic trading strategies.
"""

__version__ = "0.1.0"

# Auto-load environment variables on import
from bearplanes.utils.config import load_environment
load_environment()

__all__ = ["__version__"]

