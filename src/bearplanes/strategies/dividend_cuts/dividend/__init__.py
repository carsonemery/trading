"""Dividend data processing for dividend cuts strategy."""

from bearplanes.strategies.dividend_cuts.dividend.create_cuts_features import (
    analyze_flagged_cuts, flag_reductions, identify_continuous_series,
    merge_split_factors, prepare_dividend_data)
from bearplanes.strategies.dividend_cuts.dividend.distributions_cleaning import \
    optimize_distributions
from bearplanes.strategies.dividend_cuts.dividend.merge import merge

__all__ = [
    "merge",
    "prepare_dividend_data",
    "merge_split_factors",
    "identify_continuous_series",
    "flag_reductions",
    "analyze_flagged_cuts",
    "optimize_distributions",
]

