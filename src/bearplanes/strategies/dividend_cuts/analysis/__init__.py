"""Analysis functions for dividend cuts strategy."""

from bearplanes.strategies.dividend_cuts.analysis.returns import (
    analyze_52week_positioning, analyze_return_distribution,
    backtest_short_strategy, plot_cut_severity_vs_returns,
    plot_monte_carlo_paths)

__all__ = [
    "analyze_return_distribution",
    "analyze_52week_positioning",
    "plot_cut_severity_vs_returns",
]

