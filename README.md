# Bearplanes Capital

Quantitative trading project focused on developing, testing, and eventually trading systematic strategies.

**Current Focus/Interests:** Small cap equities, crypto arbitrage, prediction markets

## Setup

```bash
uv sync
```

Requires `.env` file with:
- `POLYGON_API_KEY`
- `DATABENTO_API_KEY`
- `WRDS_USERNAME`
- `WRDS_PASSWORD`
- `PGPASS_PATH` (path to PostgreSQL password file for WRDS)
- `ACCESS_KEY_ID` (AWS)
- `SECRET_ACCESS_KEY` (AWS)
- `BEARPLANES_DATA_DIR` (optional, custom data directory path)

## Data Sources

After evaluating Databento and Polygon, as well as datasets available through WRDS (with school access) such as CRSP and TAQ, I will be using WRDS-based resources for most of my historical data needs. For live trading, I plan on using Databento and Interactive Brokers.

I still need to implement functionality to confirm the point-in-time nature of the Compustat fundamental data and to match that information to my time series data in different ways.

If possible, I would also like to incorporate Twitter and Reddit data for sentiment analysis.

## Status

I have completed the basic data pipeline functionality for historical OHLCV data and am now working on strategy-specific code. I am applying methodologies from *Advances in Financial Machine Learning* by Marcos López de Prado and *Permutation and Randomization Tests* by Timothy Masters as I build the next stages of the project, including:

- Feature engineering on OHLCV bars
- Labeling and sampling methods
- ML model classes and cross-validation
- Backtesting framework

## Future

After developing and testing strategies with consistent out-of-sample performance, I will build out an order and trade management system and a trading client for Interactive Brokers using C#, Python, or C++.

I also plan to construct different bar types (dollar bars, volume bars, dollar imbalance bars, etc.) and develop features and strategies using these alternative sampling methods.


