import pandas as pd
import numpy as np

""" All 15 factors used, some not implemented yet. """

def accruals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate accruals factor.
    
    Accruals = (Annual change in noncash working capital - D&A) / Average total assets
    
    Where:
    - noncash working capital = actq - chq - lctq
    - Annual change = current quarter vs same quarter last year (4 quarters ago)
    - D&A = dpq (depreciation and amortization)
    - Average total assets = rolling 8 quarter (2 year) average
    
    Assumes df is already sorted by ['permno', 'datadate'].
    """
    # Calculate non-cash working capital
    df['ncwc'] = df['actq'] - df['chq'] - df['lctq']
    
    # Calculate annual change (4 quarters = 1 year)
    df['ncwc_annual_change'] = df.groupby('permno')['ncwc'].diff(4)
    
    # Average total assets over previous 2 fiscal years (8 quarters)
    df['avg_assets'] = (df.groupby('permno')['atq']
                          .rolling(8, min_periods=2)
                          .mean()
                          .reset_index(level=0, drop=True))
    
    # Accruals = (change in NCWC - depreciation & amortization) / avg assets
    df['accruals'] = (df['ncwc_annual_change'] - df['dpq']) / df['avg_assets']
    
    # Clean up intermediate columns
    df.drop(columns=['ncwc', 'ncwc_annual_change', 'avg_assets'], inplace=True)
    
    return df

def asset_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate asset growth factor.
    
    Asset Growth = The growth rate of total assets in the previous fiscal year
                 = (atq_t - atq_t-4) / atq_t-4
    
    Assumes df is already sorted by ['permno', 'datadate'].
    """
    # Year-over-year percentage change in total assets (4 quarters = 1 year)
    df['asset_growth'] = df.groupby('permno')['atq'].pct_change(4)
    
    return df

def comp_eqt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite equity issues factor.
    
    Composite Equity Issues = 12-month growth in market cap - 12-month stock return
    
    Where:
    - Market cap = price * shares outstanding
    - 12-month growth = (market_cap_today / market_cap_252_days_ago) - 1
    - 12-month return = (price_today / price_252_days_ago) - 1
    
    This captures equity issuance: if market cap grows faster than returns,
    the company issued shares.
    
    Assumes df is daily data, sorted by ['permno', 'date'].
    Requires columns: 'adj_close' and 'adj_shrout' for shares outstanding 
    """
    # Calculate current market cap (assumes shares outstanding is already forward-filled from quarterly)
    df['market_cap'] = df['adj_close'] * df['adj_shrout']
    
    # 12-month growth in market cap (252 trading days ≈ 1 year)
    df['market_cap_lag252'] = df.groupby('permno')['market_cap'].shift(252)
    df['mkt_cap_growth'] = (df['market_cap'] / df['market_cap_lag252']) - 1
    
    # 12-month cumulative stock return (252 trading days)
    df['price_lag252'] = df.groupby('permno')['adj_close'].shift(252)
    df['stock_return_12m'] = (df['adj_close'] / df['price_lag252']) - 1
    
    # Composite equity issues = market cap growth - stock return
    df['comp_equity_issues'] = df['mkt_cap_growth'] - df['stock_return_12m']
    
    # Clean up intermediate columns
    df.drop(columns=['market_cap', 'market_cap_lag252', 'mkt_cap_growth', 
                     'price_lag252', 'stock_return_12m'], inplace=True)
    
    return df

def failure_P():
    """
    # 4) Failure Probability
    # Calc: Strictly following Campbell, Hilscher, and Szilagyi (2008)
    # Cant access
    """
    pass

def gross_profitability(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate gross profitability factor.
    
    Gross Profitability = (Annual Revenue - Annual COGS) / Total Assets
    
    Where:
    - Annual Revenue = sum of last 4 quarters of revtq (trailing 12 months)
    - Annual COGS = sum of last 4 quarters of cogsq (trailing 12 months)
    - Total Assets = current quarter atq
    
    Assumes df is already sorted by ['permno', 'datadate'].
    Requires columns: 'revtq', 'cogsq', 'atq's
    """
    # Calculate trailing 12-month (4 quarters) revenue
    df['ttm_revenue'] = (df.groupby('permno')['revtq']
                           .rolling(4, min_periods=4)
                           .sum()
                           .reset_index(level=0, drop=True))
    
    # Calculate trailing 12-month (4 quarters) COGS
    df['ttm_cogs'] = (df.groupby('permno')['cogsq']
                        .rolling(4, min_periods=4)
                        .sum()
                        .reset_index(level=0, drop=True))
    
    # Gross profitability = (TTM Revenue - TTM COGS) / Total Assets
    df['gross_profitability'] = (df['ttm_revenue'] - df['ttm_cogs']) / df['atq']
    
    # Clean up intermediate columns
    df.drop(columns=['ttm_revenue', 'ttm_cogs'], inplace=True)
    
    return df

def investment_to_assets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Investment to Assets factor.
    
    The annual change in inventories scaled by lagged total assets.
    
    I/A = (invtq_t - invtq_t-4) / atq_t-4
    
    Where:
    - invtq = Inventories - Total
    - invtq_t - invtq_t-4 = Change from same quarter last year (annual change)
    - atq_t-4 = Total Assets lagged 4 quarters
    
    Assumes df is already sorted by ['permno', 'datadate'].
    """
    # Get inventory from 4 quarters ago (same quarter last year)
    df['invtq_lag4'] = df.groupby('permno')['invtq'].shift(4)
    
    # Calculate annual change in inventories
    df['inv_change'] = df['invtq'] - df['invtq_lag4']
    
    # Get total assets from 4 quarters ago (lagged book value)
    df['atq_lag4'] = df.groupby('permno')['atq'].shift(4)
    
    # Investment to Assets = change in inventory / lagged total assets
    df['investment_to_assets'] = df['inv_change'] / df['atq_lag4']
    
    # Clean up intermediate columns
    df.drop(columns=['invtq_lag4', 'inv_change', 'atq_lag4'], inplace=True)
    
    return df

def momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate momentum factor (12-2 momentum).
    
    Cumulative return from t-252 to t-22, skipping the most recent 21 days.
    
    Momentum = (price_t-22 / price_t-252) - 1
    
    Where:
    - price_t-252 = Adjusted close price 252 trading days ago (~12 months)
    - price_t-22 = Adjusted close price 22 trading days ago (~1 month)
    - Skips days t-21 to t to avoid short-term reversal effects
    
    This is the standard Jegadeesh and Titman (1993) momentum specification.
    
    Assumes df is daily data, sorted by ['permno', 'date'].
    Requires column: 'adj_close'
    """
    # Get price 252 days ago (start of momentum period)
    df['price_lag252'] = df.groupby('permno')['adj_close'].shift(252)
    
    # Get price 22 days ago (end of momentum period, skipping recent 21 days)
    df['price_lag22'] = df.groupby('permno')['adj_close'].shift(22)
    
    # Calculate cumulative return from t-252 to t-22
    df['momentum'] = (df['price_lag22'] / df['price_lag252']) - 1
    
    # Clean up intermediate columns
    df.drop(columns=['price_lag252', 'price_lag22'], inplace=True)
    
    return df

def noa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Net Operating Assets (NOA).
    
    NOA = (Operating Assets - Operating Liabilities) / Lagged Total Assets

    Top-down from totals:
    Operating Assets = atq - chq (or cheq) - ivstq (if excluding ST investments) - ivltq (LT Investments)
    Operating Liabilities = ltq (Total Liabilities) - dlcq - dlttq - dd1q (all debt components)
    
    Denominator: atq lagged one quarter (or one year for annual calc)
    
    Assumes df is already sorted by ['permno', 'datadate'].
    """
    # Calculate Operating Assets
    # Total assets minus cash and investments (non-operating assets)
    df['operating_assets'] = df['atq'] - df['chq']
    
    # Subtract short-term investments if available (fillna(0) to handle missing values)
    if 'ivstq' in df.columns:
        df['operating_assets'] = df['operating_assets'] - df['ivstq'].fillna(0)
    
    # Subtract long-term investments if available
    if 'ivltq' in df.columns:
        df['operating_assets'] = df['operating_assets'] - df['ivltq'].fillna(0)
    
    # Calculate Operating Liabilities
    # Total liabilities minus all debt components (financial liabilities)
    df['operating_liabilities'] = df['ltq']
    
    # Subtract debt in current liabilities
    if 'dlcq' in df.columns:
        df['operating_liabilities'] = df['operating_liabilities'] - df['dlcq'].fillna(0)
    
    # Subtract long-term debt
    if 'dlttq' in df.columns:
        df['operating_liabilities'] = df['operating_liabilities'] - df['dlttq'].fillna(0)
    
    # Subtract long-term debt due in one year
    if 'dd1q' in df.columns:
        df['operating_liabilities'] = df['operating_liabilities'] - df['dd1q'].fillna(0)
    
    # Calculate Net Operating Assets
    df['net_operating_assets'] = df['operating_assets'] - df['operating_liabilities']
    
    # Get lagged total assets (1 quarter lag)
    df['atq_lag1'] = df.groupby('permno')['atq'].shift(1)
    
    # NOA = Net Operating Assets / Lagged Total Assets
    df['noa'] = df['net_operating_assets'] / df['atq_lag1']
    
    # Clean up intermediate columns
    df.drop(columns=['operating_assets', 'operating_liabilities', 'net_operating_assets', 'atq_lag1'], 
            inplace=True)
    
    return df

def nsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Net Stock Issues (NSI) factor.
    
    The annual log change in split-adjusted shares outstanding.
    
    NSI = log(shares_t) - log(shares_t-252)
        = log(shares_t / shares_t-252)
    
    Where:
    - shares_t = Current split-adjusted shares outstanding
    - shares_t-252 = Shares outstanding 252 trading days ago (~12 months)
    - Log change is symmetric: issuing/repurchasing have equal magnitude effects
    
    Positive NSI = company issued shares (dilution)
    Negative NSI = company repurchased shares (buyback)
    
    Companies issuing shares typically underperform (negative expected returns).
    
    Assumes df is daily data, sorted by ['permno', 'date'].
    Requires column: 'adj_shrout' (split-adjusted shares outstanding)
    """
    # Convert shares outstanding to log base e
    df['log_shrout'] = np.log(df['adj_shrout'])
    
    # Get log shares outstanding from 252 days ago (12 months)
    df['log_shrout_lag252'] = df.groupby('permno')['log_shrout'].shift(252)
    
    # NSI = log change in shares outstanding over past year
    df['nsi'] = df['log_shrout'] - df['log_shrout_lag252']
    
    # Clean up intermediate columns
    df.drop(columns=['log_shrout', 'log_shrout_lag252'], inplace=True)
    
    return df

def o_score(df: pd.DataFrame) ->pd.DataFrame:
    """
        # 10) O Score
        # Calc: Strictly following Ohlson 1980
    """
    pass

def roa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate ROA factor (quarterly earnings growth).
    
    The ratio of quarterly earnings to last quarter's earnings.
    
    ROA = niq_t / niq_t-1
    
    Where:
    - niq_t = Net Income (Loss) for current quarter
    - niq_t-1 = Net Income (Loss) for previous quarter
    
    This measures quarter-over-quarter earnings momentum.
    Companies with strong recent earnings growth tend to continue outperforming.
    
    Assumes df is quarterly data, sorted by ['permno', 'datadate'].
    Requires column: 'niq' (Net Income quarterly)
    """
    # Get net income from previous quarter
    df['niq_lag1'] = df.groupby('permno')['niq'].shift(1)
    
    # ROA = current quarter earnings / previous quarter earnings
    df['roa'] = df['niq'] / df['niq_lag1']
    
    # Clean up intermediate columns
    df.drop(columns=['niq_lag1'], inplace=True)
    
    return df

def beta(df: pd.DataFrame)->pd.DataFrame:
    """
        # 12) Beta
        # Calc:
    """
    pass
 
def book_to_market(df: pd.DataFrame)->pd.DataFrame:
    """
    # 13) Book to market
    # Calc: The ratio of the book value of common equity to the market value of equity (ceqq / market cap)

    """
    pass

def reversal(df: pd.DataFrame)->pd.DataFrame:
    """    
    # 14) Reversal 
    # Calc: Cumulative return over the past 21 days
    """
    pass

def size(df: pd.DataFrame)->pd.DataFrame:
    """
    # 15) Size
    # Calc: ln of market cap
    """
    pass