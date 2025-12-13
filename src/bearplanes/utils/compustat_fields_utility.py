fields = {
    
    # === IDENTIFIERS & DATES ===
    'gvkey': 'Company identifier',
    'datadate': 'Period end date',
    'rdq': 'Report date (for point-in-time)',
    'fyearq': 'Fiscal year',
    'fqtr': 'Fiscal quarter',
    'tic': 'Ticker',
    'cusip': 'CUSIP',
    'conm': 'Company name',
    'updq': 'Update code (2=prelim, 3=final)',
    'ajexq': 'Adjustment factor (for per-share calcs)',
    
    # === DISTRESS/STATUS INDICATORS ===
    'staltq': 'Status Alert (bankruptcy, litigation, liquidation)',
    'compstq': 'Comparability Status (mergers, restatements)',
    
    # === INCOME STATEMENT ===
    'revtq': 'Revenue (Total)',
    'cogsq': 'Cost of Goods Sold',
    'xsgaq': 'Selling, General & Admin Expense',
    'xrdq': 'R&D Expense',
    'xintq': 'Interest Expense',
    'niq': 'Net Income',
    'oiadpq': 'Operating Income After Depreciation',
    'dpq': 'Depreciation & Amortization',
    'txtq': 'Income Taxes',
    'ebitdaq': 'EBITDA',
    'ibq': 'Income Before Extraordinary Items',
    'xoprq': 'Operating Expenses - Total',
    
    # === BALANCE SHEET ===
    'atq': 'Total Assets',
    'actq': 'Current Assets',
    'cheq': 'Cash and Short Term Investments',
    'chq': 'Cash',
    'rectq': 'Receivables',
    'invtq': 'Inventories',
    'acoq': 'Current Assets - Other',
    'lctq': 'Current Liabilities',
    'ltq': 'Total Liabilities',
    'dlcq': 'Debt in Current Liabilities (short-term)',
    'dlttq': 'Long-Term Debt',
    'ceqq': 'Common Equity',
    'wcapq': 'Working Capital',
    'apq': 'Accounts Payable',
    'lcoq': 'Current Liabilities - Other',
    'pstknq': 'Preferred Stock',
    'mibq': 'Minority Interest',
    'txditcq': 'Deferred Taxes - Investment Tax Credit',
    
    # === CASH FLOW (Quarterly YTD) ===
    'oancfy': 'Operating Activities - Net Cash Flow (YTD)',
    'capxy': 'Capital Expenditures (YTD)',
    'fincfy': 'Financing Activities - Net Cash Flow (YTD)',
    'ivncfy': 'Investing Activities - Net Cash Flow (YTD)',
    'dltisy': 'Long-Term Debt - Issuance (YTD)',
    'dlcchy': 'Current Debt - Changes (YTD)',
    'dltrq': 'Long-Term Debt - Reduction (YTD)',
    
    # === PER SHARE DATA ===
    'cshoq': 'Common Shares Outstanding',
    'cshopq': 'Common Shares Outstanding - Prior Period',
    'epspxq': 'EPS Basic - Excluding Extraordinary Items',
    'prccq': 'Price Close - Quarter',
    
    # === MARKET DATA ===
    'mkvaltq': 'Market Value (Market Cap)',
    
    # === DIVIDENDS ===
    'dvy': 'Dividends - Year to Date',
}

# DERIVED METRICS:
# PROFITABILITY:
#   - Gross Margin = (revtq - cogsq) / revtq
#   - Operating Margin = oiadpq / revtq
#   - Net Margin = niq / revtq
#   - ROA = niq / atq
#   - ROE = niq / ceqq
  
# LEVERAGE:
#   - Debt/Equity = (dlcq + dlttq) / ceqq
#   - Debt/Assets = (dlcq + dlttq) / atq
#   - Current Ratio = actq / lctq
#   - Interest Coverage = oiadpq / xintq
#   - EBITDA/Interest = ebitdaq / xintq
  
# CASH FLOW:
#   - Free Cash Flow = oancfy - capxy
#   - FCF/Revenue = (oancfy - capxy) / revtq
#   - Operating CF/Net Income = oancfy / niq
#   - Accruals Ratio = (niq - oancfy) / atq
  
# LIQUIDITY:
#   - Quick Ratio = (actq - invtq) / lctq
#   - Cash Ratio = cheq / lctq
#   - Payables Days = (apq / cogsq) * 90
#   - Working Capital Ratio = wcapq / atq
  
# DIVIDEND METRICS:
#   - Payout Ratio = dvy / niq
#   - Cash Flow Payout = dvy / oancfy
#   - Free Cash Flow Coverage = (oancfy - capxy) / dvy
#   - Debt Issuance/Dividends = dltisy / dvy
#   - Financing CF/Dividends = fincfy / dvy
#   - Total Payout = (dvy + dltrq) / oancfy
  
# CAPITAL ALLOCATION:
#   - CapEx/Depreciation = capxy / dpq
#   - Net Borrowing = dltisy - dltrq
  
# SHARE CHANGES:
#   - Dilution Rate = (cshoq - cshopq) / cshopq
  
# GROWTH RATES (quarter-over-quarter or YoY):
#   - Revenue growth
#   - Earnings growth  
#   - FCF growth
#   - Dividend growth (identifies cuts!)
  
# VALUATION:
#   - P/E = prccq / epspxq
#   - P/B = mkvaltq / ceqq
#   - EV/EBITDA (approx) 