field_list = [
    # === CRITICAL IDENTIFIERS & DATES (for point-in-time analysis) ===
    'gvkey',    # Global Company Key, a unique identifier and pkey for each co. in the DB
    'iid',
    'datadate', # Data Date - CRITICAL for point-in-time analysis (fiscal period end date)
    'fyearq',   # Fiscal Year of the quarter
    'fqtr',     # Fiscal Quarter (1, 2, 3, 4)
    'conm',     # Company Name
    'tic',      # Ticker Symbol
    'cusip',    # CUSIP identifier
    'cik',      # CIK identifier
    
    # === DATA FORMAT FIELDS (important for filtering) ===
    'indfmt',   # Industry Format - 'INDL' (industrial) or 'FS' (financial services)
    'consol',   # Consolidation Level - 'C' for consolidated (parent + subs)
    'datafmt',  # Data Format - 'STD' for standardized
    'popsrc',   # Population Source - data source identifier
    
    'exchg',    # Primary exchange
    'fyr',      # Fiscal Year End Month (month end for cos account fiscal year)
    # NOTE: The following 20 fields don't exist in comp.fundq (they're in comp.company instead)
    # Removed: add1, addzip, city, conml, ein, fyrc, ggroup, gind, gsector, gsubind, 
    # idbflag, incorp, ipodate, naics, prican, prirow, priusa, state, stko, weburl
    'acctchgq',# Adoption of accounting changes
    'acctstdq', # Accounting standard
    'ajexq',   # Adjustment Factor, cumulative by Ex-Date, a ratio that enables you to adjust per share data
    'ajpq',    # Identical to above except by Pay-Date
    'apdedateq', # Actual Period End Date - the actual date the company closes accounting for the period
    'bsprq',   # Balance Sheet Presentation - Contains the code that identifies a companys use of either a classified or unclassified balance sheet
    
    # === CRITICAL TIMING/RESTATEMENT FIELDS (Avoid Look-Ahead Bias!) ===
    'rdq',      # Report Date of Earnings - CRITICAL! Date earnings first publicly reported
    'fdateq',   # Final Date - When data was finalized (use to detect restatements)
    'pdateq',   # Preliminary Date - When data was first updated
    'finalq',   # Final Indicator Flag - CRITICAL! 'F'=Final, 'P'=Preliminary (only use 'F'!)
    'updq',     # Update Code - Restatement version (0=original, 1+=restated, use max per period)
    'compstq',  # Comparability Status - Flags accounting changes/mergers
    'srcq',     # Source Code - Data source identifier
    
    # === OTHER METADATA ===
    'curncdq',  # Native currency company reports its financial data in
    'currtrq',  # Currency translation rate, the rate used to translate int currency amounts to USD, effective as of the companys balance sheet date
    # Removed curuscnq - CAD-only conversion (use currtrq for all currencies)
    'datacqtr', # Calendar Data Year and Quarter - This item is the calendar quarter that the fiscal quarter most closely represents
    'datafqtr', # Fiscal Quarter by Year
    'staltq',   # Status Alert - Co is undergoing bankcruptcy or LBO - KEPT for distress signals
    'acchgq',   # Accounting Changes - Cumulative Effect
    'acomincq', # Accumulated Other Comprehensive Income (Loss)
    'acoq',     # Current Assets - Other Total 
    'actq',     # Current Assets - Total
    'altoq',    # Other Long Term Assets
    'ancq',     # Non Current Assets - Total
    'anoq',     # Assets Netting & Other Adjustments
    'aociderglq', # Accum Other Comp Inc - Derivatives Unrealized Gain/Loss
    'aociotherq', # Accum Other Comp Inc - Other Adjustments
    'aocipenq',   # Accum Other Comp Inc - Min Pension Liab Adj
    'aocisecglq', # Accum Other Comp Inc - Unreal G/L Ret Int in Sec Assets
    'aol2q',    # Assets Level2 (Observable)
    'aoq',      # Assets - Other - Total
    'apq',      # Account Payable/Creditors - Trade
    'aqaq',     # Acquisition/Merger After-Tax
    'aqpl1q',   # Assets Level1 (Quoted Prices)
    'aqpq',     # Acquisition/Merger Pretax
    # Removed arceq - S&P Core Earnings adjustment (rarely reported, NULL across all test stocks)
    'atq',      # Assets - Total
    'aul3q',    # Assets Level3 (Unobservable)
    # Removed billexceq - Construction/project accounting only
    'capr1q',   # Risk-Adjusted Capital Ratio - Tier 1
    'capr2q',   # Risk-Adjusted Capital Ratio - Tier 2
    'capr3q',   # Risk-Adjusted Capital Ratio - Combined
    'capsftq',  # Capitalized Software
    'capsq',    # Capital Surplus/Share Premium Reserve
    # Removed ceiexbillq - Construction/project accounting only
    'ceqq',     # Common/Ordinary Equity - Total
    'cheq',     # Cash and Short-Term Investments
    'chq',      # Cash
    'cibegniq', # Comp Inc - Beginning Net Income
    'cicurrq',  # Comp Inc - Currency Trans Adj
    'ciderglq', # Comp Inc - Derivative Gains/Losses
    'cimiiq',   # Comprehensive Income - Noncontrolling Interest
    'ciotherq', # Comp Inc - Other Adj
    'cipenq',   # Comp Inc - Minimum Pension Adj
    'ciq',      # Comprehensive Income - Total
    'cisecglq', # Comp Inc - Securities Gains/Losses
    'citotalq', # Comprehensive Income - Parent
    'cogsq',    # Cost of Goods Sold
    'csh12q',   # Common Shares Used to Calculate Earnings Per Share - 12 Months Moving
    'cshfd12',  # Common Shares Used to Calc Earnings Per Share - Fully Diluted - 12 Months Moving
    'cshfdq',   # Com Shares for Diluted EPS
    'cshiq',    # Common Shares Issued
    'cshopq',   # Total Shares Repurchased - Quarter
    'cshoq',    # Common Shares Outstanding
    'cshprq',   # Common Shares Used to Calculate Earnings Per Share - Basic
    'cstkeq',   # Common Stock Equivalents - Dollar Savings
    'cstkq',    # Common/Ordinary Stock (Capital)
    'dcomq',    # Deferred Compensation
    'dd1q',     # Long-Term Debt Due in One Year
    'deracq',   # Derivative Assets - Current
    'deraltq',  # Derivative Assets Long-Term
    'derhedglq', # Gains/Losses on Derivatives and Hedging
    'derlcq',   # Derivative Liabilities- Current
    'derlltq',  # Derivative Liabilities Long-Term
    'diladq',   # Dilution Adjustment
    'dilavq',   # Dilution Available - Excluding Extraordinary Items
    'dlcq',     # Debt in Current Liabilities
    'dlttq',    # Long-Term Debt - Total
    'doq',      # Discontinued Operations
    # Removed dpacreq - Real estate specific depreciation
    'dpactq',   # Depreciation, Depletion and Amortization (Accumulated)
    'dpq',      # Depreciation and Amortization - Total
    # Removed dpretq - Real estate specific depreciation (use dpq instead)
    'drcq',     # Deferred Revenue - Current
    'drltq',    # Deferred Revenue - Long-term
    'dteaq',    # Extinguishment of Debt After-tax
    'dtepq',    # Extinguishment of Debt Pretax
    # Removed dvintfq - Dividends & Interest Receivable CF (too niche)
    'dvpq',     # Dividends - Preferred/Preference
    'epsf12',   # Earnings Per Share (Diluted) - Excluding Extraordinary Items - 12 Months Moving
    'epsfi12',  # Earnings Per Share (Diluted) - Including Extraordinary Items
    'epsfiq',   # Earnings Per Share (Diluted) - Including Extraordinary Items
    'epsfxq',   # Earnings Per Share (Diluted) - Excluding Extraordinary items
    'epspi12',  # Earnings Per Share (Basic) - Including Extraordinary Items - 12 Months Moving
    'epspiq',   # Earnings Per Share (Basic) - Including Extraordinary Items
    'epspxq',   # Earnings Per Share (Basic) - Excluding Extraordinary Items
    'epsx12',   # Earnings Per Share (Basic) - Excluding Extraordinary Items - 12 Months Moving
    'fcaq',     # Foreign Exchange Income (Loss)
    # Removed ffoq - Funds From Operations (REIT-specific only)
    # Removed 8 Finance Division fields (NULL across all test stocks):
    # finacoq, finaoq, finchq, finivstq, finlcoq, finltoq, finnpq, finxoprq
    # Keeping these Finance Division fields (may have data for financial services companies):
    'findlcq',  # Finance Division Long-Term Debt Current
    'findltq',  # Finance Division Debt Long-Term
    'finreccq', # Finance Division Current Receivables
    'finrecltq', # Finance Division Long-Term Receivables
    'finrevq',  # Finance Division Revenue
    'finxintq', # Finance Division Interest Expense
    'gdwlamq',  # Amortization of Goodwill
    'gdwlia12', # Impairments of Goodwill AfterTax - 12mm
    'gdwliaq',  # Impairment of Goodwill After-tax
    'gdwlid12', # Impairments Diluted EPS - 12mm
    'gdwlipq',  # Impairment of Goodwill Pretax
    'gdwlq',    # Goodwill (net)
    'glaq',     # Gain/Loss After-Tax
    'glcea12',  # Gain/Loss on Sale (Core Earnings Adjusted) After-tax 12MM
    'glceaq',   # Gain/Loss on Sale (Core Earnings Adjusted) After-tax
    'glcedq',   # Gain/Loss on Sale (Core Earnings Adjusted) Diluted EPS
    'glcepq',   # Gain/Loss on Sale (Core Earnings Adjusted) Pretax
    'glivq',    # Gains/Losses on investments
    'glpq',     # Gain/Loss Pretax
    'ibadj12',  # Income Before Extra Items - Adj for Common Stock Equivalents - 12MM
    'ibadjq',   # Income Before Extraordinary Items - Adjusted for Common Stock Equivalents
    'ibcomq',   # Income Before Extraordinary Items - Available for Common
    'ibmiiq',   # Income before Extraordinary Items and Noncontrolling Interests
    'ibq',      # Income Before Extraordinary Items
    'icaptq',   # Invested Capital - Total - Quarterly
    'intaccq',  # Interest Accrued
    'intanoq',  # Other Intangibles
    'intanq',   # Intangible Assets - Total
    'invfgq',   # Inventory - Finished Goods
    'invoq',    # Inventory - Other
    'invrmq',   # Inventory - Raw Materials
    'invtq',    # Inventories - Total
    'invwipq',  # Inventory - Work in Process
    'ivaeqq',   # Investment and Advances - Equity
    'ivaoq',    # Investment and Advances - Other
    'ivltq',    # Total Long-term Investments
    'ivstq',    # Short-Term Investments- Total
    'lcoq',     # Current Liabilities - Other - Total
    'lctq',     # Current Liabilities - Total
    'lltq',     # Long-Term Liabilities (Total)
    'lnoq',     # Liabilities Netting & Other Adjustments
    'lol2q',    # Liabilities Level2 (Observable)
    'loq',      # Liabilities - Other
    'loxdrq',   # Liabilities - Other - Excluding Deferred Revenue
    'lqpl1q',   # Liabilities Level1 (Quoted Prices)
    'lseq',     # Liabilities and Stockholders Equity - Total
    'ltmibq',   # Liabilities - Total and Noncontrolling Interest
    'ltq',      # Liabilities - Total
    'lul3q',    # Liabilities Level3 (Unobservable)
    'mibnq',    # Noncontrolling Interests - Nonredeemable - Balance Sheet
    'mibq',     # Noncontrolling Interest - Redeemable - Balance Sheet
    'mibtq',    # Noncontrolling Interests - Total - Balance Sheet
    'miiq',     # Noncontrolling Interest - Income Account
    'msaq',     # Accum Other Comp Inc - Marketable Security Adjustments
    'ncoq',     # Net Charge-Offs
    'niitq',    # Net Interest Income (Tax Equivalent)
    'nimq',     # Net Interest Margin
    'niq',      # Net Income (Loss)
    'nopiq',    # Non-Operating Income (Expense) - Total
    'npatq',    # Nonperforming Assets - Total
    'npq',      # Notes Payable
    'nrtxtq',   # Nonrecurring Income Taxes - After-tax
    'oepf12',   # Earnings Per Share - Diluted - from Operations - 12MM
    'oeps12',   # Earnings Per Share from Operations - 12 Months Moving
    'oepsxq',   # Earnings Per Share - Diluted - from Operations
    'oiadpq',   # Operating Income After Depreciation - Quarterly
    'oibdpq',   # Operating Income Before Depreciation - Quarterly
    'opepsq',   # Earnings Per Share from Operations
    'optdrq',   # Dividend Rate - Assumption (%)
    'optfvgrq', # Options - Fair Value of Options Granted
    'optlifeq', # Life of Options - Assumption (# yrs)
    'optrfrq',  # Risk Free Rate - Assumption (%)
    'optvolq',  # Volatility - Assumption (%)
    'piq',      # Pretax Income
    'pllq',     # Provision for Loan/Asset Losses
    'pncq',     # Core Pension Adjustment
    'pnc12',    # Core Pension Adjustment 12-Month Moving/Trailing
    'pnrshoq',  # Nonreedemable Pref Shares
    'ppegtq',   # Property, Plant and Equipment - Total (Gross) - Quarterly
    'ppentq',   # Property Plant and Equipment - Total (Net)
    'prcaq',    # Core Post Retirement Adjustment
    'prce12',   # Core Post Retirement Adjustment 12 Months moving
    'prcraq',   # Repurchase Price - Average per share Quarter
    'prshoq',   # Redeem Pfd Shares Outs (000)
    'pstknq',   # Preferred/Preference Stock - Nonredeemable
    'pstkq',    # Preferred/Preference Stock (Capital) - Total
    'pstkrq',   # Preferred/Preference Stock - Redeemable
    'rcaq',     # Restructuring Cost After-tax
    'rcpq',     # Restructuring Cost Pretax
    'rdipaq',   # In Process R&D Expense After-tax
    'rdipq',    # In Process R& 
    'recdq',    # Receivables - Estimated Doubtful
    'rectaq',   # Accum Other Comp Inc - Cumulative Translation Adjustments
    'rectoq',   # Receivables - Current Other incl Tax Refunds
    'rectq',    # Receivables - Total
    'rectrq',   # Receivables - Trade
    'recubq',   # Unbilled Receivables - Quarterly - KEPT for revenue quality
    'req',      # Retained Earnings
    # Removed retq - Total RE Property (real estate specific)
    'reunaq',   # Unadjusted Retained Earnings
    'revtq',    # Revenue - Total
    'rllq',     # Reserve for Loan/Asset Losses
    'rraq',     # Reversal - Restructruring/Acquisition Aftertax
    'rra12',    # Reversal - Restructruring/Acquisition Aftertax 12MM
    'rstcheltq', # Long-Term Restricted Cash & Investments
    'rstcheq',  # Restricted Cash & Investments - Current
    'saleq',    # Sales/Turnover (Net)
    'seta12',   # Settlement (Litigation/Insurance) AfterTax - 12mm
    'setaq',    # Settlement (Litigation/Insurance) After-tax
    'spiq',     # Special Items
    'sretq',    # Gain/Loss on Sale of Property - KEPT for earnings quality analysis
    'stkcoq',   # Stock Compensation Expense
    'stkcpaq',  # After-tax stock compensation
    'teqq',     # Stockholders Equity - Total
    'tfvaq',    # Total Fair Value Assets
    'tfvceq',   # Total Fair Value Changes including Earnings
    'tfvlq',    # Total Fair Value Liabilities
    'tieq',     # Interest Expense - Total (Financial Services)
    'tiiq',     # Interest Income - Total (Financial Services)
    'tstknq',   # Treasury Stock - Number of Common Shares
    'tstkq',    # Treasury Stock - Total (All Capital)
    'txdbaq',   # Deferred Tax Asset - Long Term
    'txdbcaq',  # Current Deferred Tax Asset
    'txdbclq',  # Current Deferred Tax Liability
    'txdbq',    # Deferred Taxes - Balance Sheet
    'txdiq',    # Income Taxes - Deferred
    'txditcq',  # Deferred Taxes and Investment Tax Credit
    'txpq',     # Income Taxes Payable
    'txtq',     # Income Taxes - Total
    'uacoq',    # Current Assets - Other - Utility
    'uaoq',     # Other Assets - Utility
    'uaptq',    # Accounts Payable - Utility
    'uddq',     # Debt (Debentures) - Utility
    'udmbq',    # Debt (Mortgage Bonds)
    'udoltq',   # Debt (Other Long-Term)
    'ugiq',     # Gross Income (Income Before Interest Charges)
    'uinvq',    # Inventories
    'ulcoq',    # Current Liabilities - Other
    'uniamiq',  # Net Income before Extraordinary Items After Noncontrolling Interest
    'unopincq', # Nonoperating Income (Net) - Other
    'utemq',    # Maintenance Expense - Total
    'wcapq',    # Working Capital (Balance Sheet)
    'wdaq',     # Writedowns After-tax
    'xaccq',    # Accrued Expenses
    'xidoq',    # Extraordinary Items and Discontinued Operations
    'xintq',    # Interest and Related Expense- Total
    'xiq',      # Extraordinary Items
    'xoprq',    # Operating Expense- Total
    'xrdq',     # Research and Development Expense
    'xsgaq',    # Selling, General and Administrative Expenses
    'acchgy',   # Accounting Changes - Cumulative Effect
    'afudccy',  # Allowance for Funds Used During Construction (Cash Flow)
    'afudciy',  # Allowance for Funds Used During Construction (Investing) (Cash Flow)
    'amcy',     # Amortization (Cash Flow) - KEPT for cash flow analysis
    'aolochy',  # Assets and Liabilities - Other (Net Change)
    'apalchy',  # Accounts Payable and Accrued Liabilities - Increase (Decrease)
    'aqay',     # Acquisition/Merger After-Tax
    'aqcy',     # Acquisitions
    'capxy',    # Capital Expenditures
    'cdvcy',    # Cash Dividends on Common Stock (Cash Flow)
    'chechy',   # Cash and Cash Equivalents - Increase (Decrease)
    'cibegniy', # Comp Inc - Beginning Net Income
    'cicurry',  # Comp Inc - Currency Trans Adj
    'cidergly', # Comp Inc - Derivative Gains/Losses
    'cimiiy',   # Comprehensive Income - Noncontrolling Interest
    'ciothery', # Comp Inc - Other Adj
    'cipeny',   # Comp Inc - Minimum Pension Adj
    'cisecgly', # Comp Inc - Securities Gains/Losses
    'citotaly', # Comprehensive Income - Parent
    'ciy',      # Comprehensive Income - Total
    'cogsy',    # Cost of Goods Sold
    'cshpry',   # Common Shares Used to Calculate Earnings Per Share - Basic
    'depcy',    # Depreciation and Depletion (Cash Flow)
    'derhedgly', # Gains/Losses on Derivatives and Hedging
    'dilady',   # Dilution Adjustment
    'dilavy',   # Dilution Available - Excluding Extraordinary Items
    'dlcchy',   # Changes in Current Debt
    'dltisy',   # Long-Term Debt - Issuance
    'dltry',    # Long-Term Debt - Reduction
    'doy',      # Discontinued Operations
    'dpcy',     # Depreciation and Amortization - Statement of Cash Flows
    # Removed dprety - Real estate specific (have dpq, dpy already)
    'dpy',      # Depreciation and Amortization - Total
    'dteay',    # Extinguishment of Debt After-tax
    'dvpy',     # Dividends - Preferred/Preference
    'dvy',      # Cash Dividends
    'fcay',     # Foreign Exchange Income (Loss)
    # Removed ffoy - Funds From Operations (REIT-specific only)
    'fiaoy',    # Financing Activities - Other
    'fincfy',   # Financing Activities - Net Cash Flow
    # Removed finrevy - Finance Division Revenue (annual, use quarterly finrevq)
    # Removed finxinty - Finance Division Interest Expense (annual, use quarterly finxintq)
    # Removed finxopry - Finance Division Operating Expense (NULL across all test stocks)
    'fopty',    # Funds From Operations - Total
    'fsrcty',   # Sources of Funds - Total
    'fusety',   # Uses of Funds - Total
    'gdwlamy',  # Amortization of Goodwill
    'gdwliay',  # Impairment of Goodwill After-tax
    'glay',     # Gain/Loss After-Tax
    'glceay',   # Gain/Loss on Sale (Core Earnings Adjusted) After-tax
    'glivy',    # Gains/Losses on investments
    'glpy',     # Gain/Loss Pretax
    'hedgegly', # Gain/Loss on Ineffective Hedges
    'ibadjy',   # Income Before Extraordinary Items - Adjusted for Common Stock Equivalents
    'ibcomy',   # Income Before Extraordinary Items - Available for Common
    'ibcy',     # Income Before Extraordinary Items - Statement of Cash Flows
    'ibmiiy',   # Income before Extraordinary Items and Noncontrolling Interests
    'iby',      # Income Before Extraordinary Items
    'intpny',   # Interest Paid - Net
    'invchy',   # Inventory - Decrease (Increase)
    'itccy',    # Investment Tax Credit - Net (Cash Flow)
    'ivacoy',   # Investing Activities - Other
    'ivchy',    # Increase in Investments
    'ivncfy',   # Investing Activities - Net Cash Flow
    'ivstchy',  # Short-Term Investments - Change
    'miiy',     # Noncontrolling Interest - Income Account
    'niity',    # Net Interest Income (Tax Equivalent)
    'nimy',     # Net Interest Margin
    'niy',      # Net Income (Loss)
    'nopiy',    # Non-Operating Income (Expense) - Total
    'nrtxty',   # Nonrecurring Income Taxes - After-tax
    'oancfy',   # Operating Activities - Net Cash Flow
    'oepsxy',   # Earnings Per Share - Diluted - from Operations
    'oiadpy',   # Operating Income After Depreciation - Year-to-Date
    'oibdpy',   # Operating Income Before Depreciation
    'opepsy',   # Earnings Per Share from Operations
    'optdry',   # Dividend Rate - Assumption (%)
    'optfvgry', # Options - Fair Value of Options Granted
    'optlifey', # Life of Options - Assumption (# yrs)
    'optrfry',  # Risk Free Rate - Assumption (%)
    'optvoly',  # Volatility - Assumption (%)
    'pdvcy',    # Cash Dividends on Preferred/Preference Stock (Cash Flow)
    'piy',      # Pretax Income
    'plly',     # Provision for Loan/Asset Losses
    'prstkccy', # Purchase of Common Stock (Cash Flow)
    'prstkcy',  # Purchase of Common and Preferred Stock
    'prstkpcy', # Purchase of Preferred/Preference Stock (Cash Flow)
    'rcay',     # Restructuring Cost After-tax
    'rcpy',     # Restructuring Cost Pretax
    'rdipay',   # In Process R&D Expense After-tax
    'rdipy',    # In Process R&D
    'recchy',   # Accounts Receivable - Decrease (Increase)
    'revty',    # Revenue - Total
    'rray',     # Reversal - Restructruring/Acquisition Aftertax
    'rrpy',     # Reversal - Restructruring/Acquisition Pretax
    'saley',    # Sales/Turnover (Net)
    'scstkcy',  # Sale of Common Stock (Cash Flow)
    'setay',    # Settlement (Litigation/Insurance) After-tax
    'setpy',    # Settlement (Litigation/Insurance) Pretax
    'sppey',    # Sale of Property
    'sppivy',   # Sale of PP&E and Investments - (Gain) Loss
    'spstkcy',  # Sale of Preferred/Preference Stock (Cash Flow)
    # Removed srety - Annual version (have sretq for quarterly earnings quality analysis)
    'sstky',    # Sale of Common and Preferred Stock
    'stkcoy',   # Stock Compensation Expense
    'stkcpay',  # After-tax stock compensation
    'tdcy',     # Deferred Income Taxes - Net (Cash Flow)
    'tfvcey',   # Total Fair Value Changes including Earnings
    'tiey',     # Interest Expense - Total (Financial Services)
    'tiiy',     # Interest Income - Total (Financial Services)
    'txachy',   # Income Taxes - Accrued - Increase (Decrease)
    'txdcy',    # Deferred Taxes (Statement of Cash Flows)
    'txdiy',    # Income Taxes - Deferred
    'txpdy',    # Income Taxes Paid
    'txty',     # Income Taxes - Total
    'uaolochy', # Other Assets and Liabilities - Net Change (Statement of Cash Flows)
    # Removed 7 utility FOF fields (NULL across all test stocks):
    # udfccy, ufretsdy, unwccy, ustdncy, utfdocy, utfoscy, uwkcapcy
    'udvpy',    # Preferred Dividend Requirements - Utility
    'ugiy',     # Gross Income (Income Before Interest Charges) - Utility
    'uniamiy',  # Net Income before Extraordinary Items After Noncontrolling Interest - Utility
    'unopincy', # Nonoperating Income (Net) - Other - Utility
    'uoisy',    # Other Internal Sources - Net (Cash Flow)
    'updvpy',   # Preference Dividend Requirements - Utility
    'uptacy',   # Utility Plant - Gross Additions (Cash Flow)
    'uspiy',    # Special Items - Utility
    'usubdvpy', # Subsidiary Preferred Dividends - Utility
    'utmey',    # Maintenance Expense - Total
    'wcapchy',  # Working Capital Changes - Total
    'wcapcy',   # Working Capital Change - Other - Increase/(Decrease)
    'wday',     # Writedowns After-tax
    'wdpy',     # Writedowns Pretax
    'xidocy',   # Extraordinary Items and Discontinued Operations (Statement of Cash Flows)
    'xidoy',    # Extraordinary Items and Discontinued Operations
    'xinty',    # Interest and Related Expense- Total
    'xiy',      # Extraordinary Items
    'xopry',    # Operating Expense- Total
    'xrdy',     # Research and Development Expense
    'xsgay',    # Selling, General and Administrative Expenses
    'adjex',    # Cumulative Adjustment Factor by Ex-Date
    'cshtrq',   # Common Shares Traded - Quarter
    'dvpspq',   # Dividends per Share - Pay Date - Quarter
    'dvpsxq',   # Div per Share - Exdate - Quarter
    'mkvaltq',  # Market Value - Total
    'prccq',    # Price Close - Quarter
    'prchq',    # Price High - Quarter
    'prclq'     # Price Low - Quarter
]

# Fields we removed that were actually NOT available though they are said to be on the wrds query GUI:
#   - add1
#   - addzip
#   - city
#   - conml
#   - ein
#   - fyrc
#   - ggroup
#   - gind
#   - gsector
#   - gsubind
#   - idbflag
#   - incorp
#   - ipodate
#   - naics
#   - prican
#   - prirow
#   - priusa
#   - state
#   - stko
#   - weburl

# For my reference, most if not all of this metadata is actually in the comp.company table


# Actual fields that exist in comp.fundq (SELECT * with a limit of 0)
actual_fields = [
    'gvkey', 'datadate', 'fyearq', 'fqtr', 'fyr', 'indfmt', 'consol', 'popsrc', 'datafmt', 'tic', 'cusip', 'conm', 
    'acctchgq', 'acctstdq', 'adrrq', 'ajexq', 'ajpq', 'bsprq', 'compstq', 'curcdq', 'curncdq', 'currtrq', 'curuscnq', 
    'datacqtr', 'datafqtr', 'finalq', 'ogmq', 'rp', 'scfq', 'srcq', 'staltq', 'updq', 'apdedateq', 'fdateq', 'pdateq', 'rdq', 
    'acchgq', 'acomincq', 'acoq', 'actq', 'altoq', 'ancq', 'anoq', 'aociderglq', 'aociotherq', 'aocipenq', 'aocisecglq', 
    'aol2q', 'aoq', 'apq', 'aqaq', 'aqdq', 'aqepsq', 'aqpl1q', 'aqpq', 'arcedq', 'arceepsq', 'arceq', 'atq', 'aul3q', 
    'billexceq', 'capr1q', 'capr2q', 'capr3q', 'capsftq', 'capsq', 'ceiexbillq', 'ceqq', 'cheq', 'chq', 'cibegniq', 'cicurrq', 
    'ciderglq', 'cimiiq', 'ciotherq', 'cipenq', 'ciq', 'cisecglq', 'citotalq', 'cogsq', 'csh12q', 'cshfd12', 'cshfdq', 'cshiq', 
    'cshopq', 'cshoq', 'cshprq', 'cstkcvq', 'cstkeq', 'cstkq', 'dcomq', 'dd1q', 'deracq', 'deraltq', 'derhedglq', 'derlcq', 
    'derlltq', 'diladq', 'dilavq', 'dlcq', 'dlttq', 'doq', 'dpacreq', 'dpactq', 'dpq', 'dpretq', 'drcq', 'drltq', 'dteaq', 
    'dtedq', 'dteepsq', 'dtepq', 'dvintfq', 'dvpq', 'epsf12', 'epsfi12', 'epsfiq', 'epsfxq', 'epspi12', 'epspiq', 'epspxq', 
    'epsx12', 'esopctq', 'esopnrq', 'esoprq', 'esoptq', 'esubq', 'fcaq', 'ffoq', 'finacoq', 'finaoq', 'finchq', 'findlcq', 
    'findltq', 'finivstq', 'finlcoq', 'finltoq', 'finnpq', 'finreccq', 'finrecltq', 'finrevq', 'finxintq', 'finxoprq', 
    'gdwlamq', 'gdwlia12', 'gdwliaq', 'gdwlid12', 'gdwlidq', 'gdwlieps12', 'gdwliepsq', 'gdwlipq', 'gdwlq', 'glaq', 'glcea12', 
    'glceaq', 'glced12', 'glcedq', 'glceeps12', 'glceepsq', 'glcepq', 'gldq', 'glepsq', 'glivq', 'glpq', 'hedgeglq', 'ibadj12', 
    'ibadjq', 'ibcomq', 'ibmiiq', 'ibq', 'icaptq', 'intaccq', 'intanoq', 'intanq', 'invfgq', 'invoq', 'invrmq', 'invtq', 
    'invwipq', 'ivaeqq', 'ivaoq', 'ivltq', 'ivstq', 'lcoq', 'lctq', 'lltq', 'lnoq', 'lol2q', 'loq', 'loxdrq', 'lqpl1q', 
    'lseq', 'ltmibq', 'ltq', 'lul3q', 'mibnq', 'mibq', 'mibtq', 'miiq', 'msaq', 'ncoq', 'niitq', 'nimq', 'niq', 'nopiq', 
    'npatq', 'npq', 'nrtxtdq', 'nrtxtepsq', 'nrtxtq', 'obkq', 'oepf12', 'oeps12', 'oepsxq', 'oiadpq', 'oibdpq', 'opepsq', 
    'optdrq', 'optfvgrq', 'optlifeq', 'optrfrq', 'optvolq', 'piq', 'pllq', 'pnc12', 'pncd12', 'pncdq', 'pnceps12', 'pncepsq', 
    'pnciapq', 'pnciaq', 'pncidpq', 'pncidq', 'pnciepspq', 'pnciepsq', 'pncippq', 'pncipq', 'pncpd12', 'pncpdq', 'pncpeps12', 
    'pncpepsq', 'pncpq', 'pncq', 'pncwiapq', 'pncwiaq', 'pncwidpq', 'pncwidq', 'pncwiepq', 'pncwiepsq', 'pncwippq', 'pncwipq', 
    'pnrshoq', 'ppegtq', 'ppentq', 'prcaq', 'prcd12', 'prcdq', 'prce12', 'prceps12', 'prcepsq', 'prcpd12', 'prcpdq', 'prcpeps12', 
    'prcpepsq', 'prcpq', 'prcraq', 'prshoq', 'pstknq', 'pstkq', 'pstkrq', 'rcaq', 'rcdq', 'rcepsq', 'rcpq', 'rdipaq', 'rdipdq', 
    'rdipepsq', 'rdipq', 'recdq', 'rectaq', 'rectoq', 'rectq', 'rectrq', 'recubq', 'req', 'retq', 'reunaq', 'revtq', 'rllq', 
    'rra12', 'rraq', 'rrd12', 'rrdq', 'rreps12', 'rrepsq', 'rrpq', 'rstcheltq', 'rstcheq', 'saleq', 'seqoq', 'seqq', 'seta12', 
    'setaq', 'setd12', 'setdq', 'seteps12', 'setepsq', 'setpq', 'spce12', 'spced12', 'spcedpq', 'spcedq', 'spceeps12', 
    'spceepsp12', 'spceepspq', 'spceepsq', 'spcep12', 'spcepd12', 'spcepq', 'spceq', 'spidq', 'spiepsq', 'spioaq', 'spiopq', 
    'spiq', 'sretq', 'stkcoq', 'stkcpaq', 'teqq', 'tfvaq', 'tfvceq', 'tfvlq', 'tieq', 'tiiq', 'tstknq', 'tstkq', 'txdbaq', 
    'txdbcaq', 'txdbclq', 'txdbq', 'txdiq', 'txditcq', 'txpq', 'txtq', 'txwq', 'uacoq', 'uaoq', 'uaptq', 'ucapsq', 'ucconsq', 
    'uceqq', 'uddq', 'udmbq', 'udoltq', 'udpcoq', 'udvpq', 'ugiq', 'uinvq', 'ulcoq', 'uniamiq', 'unopincq', 'uopiq', 'updvpq', 
    'upmcstkq', 'upmpfq', 'upmpfsq', 'upmsubpq', 'upstkcq', 'upstkq', 'urectq', 'uspiq', 'usubdvpq', 'usubpcvq', 'utemq', 
    'wcapq', 'wdaq', 'wddq', 'wdepsq', 'wdpq', 'xaccq', 'xidoq', 'xintq', 'xiq', 'xoprq', 'xopt12', 'xoptd12', 'xoptd12p', 
    'xoptdq', 'xoptdqp', 'xopteps12', 'xoptepsp12', 'xoptepsq', 'xoptepsqp', 'xoptq', 'xoptqp', 'xrdq', 'xsgaq', 'acchgy', 
    'afudccy', 'afudciy', 'amcy', 'aolochy', 'apalchy', 'aqay', 'aqcy', 'aqdy', 'aqepsy', 'aqpy', 'arcedy', 'arceepsy', 'arcey', 
    'capxy', 'cdvcy', 'chechy', 'cibegniy', 'cicurry', 'cidergly', 'cimiiy', 'ciothery', 'cipeny', 'cisecgly', 'citotaly', 'ciy', 
    'cogsy', 'cshfdy', 'cshpry', 'cstkey', 'depcy', 'derhedgly', 'dilady', 'dilavy', 'dlcchy', 'dltisy', 'dltry', 'doy', 'dpcy', 
    'dprety', 'dpy', 'dteay', 'dtedy', 'dteepsy', 'dtepy', 'dvpy', 'dvy', 'epsfiy', 'epsfxy', 'epspiy', 'epspxy', 'esubcy', 
    'esuby', 'exrey', 'fcay', 'ffoy', 'fiaoy', 'fincfy', 'finrevy', 'finxinty', 'finxopry', 'fopoxy', 'fopoy', 'fopty', 'fsrcoy', 
    'fsrcty', 'fuseoy', 'fusety', 'gdwlamy', 'gdwliay', 'gdwlidy', 'gdwliepsy', 'gdwlipy', 'glay', 'glceay', 'glcedy', 'glceepsy', 
    'glcepy', 'gldy', 'glepsy', 'glivy', 'glpy', 'hedgegly', 'ibadjy', 'ibcomy', 'ibcy', 'ibmiiy', 'iby', 'intpny', 'invchy', 
    'itccy', 'ivacoy', 'ivchy', 'ivncfy', 'ivstchy', 'miiy', 'ncoy', 'niity', 'nimy', 'niy', 'nopiy', 'nrtxtdy', 'nrtxtepsy', 
    'nrtxty', 'oancfy', 'oepsxy', 'oiadpy', 'oibdpy', 'opepsy', 'optdry', 'optfvgry', 'optlifey', 'optrfry', 'optvoly', 'pdvcy', 
    'piy', 'plly', 'pncdy', 'pncepsy', 'pnciapy', 'pnciay', 'pncidpy', 'pncidy', 'pnciepspy', 'pnciepsy', 'pncippy', 'pncipy', 
    'pncpdy', 'pncpepsy', 'pncpy', 'pncwiapy', 'pncwiay', 'pncwidpy', 'pncwidy', 'pncwiepsy', 'pncwiepy', 'pncwippy', 'pncwipy', 
    'pncy', 'prcay', 'prcdy', 'prcepsy', 'prcpdy', 'prcpepsy', 'prcpy', 'prstkccy', 'prstkcy', 'prstkpcy', 'rcay', 'rcdy', 
    'rcepsy', 'rcpy', 'rdipay', 'rdipdy', 'rdipepsy', 'rdipy', 'recchy', 'revty', 'rray', 'rrdy', 'rrepsy', 'rrpy', 'saley', 
    'scstkcy', 'setay', 'setdy', 'setepsy', 'setpy', 'sivy', 'spcedpy', 'spcedy', 'spceepspy', 'spceepsy', 'spcepy', 'spcey', 
    'spidy', 'spiepsy', 'spioay', 'spiopy', 'spiy', 'sppey', 'sppivy', 'spstkcy', 'srety', 'sstky', 'stkcoy', 'stkcpay', 'tdcy', 
    'tfvcey', 'tiey', 'tiiy', 'tsafcy', 'txachy', 'txbcofy', 'txbcoy', 'txdcy', 'txdiy', 'txpdy', 'txty', 'txwy', 'uaolochy', 
    'udfccy', 'udvpy', 'ufretsdy', 'ugiy', 'uniamiy', 'unopincy', 'unwccy', 'uoisy', 'updvpy', 'uptacy', 'uspiy', 'ustdncy', 
    'usubdvpy', 'utfdocy', 'utfoscy', 'utmey', 'uwkcapcy', 'wcapchy', 'wcapcy', 'wday', 'wddy', 'wdepsy', 'wdpy', 'xidocy', 
    'xidoy', 'xinty', 'xiy', 'xopry', 'xoptdqpy', 'xoptdy', 'xoptepsqpy', 'xoptepsy', 'xoptqpy', 'xopty', 'xrdy', 'xsgay', 
    'iid', 'exchg', 'cik', 'costat', 'fic', 'cshtrq', 'dvpspq', 'dvpsxq', 'mkvaltq', 'prccq', 'prchq', 'prclq', 'adjex'
]