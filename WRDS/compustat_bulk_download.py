from dataclasses import fields
import os
import wrds
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

load_dotenv()
os.environ['PGPASSFILE'] = os.getenv("PGPASS_PATH")
wrds_username = os.getenv("WRDS_username")

OUTPUT_DIR = Path("data/compustat_fundamentals_q")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = 2010
END_YEAR = 2022

# Initialize a connection object using with context manager
with wrds.Connection(wrds_username=wrds_username) as db:

    # Primary Compustat library:
    # comp  -----> Compustat North America
    
    # Key Compustat tables in comp:
    # comp.funda     -----> Fundamentals Annual (yearly financial statements)
    # comp.fundq     -----> Fundamentals Quarterly (quarterly financial statements)
    # comp.company   -----> Company information
    # comp.names     -----> Company names and identifiers

    table = 'fundq'
    
    for year in range(START_YEAR, END_YEAR):
      print(f"Downloading {year}...")

      # Example: Get annual fundamentals for Apple
      sample_query = f"""
      SELECT 
      {','.join(fields)}      
      FROM comp.{table}
      WHERE YYYYMMDD >= '{year}0101'
        AND YYYYMMDD < '{year+1}0101'
        AND datafmt = 'STD'
        AND consol = 'C'
        AND indfmt = 'INDL'
      ORDER BY YYYYMMDD, permno
      """
      
      try:
          df = db.raw_sql(sample_query)
          print("\nCompustat Annual Fundamentals - AAPL:")
          print(df)
      except Exception as e:
          print(f"\nFailed: {e}")

field_list = [
  'gvkey', # Global Company Key, a unique identifier and pkey for each co. in the DB
  'conm',   # Company Name
  'tic',    # Ticker Symbol
  'cusip',  # cusip
  'cik',    # cik
  'exchg',  # Primary exchange, defined for both Xpressfeed and Compustat, need EXCHGCD or EXCHGESC
  'fyr',    # Fiscal Year End Month (month end for cos account fiscal year)
  'add1',   # First address line on tax returns
  'addzip', # Zip code
  'city',   # City of HQ
  'conml',  # Company legal name as reported in EDGAR SEC
  'ein',    # EIN 
  'fyrc',   # Fiscal Year-end Month - Current, designates the month end for a co.s accounting year
  'ggroup', # GIC Groups
  'gind',   # GIC Industries
  'gsector', # GIC Sectors
  'gsubind', # GIC Sub Industries
  'idbflag', # Indicates the source of the data for the company, (B both NA and international), (D only NA) (I International)
  'incorp',  # State of incorporation
  'ipodate', # IPO date
  'naics',   # North American Industry Classification Code
  'prican',  # Primary issue for the company (Canada)
  'prirow',  # Primary issue for the company (Rest of World)
  'priusa',  # Primary issue for the company (USA)
  'state',   # State of company HQ
  'stko',    # Stock Ownership - Identifies the stock ownership of each company, 
            # 0 - publicly traded
            # 1 - sub of pub traded co
            # 2 - sub of non pud traded co
            # 3 - non major exchange (OTC etc)
            # 4 - undergone LBO
  'weburl',  # Homepage web URL
  'acctchgq',# Adoption of accounting changes
  'acctstdq', # Accounting standard
  'ajexq',   # Adjustment Factor, cumulative by Ex-Date, a ratio that enables you to adjust per share data
  'ajpq',    # Identical to above except by Pay-Date
  'apdedateq', # Actual Period End Date - the actual date the company closes accounting for the period
  'bsprq',   # Balance Sheet Presentation - Contains the code that identifies a companys use of either a classified or unclassified balance sheet
  'compstq', # Comparability Status, important, view codes for more info
  'curncdq', # Native currency company reports its financial data in
  'currtrq', # Currency translation rate, the rate used to translate int currency amounts to USD, effective as of the companys balance sheet date
  'curuscnq', # Translate CAD into USD
  'datacqtr', # Calendar Data Year and Quarter - This item is the calendar quarter that the fiscal quarter most closely represents
  'datafqtr', # Fiscal Quarter by Year
  'fdateq',   # Final Date - The date the data is finalized for that year or quarter
  'fqtr',     # Fiscal Quarter
  'pdateq',   # Date the data is updated for the company
  'rdq',      # Report Date of Earnings (IMPORTANT), represents the date on which quarterly EPS are first publicly reported
  'staltq',   # Status Alert - Co is undergoing bankcruptcy or LBO
  'updq',     # Update Code - This item contains the code that identifies the updated status of data for the period. It consists of a one-character numeric code.
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
  'aqdq',     # Acquisition/Merger Diluted EPS Effect
  'aqepsq',   # Acquisition/Merger Basic EPS Effect
  'aqpl1q',   # Assets Level1 (Quoted Prices)
  'aqpq',     # Acquisition/Merger Pretax
  'arcedq',   # As Reported Core - Diluted EPS Effect
  'arceepsq', # As Reported Core - Basic EPS Effect
  'arceq',    # As Reported Core - After-tax
  'atq',      # Assets - Total
  'aul3q',    # Assets Level3 (Unobservable)
  'billexceq', # Billings in Excess of Cost & Earnings
  'capr1q',   # Risk-Adjusted Capital Ratio - Tier 1
  'capr2q',   # Risk-Adjusted Capital Ratio - Tier 2
  'capr3q',   # Risk-Adjusted Capital Ratio - Combined
  'capsftq',  # Capitalized Software
  'capsq',    # Capital Surplus/Share Premium Reserve
  'ceiexbillq', # Cost & Earnings in Excess of Billings
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
  'dpacreq',  # Accumulated Depreciation of RE Property
  'dpactq',   # Depreciation, Depletion and Amortization (Accumulated)
  'dpq',      # Depreciation and Amortization - Total
  'dpretq',   # Depr/Amort of Property
  'drcq',     # Deferred Revenue - Current
  'drltq',    # Deferred Revenue - Long-term
  'dteaq',    # Extinguishment of Debt After-tax
  'dtedq',    # Extinguishment of Debt Diluted EPS Effect
  'dteepsq',  # Extinguishment of Debt Basic EPS Effect
  'dtepq',    # Extinguishment of Debt Pretax
  'dvintfq',  # Dividends & Interest Receivable (Cash Flow)
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
  'ffoq',     # Funds From Operations (REIT)
  'finacoq',  # Finance Division Other Current Assets, Total
  'finaoq',   # Finance Division Other Long-Term Assets, Total
  'finchq',   # Finance Division - Cash
  'findlcq',  # Finance Division Long-Term Debt Current
  'findltq',  # Finance Division Debt Long-Term
  'finivstq', # Finance Division Short-Term Investments
  'finlcoq',  # Finance Division Other Current Liabilities, Total
  'finltoq',  # Finance Division Other Long Term Liabilities, Total
  'finnpq',   # Finance Division Notes Payable
  'finreccq', # Finance Division Current Receivables
  'finrecltq', # Finance Division Long-Term Receivables
  'finrevq',  # Finance Division Revenue
  'finxintq', # Finance Division Interest Expense
  'finxoprq', # Finance Division Operating Expense
  'gdwlamq',  # Amortization of Goodwill
  'gdwlia12', # Impairments of Goodwill AfterTax - 12mm
  'gdwliaq',  # Impairment of Goodwill After-tax
  'gdwlid12', # Impairments Diluted EPS - 12mm
  'gdwlidq',  # Impairment of Goodwill Diluted EPS Effect
  'gdwlieps12', # Impairment of Goodwill Basic EPS Effect 12MM
  'gdwliepsq', # Impairment of Goodwill Basic EPS Effect
  'gdwlipq',  # Impairment of Goodwill Pretax
  'gdwlq',    # Goodwill (net)
  'glaq',     # Gain/Loss After-Tax
  'glcea12',  # Gain/Loss on Sale (Core Earnings Adjusted) After-tax 12MM
  'glceaq',   # Gain/Loss on Sale (Core Earnings Adjusted) After-tax
  'glced12',  # Gain/Loss on Sale (Core Earnings Adjusted) Diluted EPS Effect 12MM
  'glcedq',   # Gain/Loss on Sale (Core Earnings Adjusted) Diluted EPS
  'glceeps12', # Gain/Loss on Sale (Core Earnings Adjusted) Basic EPS Effect 12MM
  'glceepsq', # Gain/Loss on Sale (Core Earnings Adjusted) Basic EPS Effect
  'glcepq',   # Gain/Loss on Sale (Core Earnings Adjusted) Pretax
  'gldq',     # Gain/Loss Diluted EPS Effect
  'glepsq',   # Gain/Loss Basic EPS Effect
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
  'nrtxtdq',  # Nonrecurring Income Taxes Diluted EPS Effect
  'nrtxtepsq', # Nonrecurring Income Taxes Basic EPS Effect
  'nrtxtq'  # Nonrecurring Income Taxes - After-tax
]