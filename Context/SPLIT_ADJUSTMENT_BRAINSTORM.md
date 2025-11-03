# Split Adjustment Design Brainstorm

## 📊 Data Structure Trade-offs

### Option 1: Dictionary (Similar to Name Changes)
```python
splits_dict = {
    'AAPL': [
        ('2020-08-31', 4.0),  # (date, split_ratio)
        ('2014-06-09', 7.0),
        ('2005-02-28', 2.0)
    ],
    'TSLA': [...]
}
```

**Pros:**
- O(1) lookup by ticker (fast!)
- Familiar pattern (matches your name_change code)
- Easy to filter by ticker first
- Memory efficient

**Cons:**
- Still need to search dates within ticker's list
- Need to iterate dates to find splits before current row's date

**Complexity:** O(k) where k = splits per ticker (usually small, <10)

---

### Option 2: List of Splits Objects (Current)
```python
list_of_splits = [
    Splits(ticker='AAPL', date='2020-08-31', ...),
    Splits(ticker='AAPL', date='2014-06-09', ...),
    ...
]
```

**Pros:**
- Simple structure
- Easy to sort/filter
- What you have now

**Cons:**
- O(n) lookup (scan all splits for each dataframe row)
- For 1M rows × 100K splits = 100 billion checks 😱

**Complexity:** O(n × s) where n = dataframe rows, s = total splits

---

### Option 3: DataFrame
```python
splits_df = pd.DataFrame({
    'ticker': ['AAPL', 'AAPL', ...],
    'date': ['2020-08-31', '2014-06-09', ...],
    'split_ratio': [4.0, 7.0, ...]
})
```

**Pros:**
- Pandas merge/join operations (optimized!)
- Built-in date filtering
- Vectorized operations
- Indexed lookups possible

**Cons:**
- Slightly more memory
- Need to understand pandas merge syntax

**Complexity:** O(n log n) for merge (much better!)

---

## 🔍 Lookup Strategy Comparison

### Strategy A: Iterate DataFrame, Lookup Splits
```python
for idx, row in df.iterrows():
    ticker = row['symbol']
    row_date = row['date']
    
    # Find all splits for this ticker before row_date
    applicable_splits = find_splits(ticker, row_date, splits_dict)
    # Apply splits...
```

**Complexity:** O(n × k) where k = avg splits per ticker
- n = 1M rows, k = 5 splits → 5M operations ✅ GOOD

### Strategy B: Iterate Splits, Apply to DataFrame
```python
for split in list_of_splits:
    # Find all rows where ticker matches and date < split_date
    mask = (df['symbol'] == split.ticker) & (df['date'] < split.date)
    df.loc[mask, 'close'] *= split.ratio  # Apply adjustment
```

**Complexity:** O(s × n) where s = total splits
- s = 100K splits, n = 1M rows → 100 billion operations ❌ BAD

**But:** If you use vectorized pandas operations, this can be fast!

---

## 🚀 Pandas Internal Mechanisms

### Does Pandas Use Hash Lookups?

**Short answer:** Yes, but only for certain operations!

1. **Indexed Operations (O(1) lookup):**
```python
df.set_index(['ticker', 'date'])
df.loc[('AAPL', '2020-08-31')]  # Fast! Uses hash
```

2. **GroupBy (uses hash internally):**
```python
df.groupby('ticker')  # Fast! Groups using hash
```

3. **Merge/Join (optimized with hash or sort-merge):**
```python
df.merge(splits_df, on=['ticker', 'date'])  # Very fast!
```

4. **Regular .loc without index (slow - scans):**
```python
df.loc[df['ticker'] == 'AAPL']  # Scans all rows - slow!
```

### Recommendation: Use Merge!
Pandas merge is **highly optimized** (uses hash joins or sort-merge):
- Handles millions of rows efficiently
- Vectorized operations
- Clean, readable code

---

## 📅 Date Filtering Optimization

### Why Filter by Date Range?

1. **Reduce API calls/data:**
   - Only fetch splits in your OHLCV date range
   - Example: If OHLCV starts 2016-01-01, don't fetch splits from 2000

2. **Reduce storage:**
   - Less data in memory
   - Faster processing

3. **Polygon API supports date filtering:**
```python
client.list_splits(
    ticker=ticker,
    execution_date_gte='2016-01-01',  # Start of your OHLCV data
    execution_date_lte='2025-10-26',  # End of your OHLCV data
    order="asc",
    limit=1000
)
```

**Implementation:**
- Find min/max date from your OHLCV dataframe
- Pass those as `execution_date_gte` and `execution_date_lte`

---

## 🧮 Split Adjustment Algorithm

### Forward vs Backward Adjustment

**Forward Adjustment (most common):**
- Adjust historical prices DOWN (divide by ratio)
- Example: 4-for-1 split on 2020-08-31
  - All prices BEFORE 2020-08-31 get divided by 4
  - Close price $400 → $100 after adjustment

**Backward Adjustment:**
- Adjust historical prices UP (multiply by ratio)
- Less common, but sometimes used

### Algorithm Steps:

1. **Sort splits by date (descending - newest first)**
   - Apply newest splits first
   - This way, older prices get adjusted by ALL later splits

2. **For each row in OHLCV:**
   - Find all splits for that ticker that happened AFTER the row's date
   - Multiply all split ratios together
   - Divide OHLCV values by cumulative ratio

3. **What gets adjusted:**
   - ✅ Open, High, Low, Close (all divided by ratio)
   - ❌ Volume (multiplied by ratio - split increases shares)
   - ❌ Date (unchanged)

### Example Calculation:

```
AAPL splits:
- 2020-08-31: 4-for-1 (ratio = 4.0)
- 2014-06-09: 7-for-1 (ratio = 7.0)

Row date: 2014-01-01
Splits after this date: Both!
Cumulative ratio: 4.0 × 7.0 = 28.0

Adjusted close = original_close / 28.0
Adjusted volume = original_volume × 28.0
```

---

## 🎯 Recommended Approach

### Hybrid: Dictionary + Pandas Merge

**Step 1: Fetch splits (with date filtering)**
```python
# Get date range from OHLCV
min_date = df['date'].min()  # '2016-01-01'
max_date = df['date'].max()  # '2025-10-26'

# Fetch with date filtering
splits = await process_tickers(tickers, min_date, max_date)
```

**Step 2: Convert to DataFrame with cumulative ratios**
```python
splits_df = pd.DataFrame([...])  # ticker, date, ratio
splits_df = splits_df.sort_values('date', ascending=False)

# Calculate cumulative ratios per ticker
# (group by ticker, apply cumulative product)
```

**Step 3: Merge with OHLCV**
```python
# Merge to get applicable splits for each row
merged = df.merge(
    splits_df,
    on='ticker',
    how='left'
)

# Filter: only use splits where split_date > row_date
mask = merged['split_date'] > merged['row_date']
```

**Step 4: Vectorized adjustment**
```python
# Apply cumulative ratio using vectorized operations
df['close_adjusted'] = df['close'] / cumulative_ratio
df['volume_adjusted'] = df['volume'] * cumulative_ratio
```

---

## ⚡ Performance Comparison

| Approach | Operations | For 1M rows × 1000 splits |
|----------|-----------|---------------------------|
| List + iterrows | O(n × s) | ~1 billion (slow!) |
| Dict + iterrows | O(n × k) | ~5 million (fast!) |
| DataFrame merge | O(n log n) | ~20 million (very fast!) |

**Winner:** DataFrame merge approach! ✅

---

## 🧠 Key Insights

1. **Pandas merge is your friend** - it's highly optimized internally
2. **Date filtering saves time** - only fetch what you need
3. **Sort splits descending** - makes cumulative ratio calculation easier
4. **Vectorized > Loops** - let pandas do the heavy lifting
5. **Dictionary is good for ticker lookup** - but DataFrame is better for date-based merges

---

## 📝 Next Steps to Think About

1. **Do you need to preserve original values?**
   - Add `close_original`, `close_adjusted` columns?
   - Or overwrite original?

2. **What about reverse splits?**
   - Same logic, just ratio < 1.0
   - Example: 1-for-5 reverse split = ratio = 0.2

3. **What about dividends?**
   - Different adjustment needed
   - Usually subtract dividend amount from price

4. **Performance testing:**
   - Try with small dataset first
   - Measure time for each approach
   - Scale up to full dataset

