# API Reference

Complete API documentation for chronos-lab.

## High-Level APIs

These are the primary interfaces you'll use for most tasks:

- **[Data Sources](sources.md)** - Fetch data from Yahoo Finance, Intrinio, and ArcticDB
- **[Storage](storage.md)** - Persist data to ArcticDB
- **[Settings](settings.md)** - Configuration management

## Low-Level APIs

For advanced use cases requiring fine-grained control:

- **[ArcticDB Wrapper](arcdb.md)** - Direct ArcticDB operations
- **[Intrinio Wrapper](intrinio.md)** - Direct Intrinio SDK access

## Module Overview

### chronos_lab.sources

High-level functions for fetching OHLCV time series data:

- `ohlcv_from_yfinance()` - Fetch data from Yahoo Finance
- `ohlcv_from_intrinio()` - Fetch data from Intrinio API
- `ohlcv_from_arcticdb()` - Retrieve stored data from ArcticDB
- `securities_from_intrinio()` - Fetch securities lists from Intrinio

[View detailed documentation →](sources.md)

### chronos_lab.storage

High-level functions for persisting data:

- `ohlcv_to_arcticdb()` - Store OHLCV data in ArcticDB

[View detailed documentation →](storage.md)

### chronos_lab.settings

Configuration management:

- `Settings` - Pydantic model for application configuration
- `get_settings()` - Get cached settings instance

[View detailed documentation →](settings.md)

### chronos_lab.arcdb

Low-level ArcticDB wrapper (advanced use):

- `ArcDB` - Class for direct ArcticDB operations
  - `batch_store()` - Store multiple symbols
  - `batch_read()` - Read multiple symbols
  - `batch_update()` - Update existing symbols

[View detailed documentation →](arcdb.md)

### chronos_lab.intrinio

Low-level Intrinio SDK wrapper (advanced use):

- `Intrinio` - Class for direct Intrinio SDK access
  - `get_all_securities()` - Fetch securities lists
  - `get_security_stock_prices()` - Fetch price data

[View detailed documentation →](intrinio.md)

## Data Format Conventions

All OHLCV data in chronos-lab follows these conventions:

### Column Names
- `date`: UTC timezone-aware datetime
- `open`: Opening price
- `high`: High price
- `low`: Low price
- `close`: Closing price
- `volume`: Trading volume
- `symbol`: Ticker symbol
- `interval`: Data interval (for intraday data only)

### Index Structure

**MultiIndex DataFrame** (default):
```python
# (date, symbol) MultiIndex
                              open   high    low   close    volume
date                symbol
2024-01-17 05:00:00+00:00 AAPL   182.16  183.09  180.89  182.68  52242800
                          MSFT   388.39  397.11  385.17  396.95  34049200
```

**Dictionary Format** (when `output_dict=True`):
```python
{
    'AAPL': DataFrame with DatetimeIndex,
    'MSFT': DataFrame with DatetimeIndex,
    ...
}
```

**Wide/Pivoted Format** (when `pivot=True`):
```python
# DatetimeIndex with MultiIndex columns
date                      close_AAPL  close_MSFT  close_GOOGL
2024-01-17 05:00:00+00:00     182.68      396.95       141.82
2024-01-18 05:00:00+00:00     185.56      402.56       143.47
```

## Common Patterns

### Fetching and Storing

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch
prices = ohlcv_from_yfinance(symbols=['AAPL', 'MSFT'], period='1y')

# Store
ohlcv_to_arcticdb(ohlcv=prices, library_name='yfinance')
```

### Retrieving and Analyzing

```python
from chronos_lab.sources import ohlcv_from_arcticdb

# Retrieve in wide format
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT'],
    period='3m',
    columns=['close'],
    pivot=True,
    library_name='yfinance'
)

# Analyze
returns = prices.pct_change()
correlation = returns.corr()
```

## Error Handling

All functions return `None` on error and log details. Always check return values:

```python
prices = ohlcv_from_yfinance(symbols=['AAPL'], period='1y')
if prices is None:
    print("Failed to fetch data")
else:
    print(f"Fetched {len(prices)} rows")
```

For storage operations, check the status code:

```python
result = ohlcv_to_arcticdb(ohlcv=prices, library_name='yfinance')
if result['statusCode'] == 0:
    print("Success")
elif result['statusCode'] == 1:
    print(f"Partial failure: {result['skipped_symbols']}")
else:
    print("Complete failure")
```
