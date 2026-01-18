# chronos-lab

A lightweight Python library for time series financial data analysis with modular architecture. Install only what you need—from minimal data fetching to full-featured storage capabilities.

## Features

✨ **Modular Design**: Install only what you need with optional extras
📊 **Multiple Data Sources**: Yahoo Finance (free) and Intrinio (institutional)
⚡ **High-Performance Storage**: ArcticDB for time series, datasets for metadata
🔄 **Flexible Outputs**: MultiIndex DataFrames or symbol dictionaries
📈 **Intraday Support**: 1m, 5m, 15m, 1h bars for algorithmic trading
🌐 **Distributed Workflows**: DynamoDB datasets for multi-process coordination

**[📚 Read the full documentation](https://vitaliknet.github.io/chronos-lab/)**

## Installation

Requires Python 3.12+ on macOS or Linux.

```bash
# Quick start: Yahoo Finance only
uv pip install chronos-lab[yfinance]

# With persistence: Add ArcticDB storage
uv pip install chronos-lab[yfinance,arcticdb]

# Professional: Add Intrinio data
uv pip install chronos-lab[yfinance,intrinio,arcticdb]

# With AWS: Add S3 and DynamoDB support
uv pip install chronos-lab[yfinance,arcticdb,aws]
```

Or with pip:
```bash
pip install chronos-lab[yfinance,arcticdb]
```
## Configuration

On first import, chronos-lab creates `~/.chronos_lab/.env` with default settings:

```bash
# Edit configuration
nano ~/.chronos_lab/.env
```

Configure API keys (Intrinio), storage paths (ArcticDB, datasets), and logging. All settings support environment variable overrides. See the [Configuration Guide](https://vitaliknet.github.io/chronos-lab/configuration/) for details.
## Quick Start

### Fetching Market Data

Get daily or intraday price data with one line:

```python
from chronos_lab.sources import ohlcv_from_yfinance

# Daily data for the last year
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    period='1y'
)

# 5-minute intraday bars
intraday = ohlcv_from_yfinance(
    symbols=['SPY', 'QQQ'],
    period='5d',
    interval='5m'
)
```

Returns a MultiIndex DataFrame with `(date, symbol)` levels for easy multi-symbol analysis.

### Persistent Storage

#### Time Series Storage (ArcticDB)

Store OHLCV data in high-performance ArcticDB for later retrieval:

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch and store
prices = ohlcv_from_yfinance(symbols=['AAPL', 'MSFT', 'GOOGL'], period='1y')
ohlcv_to_arcticdb(ohlcv=prices, library_name='yfinance', adb_mode='write')

# Read back with date filtering and pivoting
from chronos_lab.sources import ohlcv_from_arcticdb

wide_prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    period='3m',
    columns=['close'],
    pivot=True,
    group_by='column',
    library_name='yfinance'
)
```

#### Structured Data Storage (Datasets)

Store watchlists, portfolio compositions, and security metadata as datasets:

```python
from chronos_lab.storage import to_dataset
from chronos_lab.sources import from_dataset, ohlcv_from_yfinance

# Create and store a watchlist
watchlist = {
    'big_tech': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'],
    'semiconductors': ['NVDA', 'AMD', 'INTC', 'TSM', 'QCOM']
}
to_dataset(dataset_name='my_watchlist', dataset=watchlist)

# Load watchlist and fetch prices
saved_watchlist = from_dataset(dataset_name='my_watchlist', output_dict=True)
all_symbols = saved_watchlist['big_tech'] + saved_watchlist['semiconductors']

prices = ohlcv_from_yfinance(
    symbols=all_symbols,
    period='5d'
)

print(f"Fetched {len(all_symbols)} symbols: {prices.shape}")
```

**Datasets vs ArcticDB**: Use datasets for structured metadata (portfolios, watchlists, security details) and ArcticDB for time series (OHLCV prices). In distributed environments, datasets can be stored in DynamoDB for multi-process workflows.

### Institutional Data (Intrinio)

Access professional financial data with an Intrinio subscription:

```python
from chronos_lab.sources import ohlcv_from_intrinio

prices = ohlcv_from_intrinio(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    interval='daily'  # or '5m', '1h', etc.
)
```

## Documentation

- **[Getting Started Guide](https://vitaliknet.github.io/chronos-lab/getting-started/)** - Installation, first workflows, common patterns
- **[Configuration](https://vitaliknet.github.io/chronos-lab/configuration/)** - API keys, storage backends, environment setup
- **[API Reference](https://vitaliknet.github.io/chronos-lab/api/)** - Complete function and class documentation

## Why chronos-lab?

**Modular**: Start with just Yahoo Finance, add ArcticDB when you need persistence, scale to Intrinio for institutional data.

**Fast**: ArcticDB provides columnar storage optimized for time series queries. Datasets offer instant metadata lookups.

**Flexible**: MultiIndex DataFrames for analysis, pivoted wide format for charting, symbol dictionaries for individual processing.

**Production-Ready**: S3 backend for ArcticDB, DynamoDB for datasets, environment-based configuration for dev/staging/prod.

## License

MIT License - see [LICENSE](LICENSE) file for details.
