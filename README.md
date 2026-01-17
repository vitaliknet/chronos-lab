# chronos-lab

A lightweight Python library for time series analysis with a modular architecture. Install only what you need—from minimal data fetching to full-featured storage and MCP server capabilities.

## Installation

chronos-lab requires Python 3.13+ and runs on macOS and Linux.

### Core Installation

Install the minimal package with just NumPy, pandas, and Pydantic:

```bash
uv pip install chronos-lab
```

or with pip:

```bash
pip install chronos-lab
```

### Installation with Extras

chronos-lab follows a modular design—install additional features as needed:

#### Data Sources

```bash
# Yahoo Finance for quick market data analysis
uv pip install chronos-lab[yfinance]

# Intrinio for professional financial data
uv pip install chronos-lab[intrinio]
```

#### Storage & Infrastructure

```bash
# ArcticDB for high-performance time series storage
uv pip install chronos-lab[arcticdb]

# MCP server capabilities
uv pip install chronos-lab[mcp]
```

#### Combined Installations

```bash
# Simple use case: yfinance for on-the-fly analysis
uv pip install chronos-lab[yfinance]

# Advanced use case: MCP server with ArcticDB and S3
uv pip install chronos-lab[mcp,arcticdb]

# All features
uv pip install chronos-lab[yfinance,intrinio,arcticdb,mcp]
```

### Development Installation

```bash
git clone https://github.com/yourusername/chronos-lab.git
cd chronos-lab

# Install with all extras
uv sync --all-extras

# Or install with specific extras only
uv sync --extra yfinance --extra mcp
```
## Configuration

On first import of chronos-lab (e.g., `import chronos_lab` or `from chronos_lab.sources import ...`), the package automatically creates `~/.chronos_lab/.env` with default settings. This file can be edited to configure API keys, storage paths, and other options:

```bash
# View or edit configuration
nano ~/.chronos_lab/.env
```

The configuration file includes settings for data sources (Intrinio API), storage backends (ArcticDB local/S3), and logging levels. All settings can also be overridden via environment variables.
## Quick Start

## Fetching Market Data

### Yahoo Finance: OHLCV Time Series

Get daily price data for multiple symbols with minimal setup:

```python
from chronos_lab.sources import ohlcv_from_yfinance

# Download last year of daily data
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    period='1y'
)

# Or specify exact dates
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-12-31',
    interval='1d'
)
```

Returns a MultiIndex DataFrame with `(date, symbol)` levels for easy multi-symbol analysis.

#### Intraday Data

Fetch high-frequency data for algorithmic trading or analysis:

```python
# Get 5-minute bars for today
intraday = ohlcv_from_yfinance(
    symbols=['SPY', 'QQQ'],
    period='1d',
    interval='5m'
)
```

#### Working with Individual Symbols

Get separate DataFrames per symbol for focused analysis:

```python
# Returns dict: {'AAPL': DataFrame, 'MSFT': DataFrame}
prices_dict = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT'],
    period='6mo',
    output_dict=True
)

aapl_prices = prices_dict['AAPL']
```

### Intrinio

Access institutional-quality financial data:

```python
from chronos_lab.sources import ohlcv_from_intrinio

# Daily data (requires Intrinio API key)
prices = ohlcv_from_intrinio(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    interval='daily',
    api_key='your_api_key'  # or set in ~/.chronos_lab/.env
)

# Intraday bars
intraday = ohlcv_from_intrinio(
    symbols=['SPY'],
    start_date='2024-01-15',
    end_date='2024-01-16',
    interval='5m'
)
```

### Persistent Storage with ArcticDB

Store and version your time series data efficiently:

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch and store in one workflow
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    period='1y'
)

ohlcv_to_arcticdb(
    ohlcv=prices,
    library_name='yfinance',
    adb_mode='write'
)
```

Works with both MultiIndex DataFrames and symbol dictionaries. If library_name is not specified, the default library name is used from the configuration file.

### Reading from ArcticDB

Retrieve stored time series with flexible date filtering and formatting:

```python
from chronos_lab.sources import ohlcv_from_arcticdb

# Get last 3 months of data
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    period='3m',
    library_name='yfinance'
)

# Or specify exact date range
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT'],
    start_date='2026-01-01',
    end_date='2026-01-15',
    library_name='yfinance'
)
```

#### Wide Format for Analysis

Transform to wide format for correlation analysis or charting:

```python
# Pivot to wide format: one column per symbol
wide_prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='1y',
    columns=['close'],
    library_name='yfinance',
    pivot=True,
    group_by='column'  # Results in: close_AAPL, close_MSFT, etc.
)

# Calculate returns matrix
returns = wide_prices.pct_change()
correlation_matrix = returns.corr()
```

