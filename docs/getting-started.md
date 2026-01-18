# Getting Started

This guide will walk you through installing chronos-lab and building your first time series analysis workflow.

## Prerequisites

- Python 3.12 or higher
- macOS or Linux operating system is recommended
- Basic familiarity with pandas and time series data

## Installation

### Step 1: Choose Your Installation Type

chronos-lab uses a modular architecture. Choose the installation that matches your needs:

**For quick experiments** (Yahoo Finance only):
```bash
uv pip install chronos-lab[yfinance]
```

**For institutional data** (Intrinio API):
```bash
uv pip install chronos-lab[intrinio]
```

**For persistent storage** (ArcticDB):
```bash
uv pip install chronos-lab[yfinance,arcticdb]
```

**For everything**:
```bash
uv pip install chronos-lab[yfinance,intrinio,arcticdb,mcp]
```

### Step 2: Verify Installation

```python
import chronos_lab
from chronos_lab.sources import ohlcv_from_yfinance

print("chronos-lab installed successfully!")
```

On first import, chronos-lab will create `~/.chronos_lab/.env` with default configuration.

## Your First Workflow

### Fetching Data

Let's fetch some stock price data using Yahoo Finance:

```python
from chronos_lab.sources import ohlcv_from_yfinance

# Fetch daily prices for tech stocks
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='1y',  # Last year of data
    interval='1d'  # Daily bars
)

# View the data
print(prices.head())
print(f"\nShape: {prices.shape}")
print(f"Date range: {prices.index.get_level_values('date').min()} to {prices.index.get_level_values('date').max()}")
```

The returned DataFrame has a MultiIndex with `(date, symbol)` levels:

```
                                    open         high          low        close      volume
date                symbol
2024-01-17 05:00:00+00:00  AAPL    182.16  183.08999634  180.88999939  182.67999268  52242800
                           AMZN    153.39  153.88000488  151.27999878  153.38000488  45143900
                           GOOGL   141.70  142.72999573  140.50000000  141.82000732  21677100
                           MSFT    388.39  397.10998535  385.16998291  396.95001221  34049200
```

### Working with the Data

```python
import pandas as pd

# Get closing prices only
closes = prices['close'].unstack('symbol')
print(closes.head())

# Calculate daily returns
returns = closes.pct_change()

# Calculate correlation matrix
correlation = returns.corr()
print("\nCorrelation Matrix:")
print(correlation)

# Get specific symbol
aapl = prices.xs('AAPL', level='symbol')
print(f"\nAAPL data shape: {aapl.shape}")
```

### Adding Persistent Storage

chronos-lab provides two types of persistent storage, each optimized for different data:

- **ArcticDB**: High-performance time series storage for OHLCV price data
- **Datasets**: Structured data storage for portfolio composition, watchlists, security metadata, etc.

**Important**: Use ArcticDB for time series data and datasets for structured/metadata.

#### Storing Time Series Data (ArcticDB)

Install ArcticDB support if you haven't already:

```bash
uv pip install chronos-lab[arcticdb]
```

Now store your time series data for later use:

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch fresh data
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='1y'
)

# Store in ArcticDB
result = ohlcv_to_arcticdb(
    ohlcv=prices,
    library_name='yfinance',  # Library to store in
    adb_mode='write'  # Overwrite existing data
)

if result['statusCode'] == 0:
    print("✓ Data stored successfully!")
else:
    print(f"⚠ Some symbols failed: {result.get('skipped_symbols', [])}")
```

#### Storing Structured Datasets

Datasets are ideal for storing structured data needed in your research workflows:

- **Portfolio composition**: Holdings, weights, rebalance dates
- **Watchlists**: Custom symbol lists, sector groupings
- **Index composition**: S&P 500 constituents, sector mappings
- **Security metadata**: Company names, exchanges, sectors, market caps

Store datasets locally as JSON files or to DynamoDB for distributed workflows:

```python
from chronos_lab.sources import securities_from_intrinio
from chronos_lab.storage import to_dataset

# Example 1: Store security metadata from Intrinio
securities = securities_from_intrinio()

# Store as local dataset
result = to_dataset(
    dataset_name='securities_intrinio',
    dataset=securities.to_dict(orient='index')
)

# Example 2: Store portfolio composition
portfolio = {
    'AAPL': {'weight': 0.30, 'shares': 100, 'sector': 'Technology'},
    'MSFT': {'weight': 0.25, 'shares': 50, 'sector': 'Technology'},
    'JPM': {'weight': 0.20, 'shares': 75, 'sector': 'Financials'},
    'JNJ': {'weight': 0.15, 'shares': 60, 'sector': 'Healthcare'},
    'XOM': {'weight': 0.10, 'shares': 80, 'sector': 'Energy'}
}

result = to_dataset(
    dataset_name='my_portfolio',
    dataset=portfolio
)

# Example 3: Store custom watchlist
watchlist = {
    'tech_leaders': {
        'symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA'],
        'description': 'Large-cap technology leaders',
        'created': '2024-01-15'
    },
    'dividend_stocks': {
        'symbols': ['JNJ', 'PG', 'KO', 'PEP', 'MCD'],
        'description': 'Dividend aristocrats',
        'created': '2024-01-15'
    }
}

result = to_dataset(
    dataset_name='watchlists',
    dataset=watchlist
)

if result['statusCode'] == 0:
    print("✓ Dataset stored successfully!")
```

**Distributed Workflows**: In production environments, independent processes can write datasets to DynamoDB (use `ddb_` prefix), and other processes can read them with `from_dataset()`. Configure DynamoDB in `~/.chronos_lab/.env`:

```bash
# Dataset Settings
DATASET_LOCAL_PATH=~/.chronos_lab/datasets
DATASET_DDB_TABLE_NAME=my-datasets-table
DATASET_DDB_MAP='{"ddb_securities": {"pk": "DATASET#securities", "sk": "ticker"}}'
```

### Reading Stored Data

#### Reading Time Series Data (ArcticDB)

Retrieve OHLCV time series data from ArcticDB with flexible date filtering and pivoting:

```python
from chronos_lab.sources import ohlcv_from_arcticdb

# Read last 3 months
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='3m',
    library_name='yfinance'
)

# Read specific date range
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-06-30',
    library_name='yfinance'
)

# Read and pivot for analysis
wide_prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='1y',
    columns=['close'],  # Only close prices
    pivot=True,  # Reshape to wide format
    group_by='column',  # close_AAPL, close_MSFT, etc.
    library_name='yfinance'
)

print(wide_prices.head())
```

#### Reading Structured Datasets

Retrieve structured datasets that were stored with `to_dataset()`:

```python
from chronos_lab.sources import from_dataset
import pandas as pd

# Example 1: Read portfolio composition
portfolio_df = from_dataset(dataset_name='my_portfolio')
print("Portfolio Composition:")
print(portfolio_df)
print(f"\nTotal weight: {portfolio_df['weight'].sum()}")
print(f"Sector allocation:\n{portfolio_df.groupby('sector')['weight'].sum()}")

# Example 2: Read watchlists for analysis
watchlists = from_dataset(
    dataset_name='watchlists',
    output_dict=True  # Get as dictionary
)

# Use watchlist symbols to fetch prices
tech_symbols = watchlists['tech_leaders']['symbols']
prices = ohlcv_from_arcticdb(
    symbols=tech_symbols,
    period='1y',
    library_name='yfinance'
)

# Example 3: Read security metadata
securities = from_dataset(dataset_name='securities_metadata')
print(f"\nTotal securities: {len(securities)}")
print(f"Exchanges: {securities['exchange'].unique()}")

# Example 4: Read from DynamoDB (distributed workflows)
# In a distributed environment, one process writes datasets to DynamoDB
# and other processes read them
ddb_securities = from_dataset(dataset_name='ddb_securities')
print(f"\nDynamoDB securities: {len(ddb_securities)}")
```

**Use Cases for Datasets**:

- **Research workflows**: Load portfolio composition, then fetch prices for those symbols
- **Backtesting**: Store universe definitions, rebalance schedules, or factor definitions
- **Distributed systems**: One process updates security master details in DynamoDB, multiple processes read it
- **Configuration management**: Store strategy parameters, risk limits, or trading schedules

**Remember**: Datasets are for structured/metadata, not time series. Always use ArcticDB for OHLCV price data.

## Using Intrinio Data

For institutional-quality data, you'll need an Intrinio API subscription.

### Step 1: Configure API Key

Edit `~/.chronos_lab/.env` or set the `INTRINIO_API_KEY` environment variable:

```bash
# Intrinio API Settings
INTRINIO_API_KEY=your_api_key_here
```

### Step 2: Fetch Data

```python
from chronos_lab.sources import ohlcv_from_intrinio

# Fetch daily data
prices = ohlcv_from_intrinio(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-12-31',
    interval='daily'
)

# Fetch intraday data
intraday = ohlcv_from_intrinio(
    symbols=['SPY'],
    start_date='2024-01-15',
    end_date='2024-01-16',
    interval='5m'
)
```

## Common Patterns

### Pattern 1: Daily Update Workflow

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch latest day
latest = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='1d'
)

# Append to existing data
ohlcv_to_arcticdb(
    ohlcv=latest,
    library_name='yfinance',
    adb_mode='append'  # Append instead of overwrite
)
```

### Pattern 2: Multi-Symbol Analysis

```python
from chronos_lab.sources import ohlcv_from_arcticdb
import pandas as pd

# Get data in wide format
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA'],
    period='1y',
    columns=['close'],
    pivot=True,
    group_by='column',
    library_name='yfinance'
)

# Calculate returns
returns = prices.pct_change()

# Find best/worst performers
cumulative_returns = (1 + returns).cumprod() - 1
best_performer = cumulative_returns.iloc[-1].idxmax()
worst_performer = cumulative_returns.iloc[-1].idxmin()

print(f"Best: {best_performer}")
print(f"Worst: {worst_performer}")
```

### Pattern 3: Intraday Analysis

```python
from chronos_lab.sources import ohlcv_from_yfinance

# Fetch 5-minute bars for today
intraday = ohlcv_from_yfinance(
    symbols=['SPY', 'QQQ'],
    period='1d',
    interval='5m'
)

# Group by time of day
intraday_reset = intraday.reset_index()
intraday_reset['time'] = intraday_reset['date'].dt.time

# Average volume by time of day
avg_volume_by_time = intraday_reset.groupby('time')['volume'].mean()
print(avg_volume_by_time)
```

## Next Steps

- [Configuration Guide](configuration.md) - Detailed configuration options
- [API Reference](api/index.md) - Complete API documentation
- [Examples](examples.md) - More real-world examples

## Troubleshooting

### Issue: "No module named 'yfinance'"

**Solution**: Install the yfinance extra:
```bash
uv pip install chronos-lab[yfinance]
```

### Issue: "No module named 'arcticdb'"

**Solution**: Install the arcticdb extra:
```bash
uv pip install chronos-lab[arcticdb]
```

### Issue: Rate limit errors with Yahoo Finance

**Solution**: Yahoo Finance has rate limits. Add delays between requests or reduce the number of symbols per call (max 100).

### Issue: ArcticDB connection errors

**Solution**: Check that `~/.chronos_lab/.env` has valid `ARCTICDB_LOCAL_PATH` or `ARCTICDB_S3_BUCKET` configured.
