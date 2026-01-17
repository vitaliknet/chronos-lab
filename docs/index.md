# Chronos Lab

A lightweight Python library for time series analysis with a modular architecture. Install only what you need—from minimal data fetching to full-featured storage and MCP server capabilities.

## Features

- **Modular Design**: Install only the features you need via optional extras
- **Multiple Data Sources**: Yahoo Finance (yfinance) and Intrinio API integration
- **High-Performance Storage**: ArcticDB time series database with versioning support
- **MCP Server**: Model Context Protocol server capabilities (coming soon)
- **Type-Safe**: Built with Pydantic for configuration management
- **Well-Documented**: Comprehensive docstrings and examples

## Installation

chronos-lab requires Python 3.12+, tested to run on macOS and Linux.

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

=== "Data Sources"

    ```bash
    # Yahoo Finance for quick market data analysis
    uv pip install chronos-lab[yfinance]

    # Intrinio for professional financial data
    uv pip install chronos-lab[intrinio]
    ```

=== "Storage & Infrastructure"

    ```bash
    # ArcticDB for high-performance time series storage
    uv pip install chronos-lab[arcticdb]

    # MCP server capabilities
    uv pip install chronos-lab[mcp]
    ```

=== "Combined"

    ```bash
    # Simple use case: yfinance for on-the-fly analysis
    uv pip install chronos-lab[yfinance]

    # Advanced use case: ArcticDB and S3
    uv pip install chronos-lab[arcticdb,aws]

    # All features
    uv pip install chronos-lab[yfinance,intrinio,arcticdb,aws]
    ```

### Development Installation

```bash
git clone https://github.com/vitaliknet/chronos-lab.git
cd chronos-lab

# Install with all extras
uv sync --all-extras

# Or install with specific extras only
uv sync --extra yfinance --extra mcp
```

## Quick Start

### Fetching Market Data

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

### Persistent Storage with ArcticDB

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch and store in one workflow
prices = ohlcv_from_yfinance(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='1y'
)

ohlcv_to_arcticdb(
    ohlcv=prices,
    library_name='yfinance',
    adb_mode='write'
)
```

### Reading from ArcticDB

```python
from chronos_lab.sources import ohlcv_from_arcticdb

# Get last 3 months of data
prices = ohlcv_from_arcticdb(
    symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    period='3m',
    library_name='yfinance'
)

# Transform to wide format for correlation analysis
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

## Configuration

On first import of chronos-lab (e.g., `import chronos_lab` or `from chronos_lab.sources import ...`), the package automatically creates `~/.chronos_lab/.env` with default settings. This file can be edited to configure API keys, storage paths, and other options:

```bash
# View or edit configuration
nano ~/.chronos_lab/.env
```

The configuration file includes settings for data sources (Intrinio API), storage backends (ArcticDB local/S3), and logging levels. All settings can also be overridden via environment variables.

See the [Configuration Guide](configuration.md) for detailed setup instructions.

## Architecture

chronos-lab is organized into modular components:

- **[Data Sources](api/sources.md)**: High-level functions for fetching data from Yahoo Finance, Intrinio, and ArcticDB
- **[Storage](api/storage.md)**: Operations for persisting data to ArcticDB
- **[Settings](api/settings.md)**: Configuration management using Pydantic Settings
- **[Low-Level APIs](api/arcdb.md)**: Direct access to ArcticDB and Intrinio SDK for advanced use cases

## Next Steps

- [Getting Started Guide](getting-started.md) - Detailed walkthrough
- [Configuration](configuration.md) - Setup and configuration options
- [API Reference](api/index.md) - Complete API documentation
- [Examples](examples.md) - Real-world usage examples

## License

MIT License - see [LICENSE](https://github.com/vitaliknet/chronos-lab/blob/main/LICENSE) for details.
