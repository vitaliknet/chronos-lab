# Welcome to Chronos Lab

A lightweight Python library for time series financial data analysis with modular architecture.

## Quick Links

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } __Getting Started__

    ---

    Install chronos-lab and build your first data pipeline

    [:octicons-arrow-right-24: Installation & Quick Start](getting-started.md)

-   :material-cog:{ .lg .middle } __Configuration__

    ---

    Configure API keys, storage backends, and environment settings

    [:octicons-arrow-right-24: Configuration Guide](configuration.md)

-   :material-book-open:{ .lg .middle } __API Reference__

    ---

    Complete documentation for all functions and classes

    [:octicons-arrow-right-24: Browse API Docs](api/index.md)

-   :material-code-braces:{ .lg .middle } __Examples__

    ---

    Real-world patterns and workflows

    [:octicons-arrow-right-24: View Examples](examples.md)

</div>

## Key Features

**Modular Design**
: Install only what you need via optional extras (yfinance, intrinio, arcticdb, aws)

**Multiple Data Sources**
: Yahoo Finance for quick analysis, Intrinio for institutional data

**High-Performance Storage**
: ArcticDB for time series, datasets for structured metadata

**Intraday Support**
: 1m, 5m, 15m, 1h bars for algorithmic trading and backtesting

**Distributed Workflows**
: S3 backend for ArcticDB, DynamoDB for multi-process dataset coordination

**Type-Safe Configuration**
: Pydantic-based settings with environment variable overrides

## Quick Example

```python
from chronos_lab.sources import ohlcv_from_yfinance
from chronos_lab.storage import ohlcv_to_arcticdb

# Fetch data
prices = ohlcv_from_yfinance(symbols=['AAPL', 'MSFT'], period='1y')

# Store for later
ohlcv_to_arcticdb(ohlcv=prices, library_name='yfinance')
```

## Installation

```bash
# Quick start
uv pip install chronos-lab[yfinance,arcticdb]

# With Intrinio
uv pip install chronos-lab[yfinance,intrinio,arcticdb]

# With AWS support
uv pip install chronos-lab[yfinance,arcticdb,aws]
```

---

**Ready to dive in?** Start with the [Getting Started Guide](getting-started.md)
