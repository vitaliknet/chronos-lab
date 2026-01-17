# Intrinio Wrapper API

Low-level wrapper for direct Intrinio SDK access.

!!! warning "Advanced API"
    This is a low-level API intended for advanced use cases. For most scenarios, use the high-level functions:

    - `ohlcv_from_intrinio()` for fetching OHLCV data
    - `securities_from_intrinio()` for fetching securities lists

## Overview

The `chronos_lab.intrinio` module provides the `Intrinio` class for direct access to the Intrinio SDK, custom pagination handling, and access to additional Intrinio APIs beyond stock prices.

### When to Use This API

- Custom pagination requirements
- Access to additional Intrinio APIs (Company, Exchange, etc.)
- Fine-grained control over API parameters
- Direct SDK method calls

### When NOT to Use This API

- Standard OHLCV data fetching (use `ohlcv_from_intrinio()`)
- Securities list fetching (use `securities_from_intrinio()`)
- Simple data retrieval operations

## Available SDK APIs

The `Intrinio` class exposes these SDK API objects:

- `_SecurityApi`: Security prices, fundamentals, ownership
- `_CompanyApi`: Company information, filings, news
- `_StockExchangeApi`: Exchange and market data

For complete SDK documentation, see: [https://docs.intrinio.com/documentation/python](https://docs.intrinio.com/documentation/python)

## Class

::: chronos_lab.intrinio.Intrinio
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - __init__
        - get_all_securities
        - get_security_stock_prices
