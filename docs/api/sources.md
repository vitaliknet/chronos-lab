# Data Sources API

High-level functions for fetching OHLCV time series data from multiple sources.

## Overview

The `chronos_lab.sources` module provides unified interfaces for fetching data from:

- **Yahoo Finance**: Free market data via yfinance
- **Intrinio**: Institutional-quality financial data (requires API subscription)
- **ArcticDB**: Retrieve previously stored time series data

All functions return data in consistent formats with UTC timezone-aware timestamps.

## Functions

::: chronos_lab.sources.ohlcv_from_yfinance
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.sources.ohlcv_from_intrinio
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.sources.ohlcv_from_arcticdb
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.sources.securities_from_intrinio
    options:
      show_root_heading: true
      heading_level: 3
