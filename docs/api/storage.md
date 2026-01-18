# Storage API

High-level functions for persisting time series data and structured datasets.

## Overview

The `chronos_lab.storage` module provides functions for storing data:

- **Time Series**: Store OHLCV data in ArcticDB (high-performance time series database)
- **Datasets**: Store structured data (portfolios, watchlists, metadata) locally or in DynamoDB

## Time Series Functions

::: chronos_lab.storage.ohlcv_to_arcticdb
    options:
      show_root_heading: true
      heading_level: 3

## Dataset Functions

::: chronos_lab.storage.to_dataset
    options:
      show_root_heading: true
      heading_level: 3
