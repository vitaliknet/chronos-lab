# Interactive Brokers API

Low-level wrapper for Interactive Brokers market data operations.

!!! warning "Advanced API"
    This is a low-level API intended for advanced use cases, particularly streaming and real-time data. For most scenarios, use the high-level functions:

    - `ohlcv_from_ib()` for historical data retrieval
    - `ohlcv_from_ib_async()` for asynchronous historical data retrieval

## Overview

The `chronos_lab.ib` module provides the `IBMarketData` singleton class for direct control over Interactive Brokers TWS/Gateway connections, real-time tick subscriptions, streaming bar data, and contract management.

### When to Use This API

- Real-time tick data subscriptions
- Streaming bar data with live updates
- Custom contract qualification workflows
- Direct access to ib_async API
- Fine-grained control over subscription management
- Batch asynchronous operations with rate limiting

### When NOT to Use This API

- Simple historical data fetching (use `ohlcv_from_ib()`)
- Asynchronous historical data fetching (use `ohlcv_from_ib_async()`)
- One-time data retrieval without subscriptions

## Functions

::: chronos_lab.ib.get_ib
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.ib.map_interval_to_barsize
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.ib.calculate_ib_params
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.ib.hist_to_ohlcv
    options:
      show_root_heading: true
      heading_level: 3

## Class

::: chronos_lab.ib.IBMarketData
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - get_instance
        - connect
        - disconnect
        - get_hist_data
        - get_hist_data_async
        - sub_ticks
        - unsub_ticks
        - get_ticks
        - sub_bars
        - sub_bars_async
        - unsub_bars
        - get_bars
        - subscribe_bars
        - subscribe_bars_async
        - symbols_to_contracts
        - symbols_to_contracts_async
        - lookup_cds
        - lookup_cds_async
        - get_cds
