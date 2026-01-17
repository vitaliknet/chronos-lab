# ArcticDB Wrapper API

Low-level wrapper for direct ArcticDB operations.

!!! warning "Advanced API"
    This is a low-level API intended for advanced use cases. For most scenarios, use the high-level functions:

    - `ohlcv_from_arcticdb()` for reading data
    - `ohlcv_to_arcticdb()` for writing data

## Overview

The `chronos_lab.arcdb` module provides the `ArcDB` class for direct control over ArcticDB operations, custom batch processing, and access to underlying ArcticDB APIs.

### When to Use This API

- Custom batch processing workflows
- Fine-grained control over versioning
- Direct access to ArcticDB Library API
- Advanced query operations

### When NOT to Use This API

- Standard data fetching (use `ohlcv_from_arcticdb()`)
- Standard data storage (use `ohlcv_to_arcticdb()`)
- Simple read/write operations

## Class

::: chronos_lab.arcdb.ArcDB
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - __init__
        - batch_store
        - batch_read
        - batch_update
