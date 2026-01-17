"""Persistent storage operations for OHLCV time series data using ArcticDB.

This module provides high-level functions for storing OHLCV data in ArcticDB,
a high-performance time series database. Data can be stored from either MultiIndex
DataFrames or dictionaries of DataFrames, with automatic symbol-level organization.

Typical Usage:
    Store data fetched from external sources:

        >>> from chronos_lab.sources import ohlcv_from_yfinance
        >>> from chronos_lab.storage import ohlcv_to_arcticdb
        >>>
        >>> # Fetch data
        >>> prices = ohlcv_from_yfinance(
        ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
        ...     period='1y'
        ... )
        >>>
        >>> # Store in ArcticDB
        >>> result = ohlcv_to_arcticdb(
        ...     ohlcv=prices,
        ...     library_name='yfinance',
        ...     adb_mode='write'
        ... )
        >>> print(result['statusCode'])  # 0 on success
"""

from chronos_lab import logger
from chronos_lab.settings import get_settings
from typing import Optional, Dict
import pandas as pd


def ohlcv_to_arcticdb(
        *,
        ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
        library_name: Optional[str] = None,
        adb_mode: str = 'write'
) -> Dict[str, int]:
    """Store OHLCV data to ArcticDB library for persistent time series storage.

    Accepts OHLCV data in multiple formats and stores it in ArcticDB with symbol-level
    organization. Automatically splits MultiIndex DataFrames by symbol for efficient
    per-symbol versioning and retrieval.

    Args:
        ohlcv: OHLCV data in one of two formats:
            - MultiIndex DataFrame with ('date', 'id'/'symbol') levels
            - Dictionary mapping symbols to individual DataFrames
        library_name: ArcticDB library name for storage. If None, uses
            ARCTICDB_DEFAULT_LIBRARY_NAME from ~/.chronos_lab/.env configuration.
        adb_mode: Storage mode for ArcticDB operations:
            - 'write': Overwrite existing data (default)
            - 'append': Append new data to existing symbols

    Returns:
        Dictionary with status information:
            - 'statusCode': 0 on success, -1 on failure, 1 if some symbols failed
            - 'skipped_symbols': List of symbols that failed to store (if any)

    Raises:
        None: Errors are logged but not raised. Check statusCode in return value.

    Examples:
        Store MultiIndex DataFrame from Yahoo Finance:
            >>> from chronos_lab.sources import ohlcv_from_yfinance
            >>> from chronos_lab.storage import ohlcv_to_arcticdb
            >>>
            >>> # Fetch data
            >>> prices = ohlcv_from_yfinance(
            ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
            ...     period='1y'
            ... )
            >>>
            >>> # Store in ArcticDB
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=prices,
            ...     library_name='yfinance',
            ...     adb_mode='write'
            ... )
            >>> if result['statusCode'] == 0:
            ...     print("Successfully stored data")

        Store dictionary of DataFrames from Intrinio:
            >>> from chronos_lab.sources import ohlcv_from_intrinio
            >>>
            >>> # Fetch as dictionary
            >>> prices_dict = ohlcv_from_intrinio(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='3mo',
            ...     interval='daily',
            ...     output_dict=True
            ... )
            >>>
            >>> # Store dictionary directly
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=prices_dict,
            ...     library_name='intrinio'
            ... )

        Append new data to existing symbols:
            >>> # Fetch latest data
            >>> new_prices = ohlcv_from_yfinance(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='1d'
            ... )
            >>>
            >>> # Append to existing data
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=new_prices,
            ...     library_name='yfinance',
            ...     adb_mode='append'
            ... )

        Handle partial failures:
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=prices,
            ...     library_name='yfinance'
            ... )
            >>> if result['statusCode'] == 1:
            ...     print(f"Failed symbols: {result['skipped_symbols']}")
            >>> elif result['statusCode'] == 0:
            ...     print("All symbols stored successfully")

    Note:
        - Input DataFrame must have exactly 2-level MultiIndex: ('date', 'id'/'symbol')
        - Each symbol is stored as a separate versioned entity in ArcticDB
        - Storage mode 'write' overwrites existing data; use 'append' to add new rows
        - Previous versions are pruned automatically to save space
        - All timestamps should be UTC timezone-aware
    """
    from chronos_lab.arcdb import ArcDB

    response = {
        'statusCode': 0,
    }

    if library_name is None:
        settings = get_settings()
        library_name = settings.arcticdb_default_library_name

    if isinstance(ohlcv, pd.DataFrame):
        if ohlcv.index.nlevels != 2:
            logger.error(f"Expected MultiIndex with 2 levels, got {ohlcv.index.nlevels}")
            response['statusCode'] = -1
            return response

        level_0_name = ohlcv.index.names[0]
        level_1_name = ohlcv.index.names[1]

        if level_0_name != 'date' or level_1_name not in ['id', 'symbol']:
            logger.error(
                f"Index levels are ('{level_0_name}', '{level_1_name}'), expected ('date', 'id') or ('date', 'symbol')")
            response['statusCode'] = -1
            return response

        ohlcv_dict = dict(tuple(ohlcv.reset_index(level=1).groupby(level_1_name)))
    else:
        ohlcv_dict = ohlcv

    try:
        ac = ArcDB(library_name=library_name)
        ac_res = ac.batch_store(data_dict=ohlcv_dict, mode=adb_mode, prune_previous_versions=True)

        if ac_res['statusCode'] == 0:
            logger.info("Successfully stored prices for %s symbols in ArcticDB", len(ohlcv_dict))
        else:
            logger.error("Failed to store data in ArcticDB")
            response['statusCode'] = -1
    except Exception as e:
        logger.error("Exception while storing in ArcticDB: %s", str(e))
        response['statusCode'] = -1

    return response


__all__ = [
    'ohlcv_to_arcticdb'
]
