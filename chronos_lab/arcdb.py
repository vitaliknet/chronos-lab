"""Low-level ArcticDB wrapper for high-performance time series storage operations.

This module provides the ArcDB class, a low-level wrapper around ArcticDB for managing
time series data storage with support for local (LMDB), S3, and in-memory backends.

**IMPORTANT**: This is a low-level wrapper intended for internal use. For most use cases,
prefer the high-level functions in chronos_lab.sources and chronos_lab.storage modules:
    - Use ohlcv_from_arcticdb() for reading data
    - Use ohlcv_to_arcticdb() for writing data

Direct usage of this module should be limited to advanced scenarios requiring fine-grained
control over ArcticDB operations, custom batch processing, or direct access to underlying
ArcticDB APIs.

Backend Configuration:
    Local (LMDB):
        Set ARCTICDB_LOCAL_PATH in ~/.chronos_lab/.env
        Example: ARCTICDB_LOCAL_PATH=~/.chronos_lab/arcticdb

    AWS S3:
        Prerequisites:
            1. Install aws extra: pip install chronos-lab[arcticdb,aws]
            2. Configure AWS CLI: aws configure (creates ~/.aws/credentials)
            3. Set AWS_PROFILE environment variable (if using named profiles)
            4. Set ARCTICDB_S3_BUCKET in ~/.chronos_lab/.env

        Example:
            ARCTICDB_S3_BUCKET=my-timeseries-bucket
            AWS_PROFILE=my-profile  # Optional, exported as environment variable

    In-Memory:
        No configuration needed (not recommended for production)

Advanced API Access:
    The ArcDB class exposes underlying ArcticDB objects for advanced operations:
        - _ac: Arctic instance (connection to storage backend)
        - _lib: Library instance (specific library within Arctic)

    For detailed API documentation, refer to:
        - Arctic API: https://docs.arcticdb.io/dev/api/arctic/
        - Library API: https://docs.arcticdb.io/dev/api/library/

Typical Usage:
    Basic operations (internal use):
        >>> from chronos_lab.arcdb import ArcDB
        >>>
        >>> # Initialize connection
        >>> ac = ArcDB(library_name='yfinance')
        >>>
        >>> # Store multiple symbols
        >>> data_dict = {'AAPL': aapl_df, 'MSFT': msft_df}
        >>> result = ac.batch_store(data_dict, mode='write')
        >>>
        >>> # Read multiple symbols
        >>> result = ac.batch_read(symbol_list=['AAPL', 'MSFT'])
        >>> df = result['payload']

    Advanced direct API access:
        >>> ac = ArcDB(library_name='yfinance')
        >>> # Access underlying Arctic instance
        >>> arctic = ac._ac
        >>> # Access underlying Library instance
        >>> library = ac._lib
        >>> # Use ArcticDB API directly
        >>> library.list_symbols()
"""

from chronos_lab import logger
from chronos_lab.settings import get_settings
import pandas as pd
import os
import arcticdb as adb
import concurrent.futures
from pathlib import Path


class ArcDB:
    """Low-level wrapper for ArcticDB time series database operations.

    Provides batch storage and retrieval operations with support for multiple storage
    backends (local LMDB, AWS S3, in-memory). Manages connection lifecycle and provides
    access to underlying ArcticDB API objects for advanced operations.

    **NOTE**: This is a low-level class. For typical use cases, prefer high-level
    functions in chronos_lab.sources and chronos_lab.storage modules.

    Attributes:
        _ac: Arctic instance (connection to storage backend). For advanced operations,
            see https://docs.arcticdb.io/dev/api/arctic/
        _lib: Library instance (specific library within Arctic). For advanced operations,
            see https://docs.arcticdb.io/dev/api/library/
        _bucket_name: S3 bucket name (if using S3 backend)
        _local_path: Local filesystem path (if using LMDB backend)
        _library_name: Name of the ArcticDB library

    Examples:
        Basic usage with local storage:
            >>> from chronos_lab.arcdb import ArcDB
            >>> import pandas as pd
            >>>
            >>> # Initialize connection
            >>> ac = ArcDB(library_name='my_data')
            >>>
            >>> # Store data
            >>> data = {'AAPL': aapl_df, 'MSFT': msft_df}
            >>> result = ac.batch_store(data, mode='write')
            >>>
            >>> # Read data
            >>> result = ac.batch_read(['AAPL', 'MSFT'])
            >>> df = result['payload']

        AWS S3 backend:
            >>> # Requires AWS CLI configuration and aws extra: pip install chronos-lab[arcticdb,aws]
            >>> # export AWS_PROFILE=my-profile (if using named profiles)
            >>> ac = ArcDB(
            ...     library_name='my_data',
            ...     bucket_name='my-timeseries-bucket'
            ... )

        Advanced API access:
            >>> ac = ArcDB(library_name='my_data')
            >>> # List all symbols
            >>> symbols = ac._lib.list_symbols()
            >>> # Get symbol metadata
            >>> metadata = ac._lib.get_info('AAPL')
            >>> # Direct read with query
            >>> df = ac._lib.read('AAPL', date_range=(start_date, end_date)).data
    """

    def __init__(self,
                 *,
                 bucket_name=None,
                 local_path=None,
                 library_name):
        """Initialize ArcticDB connection with specified backend.

        Establishes connection to ArcticDB using configuration from ~/.chronos_lab/.env
        or provided parameters. Automatically creates library if it doesn't exist.

        Args:
            bucket_name: AWS S3 bucket name for S3 backend. If None, uses
                ARCTICDB_S3_BUCKET from configuration. Takes precedence over local_path.
            local_path: Local filesystem path for LMDB backend. If None, uses
                ARCTICDB_LOCAL_PATH from configuration. Ignored if bucket_name is set.
            library_name: Name of the ArcticDB library to use or create.

        Raises:
            Exception: If connection initialization fails (logged and re-raised).

        Note:
            - Backend priority: S3 > Local LMDB > In-memory
            - S3 backend requires aws extra and AWS CLI configuration
            - Local path is created automatically if it doesn't exist
            - In-memory backend used if neither S3 nor local path configured
        """

        settings = get_settings()

        if not bucket_name:
            self._bucket_name = settings.arcticdb_s3_bucket
        else:
            self._bucket_name = bucket_name

        if not local_path:
            self._local_path = Path(settings.arcticdb_local_path).expanduser()
        else:
            self._local_path = Path(local_path).expanduser()

        self._library_name = library_name
        self._ac = None
        self._lib = None
        self._initialize_connection()

    def _initialize_connection(self):
        try:
            if self._bucket_name:
                from chronos_lab.aws import session

                uri = f"s3://s3.{session.region_name}.amazonaws.com:{self._bucket_name}?aws_auth=true"
                logger.info(f"Initializing ArcticDB with S3 backend using bucket: {uri}")
                self._ac = adb.Arctic(uri)
            elif self._local_path:
                logger.info(f"Initializing ArcticDB with local backend using path: {self._local_path}")
                if not os.path.exists(self._local_path):
                    os.makedirs(self._local_path)
                uri = f"lmdb://{self._local_path}"
                self._ac = adb.Arctic(uri)
            else:
                logger.warning(
                    "No storage backend specified. Using in-memory storage (not recommended for production).")
                self._ac = adb.Arctic("memory://")

            self._lib = self._ac.get_library(self._library_name, create_if_missing=True)
            logger.info(f"Successfully connected to ArcticDB library: {self._library_name}")

        except Exception as e:
            logger.error(f"Failed to initialize ArcticDB connection: {str(e)}")
            raise

    def batch_store(self,
                    data_dict,
                    mode='append',
                    **kwargs):
        """Store multiple symbols in batch with write or append mode.

        Writes or appends DataFrames for multiple symbols in a single batch operation.
        Each symbol is stored as a separate versioned entity in ArcticDB.

        Args:
            data_dict: Dictionary mapping symbol names (str) to pandas DataFrames.
                Each DataFrame should have a DatetimeIndex.
            mode: Storage mode, either 'append' (default) or 'write'.
                - 'append': Add new rows to existing data
                - 'write': Overwrite existing data completely
            **kwargs: Additional keyword arguments passed to ArcticDB write/append
                operations (e.g., prune_previous_versions=True).

        Returns:
            Dictionary with status information:
                - 'statusCode': 0 on complete success, 1 if some symbols failed,
                  -1 on complete failure
                - 'skipped_symbols': List of symbols that failed to store

        Examples:
            Write mode (overwrite):
                >>> ac = ArcDB(library_name='yfinance')
                >>> data = {
                ...     'AAPL': aapl_df,
                ...     'MSFT': msft_df,
                ...     'GOOGL': googl_df
                ... }
                >>> result = ac.batch_store(
                ...     data,
                ...     mode='write',
                ...     prune_previous_versions=True
                ... )
                >>> print(f"Status: {result['statusCode']}")

            Append mode (add new data):
                >>> new_data = {'AAPL': new_aapl_df, 'MSFT': new_msft_df}
                >>> result = ac.batch_store(new_data, mode='append')
                >>> if result['skipped_symbols']:
                ...     print(f"Failed: {result['skipped_symbols']}")
        """

        response = {
            'statusCode': 0,
            'skipped_symbols': []
        }

        try:
            if not isinstance(data_dict, dict):
                logger.error("data_dict must be a dictionary")
                response['statusCode'] = -1
                return response

            payloads = []
            for symbol_key, data in data_dict.items():
                if not isinstance(data, pd.DataFrame):
                    logger.warning(f"Data for {symbol_key} is not a DataFrame, skipping")
                    response['skipped_symbols'].append(symbol_key)
                    continue

                payloads.append(adb.WritePayload(symbol_key, data))
                logger.debug(f"Created WritePayload for {symbol_key} with {len(data)} records")

            if not payloads:
                logger.warning("No valid data to write in batch")
                response['statusCode'] = -1
                return response

            if mode == 'append':
                results = self._lib.append_batch(payloads, **kwargs)
            else:
                results = self._lib.write_batch(payloads, **kwargs)

            for i, result in enumerate(results):
                symbol_key = payloads[i].symbol
                if hasattr(result, 'error_code') and result.error_code:
                    logger.error(f"Error writing data for {symbol_key}: {result}")
                    response['skipped_symbols'].append(symbol_key)
                else:
                    logger.info(f"Wrote data for {symbol_key}")

            if response['skipped_symbols']:
                response['statusCode'] = 1
                logger.warning(f"Failed to write {len(response['skipped_symbols'])} symbols")

            return response

        except Exception as e:
            logger.error(f"Error in batch write: {str(e)}")
            response['statusCode'] = -1
            return response

    def batch_update(self, data_dict, **kwargs):
        """Update existing symbols in batch using concurrent operations.

        Updates multiple existing symbols concurrently using ThreadPoolExecutor for
        improved performance. Use this for modifying existing data ranges.

        Args:
            data_dict: Dictionary mapping symbol names (str) to pandas DataFrames.
                Symbols must already exist in the library.
            **kwargs: Additional keyword arguments passed to ArcticDB update operation.

        Returns:
            Dictionary with status information:
                - 'statusCode': 0 on complete success, 1 if some symbols failed,
                  -1 on complete failure
                - 'skipped_symbols': List of symbols that failed to update

        Examples:
            Update existing data:
                >>> ac = ArcDB(library_name='yfinance')
                >>> updated_data = {
                ...     'AAPL': corrected_aapl_df,
                ...     'MSFT': corrected_msft_df
                ... }
                >>> result = ac.batch_update(updated_data)
                >>> if result['statusCode'] == 0:
                ...     print("All symbols updated successfully")
                >>> else:
                ...     print(f"Failed symbols: {result['skipped_symbols']}")

        Note:
            - Symbols must exist in the library before updating
            - Updates are performed concurrently using ThreadPoolExecutor
            - For appending new data, use batch_store(mode='append') instead
        """

        response = {
            'statusCode': 0,
            'skipped_symbols': []
        }

        try:
            if not isinstance(data_dict, dict):
                logger.error("data_dict must be a dictionary")
                response['statusCode'] = -1
                return response

            def update_symbol(symbol_key, data):
                try:
                    self._lib.update(symbol_key, data, **kwargs)
                    return symbol_key, True
                except Exception as e:
                    logger.error(f"Error updating data for {symbol_key}: {str(e)}")
                    return symbol_key, False

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(update_symbol, symbol_key, data): symbol_key
                           for symbol_key, data in data_dict.items()}

                for future in concurrent.futures.as_completed(futures):
                    symbol_key, success = future.result()
                    if not success:
                        response['skipped_symbols'].append(symbol_key)

            if response['skipped_symbols']:
                response['statusCode'] = 1
                logger.warning(f"Failed to update {len(response['skipped_symbols'])} symbols")

            return response

        except Exception as e:
            logger.error(f"Error in batch update: {str(e)}")
            response['statusCode'] = -1
            return response

    def batch_read(self, symbol_list, qb_join='inner', **kwargs):
        """Read and join multiple symbols in batch operation.

        Reads multiple symbols and joins them into a single DataFrame using ArcticDB's
        batch read and join functionality. Supports date range filtering and column
        selection via kwargs.

        Args:
            symbol_list: List of symbol names (str) to read.
            qb_join: Join strategy for combining symbols, either 'inner' (default)
                or 'outer'. Inner join includes only dates present in all symbols;
                outer join includes all dates with NaN for missing values.
            **kwargs: Additional keyword arguments passed to ArcticDB ReadRequest:
                - date_range: Tuple of (start_date, end_date) for filtering
                - columns: List of column names to retrieve

        Returns:
            Dictionary with read results:
                - 'statusCode': 0 on success, -1 on failure
                - 'payload': Combined DataFrame with all symbols and a 'symbol'
                  column, or None on error

        Examples:
            Basic batch read:
                >>> ac = ArcDB(library_name='yfinance')
                >>> result = ac.batch_read(['AAPL', 'MSFT', 'GOOGL'])
                >>> if result['statusCode'] == 0:
                ...     df = result['payload']
                ...     print(df.head())

            Read with date range:
                >>> from datetime import datetime
                >>> result = ac.batch_read(
                ...     symbol_list=['AAPL', 'MSFT'],
                ...     qb_join='outer',
                ...     date_range=(
                ...         datetime(2024, 1, 1),
                ...         datetime(2024, 12, 31)
                ...     )
                ... )
                >>> df = result['payload']

            Read specific columns:
                >>> result = ac.batch_read(
                ...     symbol_list=['AAPL', 'MSFT', 'GOOGL'],
                ...     columns=['close', 'volume']
                ... )

        Note:
            - Returns concatenated DataFrame with 'symbol' column for identification
            - Inner join is more restrictive; use outer join for comprehensive data
            - Date range filtering is applied before join operation
        """
        response = {
            'statusCode': 0,
            'payload': None
        }

        if not isinstance(symbol_list, list):
            logger.error("symbol_list must be a list")
            response['statusCode'] = -1
            return response

        if not symbol_list:
            logger.warning("Empty symbol_list provided")
            response['statusCode'] = -1
            return response

        read_requests = []
        for symbol_key in symbol_list:
            read_request = adb.ReadRequest(symbol_key, **kwargs)
            read_requests.append(read_request)
            logger.debug(f"Created ReadRequest for {symbol_key}")

        try:
            q = adb.QueryBuilder().concat(qb_join)
            df = self._lib.read_batch_and_join(read_requests, q).data
            logger.info(f"Successfully read {len(df)} total records across {len(symbol_list)} symbols")

            response['payload'] = df

        except Exception as e:
            logger.error(f"Error during read_batch_and_join: {str(e)}")
            response['statusCode'] = -1

        return response


