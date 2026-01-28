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
from typing import Optional, Dict, Any, List
import pandas as pd


def ohlcv_to_arcticdb(
        *,
        ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
        backend: Optional[str] = None,
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
        backend: Storage backend type ('s3', 'lmdb', or 'mem', case-insensitive).
            If None, uses ARCTICDB_DEFAULT_BACKEND from ~/.chronos_lab/.env configuration.
            Overrides the default backend setting for this operation.
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
            >>> # Store in ArcticDB with default backend
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=prices,
            ...     library_name='yfinance',
            ...     adb_mode='write'
            ... )
            >>> if result['statusCode'] == 0:
            ...     print("Successfully stored data")

        Store with explicit backend selection:
            >>> # Store to S3 backend explicitly
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=prices,
            ...     backend='s3',
            ...     library_name='yfinance',
            ...     adb_mode='write'
            ... )
            >>>
            >>> # Store to local LMDB backend explicitly
            >>> result = ohlcv_to_arcticdb(
            ...     ohlcv=prices,
            ...     backend='lmdb',
            ...     library_name='yfinance'
            ... )

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
        ac = ArcDB(library_name=library_name, backend=backend)
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


def to_dataset(dataset_name: str,
               dataset: Dict[str, Any]) -> Dict[str, int]:
    """Save a dataset to local JSON file or DynamoDB table.

    Stores structured dataset dictionary based on naming convention. Automatically
    creates parent directories for local storage if needed.

    Args:
        dataset_name: Dataset identifier. Use 'ddb_' prefix for DynamoDB datasets,
            no prefix for local JSON files.
        dataset: Dictionary mapping keys to item attribute dictionaries

    Returns:
        Dictionary with 'statusCode': 0 on success, -1 on failure

    Examples:
        Save to local JSON file:
            >>> from chronos_lab.storage import to_dataset
            >>>
            >>> data = {
            ...     'AAPL': {'name': 'Apple Inc.', 'sector': 'Technology', 'price': 175.50},
            ...     'MSFT': {'name': 'Microsoft', 'sector': 'Technology', 'price': 420.00}
            ... }
            >>> result = to_dataset(dataset_name='tech_stocks', dataset=data)
            >>> if result['statusCode'] == 0:
            ...     print("Dataset saved successfully")

        Save to DynamoDB:
            >>> data = {
            ...     'security1': {'ticker': 'AAPL', 'exchange': 'NASDAQ'},
            ...     'security2': {'ticker': 'MSFT', 'exchange': 'NASDAQ'}
            ... }
            >>> result = to_dataset(dataset_name='ddb_securities', dataset=data)

    Note:
        - Local datasets: Saved to {DATASET_LOCAL_PATH}/{name}.json
        - DynamoDB datasets: Require DATASET_DDB_TABLE_NAME and DATASET_DDB_MAP configuration
        - DynamoDB items automatically get 'pk' and 'sk' keys from dataset map
        - Datetime objects serialized as ISO strings
        - Local file parent directories created automatically
    """
    from chronos_lab.dataset import Dataset

    ds = Dataset()
    return ds.save_dataset(dataset_name, dataset)


def _to_s3_store(*,
                 s3_key: str,
                 s3_body: bytes,
                 s3_prefix: Optional[str] = None,
                 s3_metadata: Optional[Dict[str, str]] = None,
                 **s3_put_object_kwargs
                 ):
    """Store file content to S3 bucket configured in settings.

    Args:
        s3_key: S3 object key (file name).
        s3_body: File content as bytes.
        s3_prefix: Optional S3 prefix to prepend to key (folder path). Defaults to None.
        s3_metadata: Optional metadata dict to attach to S3 object. Defaults to None.
        **s3_put_object_kwargs: Additional arguments passed to boto3 put_object call.

    Returns:
        Dictionary with 'statusCode' (0 on success, -1 on failure) and optionally
        's3_client_response' containing boto3 response on success.
    """
    from chronos_lab.aws import session, ClientError

    response = {
        'statusCode': 0
    }

    s3_client = session.client('s3')
    settings = get_settings()

    if not settings.store_s3_bucket:
        logger.error('No S3 bucket configured for storing data. Set STORE_S3_BUCKET setting.')
        response['statusCode'] = -1
        return response

    s3_name = f"{s3_prefix}/{s3_key}" if s3_prefix else s3_key

    try:
        res_put = s3_client.put_object(Body=s3_body,
                                       Bucket=settings.store_s3_bucket,
                                       Key=s3_name,
                                       Metadata=s3_metadata,
                                       **s3_put_object_kwargs)
        logger.info('File %s was saved to bucket %s. Details: %s', s3_name, settings.store_s3_bucket,
                    res_put)
        response['s3_client_response'] = res_put
    except ClientError as e:
        logger.error('Failed to save file %s to bucket %s. Details: %s', s3_name,
                     settings.store_s3_bucket, e)

    return response


def _to_local_store(*,
                    file_name: str,
                    content: bytes,
                    folder: Optional[str] = None,
                    ):
    """Store file content to local filesystem path configured in settings.

    Args:
        file_name: Name of file to save.
        content: File content as bytes.
        folder: Optional subdirectory within configured store path. Defaults to None.

    Returns:
        Dictionary with 'statusCode' (0 on success, -1 on failure) and optionally
        'file_path' containing full path to saved file on success.
    """
    from pathlib import Path

    response = {
        'statusCode': 0
    }

    settings = get_settings()

    if not settings.store_local_path:
        logger.error('No local path configured for storing data. Set STORE_LOCAL_PATH setting.')
        response['statusCode'] = -1
        return response

    base_path = Path(settings.store_local_path).expanduser()
    base_path.mkdir(parents=True, exist_ok=True)

    if folder:
        target_dir = base_path / folder
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / file_name
    else:
        file_path = base_path / file_name

    try:
        file_path.write_bytes(content)
        logger.info('File %s was saved to %s', file_name, file_path)
        response['file_path'] = str(file_path)
    except Exception as e:
        logger.error('Failed to save file %s to %s. Details: %s', file_name, file_path, e)
        response['statusCode'] = -1

    return response


def to_store(*,
             file_name: str,
             content: bytes,
             folder: Optional[str] = None,
             stores: Optional[List[str]] = None,
             s3_metadata: Optional[Dict[str, str]] = None,
             s3_put_object_kwargs: Optional[Dict[str, Any]] = None):
    """Store file content to local filesystem and/or S3 based on configuration.

    Saves arbitrary file content (images, JSON, binary data) to configured storage
    backends. Supports local filesystem and S3, or both simultaneously.

    Args:
        file_name: Name of the file to save.
        content: File content as bytes.
        folder: Optional subdirectory/prefix within the configured storage path.
            For local storage, creates subdirectory under STORE_LOCAL_PATH.
            For S3, prepends to object key. Defaults to None.
        stores: List of storage backends to use. Options: ['local'], ['s3'],
            or ['local', 's3'] for both. Defaults to ['local'].
        s3_metadata: Optional metadata dict to attach to S3 object (ignored for local).
            Defaults to None.
        s3_put_object_kwargs: Additional arguments passed to boto3 put_object
            (e.g., ContentType, ACL). Defaults to None.

    Returns:
        Dictionary with status information:
            - 'local_statusCode': 0 on success, -1 on failure (if 'local' in stores)
            - 'file_path': Full local path to saved file (if successful)
            - 's3_statusCode': 0 on success, -1 on failure (if 's3' in stores)
            - 's3_client_response': boto3 response dict (if S3 successful)

    Example:
        ```python
        from chronos_lab.plot import plot_ohlcv_anomalies
        from chronos_lab.storage import to_store

        # Generate a plot
        plot_data = plot_ohlcv_anomalies(anomalies_df, plot_to_store=False)

        # Save to both local and S3
        result = to_store(
            file_name=plot_data['file_name'],
            content=plot_data['content'],
            folder='anomaly_charts',
            stores=['local', 's3'],
            s3_metadata={'symbol': 'AAPL', 'analysis_type': 'anomaly'}
        )

        if result['local_statusCode'] == 0:
            print(f"Saved locally to: {result['file_path']}")
        if result['s3_statusCode'] == 0:
            print("Saved to S3 successfully")
        ```
    """
    if stores is None:
        stores = ['local']

    if s3_put_object_kwargs is None:
        s3_put_object_kwargs = {}

    response = {}

    if 'local' in stores:
        local_response = _to_local_store(
            file_name=file_name,
            content=content,
            folder=folder
        )
        response['local_statusCode'] = local_response['statusCode']
        if 'file_path' in local_response:
            response['file_path'] = local_response['file_path']

    if 's3' in stores:
        s3_response = _to_s3_store(
            s3_key=file_name,
            s3_body=content,
            s3_prefix=folder,
            s3_metadata=s3_metadata,
            **s3_put_object_kwargs
        )
        response['s3_statusCode'] = s3_response['statusCode']
        if 's3_client_response' in s3_response:
            response['s3_client_response'] = s3_response['s3_client_response']

    return response


__all__ = [
    'ohlcv_to_arcticdb',
    'to_dataset',
    'to_store',
]
