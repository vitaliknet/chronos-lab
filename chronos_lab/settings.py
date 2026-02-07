"""Application settings and configuration management using Pydantic Settings.

This module provides centralized configuration management for chronos-lab using Pydantic
Settings. Configuration is loaded from ~/.chronos_lab/.env file with support for
environment variable overrides.

Configuration File:
    On first import of chronos_lab, the package automatically creates ~/.chronos_lab/.env
    from the bundled .env.example template if it doesn't exist. This file contains:
        - API keys (Intrinio)
        - ArcticDB storage configuration (backend, local path, S3 bucket, default library)
        - Interactive Brokers connection configuration (host, port, client ID, account, concurrency)
        - Dataset storage configuration (local path, DynamoDB table and mapping)
        - Generic store configuration (local path, S3 bucket)
        - Hamilton cache path for DAG execution caching
        - Logging level

Environment Variable Overrides:
    All settings can be overridden using environment variables with uppercase names:
        - ARCTICDB_DEFAULT_BACKEND: Default ArcticDB backend (LMDB or S3)
        - ARCTICDB_DEFAULT_LIBRARY_NAME: Default ArcticDB library name
        - ARCTICDB_LOCAL_PATH: Local filesystem path for ArcticDB LMDB backend
        - ARCTICDB_S3_BUCKET: S3 bucket name for ArcticDB S3 backend
        - DATASET_LOCAL_PATH: Local filesystem path for dataset JSON storage
        - DATASET_DDB_TABLE_NAME: DynamoDB table name for dataset storage
        - DATASET_DDB_MAP: JSON mapping of dataset names to DynamoDB key structure
        - HAMILTON_CACHE_PATH: Path for Hamilton Driver caching
        - IB_GATEWAY_HOST: Interactive Brokers Gateway/TWS hostname
        - IB_GATEWAY_PORT: Interactive Brokers Gateway/TWS port
        - IB_GATEWAY_READONLY: IB read-only mode (true/false)
        - IB_GATEWAY_CLIENT_ID: IB client ID for connection
        - IB_GATEWAY_ACCOUNT: IB account identifier
        - IB_REF_DATA_CONCURRENCY: Max concurrent IB reference data requests
        - IB_HISTORICAL_DATA_CONCURRENCY: Max concurrent IB historical data requests
        - INTRINIO_API_KEY: Intrinio API key
        - LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        - STORE_LOCAL_PATH: Local filesystem path for generic store
        - STORE_S3_BUCKET: S3 bucket name for generic store

Typical Usage:
    Access configuration settings:
        >>> from chronos_lab.settings import get_settings
        >>>
        >>> settings = get_settings()
        >>> api_key = settings.intrinio_api_key
        >>> library_name = settings.arcticdb_default_library_name
        >>> ib_host = settings.ib_gateway_host
        >>> ib_port = settings.ib_gateway_port
        >>> cache_path = settings.hamilton_cache_path

    Override with environment variables:
        >>> import os
        >>> os.environ['INTRINIO_API_KEY'] = 'my_custom_key'
        >>> os.environ['LOG_LEVEL'] = 'DEBUG'
        >>> os.environ['IB_GATEWAY_PORT'] = '4001'
        >>> os.environ['IB_GATEWAY_ACCOUNT'] = 'DU1234567'
        >>> os.environ['HAMILTON_CACHE_PATH'] = '~/.chronos_lab/cache'
        >>> # Settings will reflect environment variable values

    Edit configuration file directly:
        >>> # Edit ~/.chronos_lab/.env
        >>> # INTRINIO_API_KEY=your_api_key_here
        >>> # ARCTICDB_LOCAL_PATH=~/.chronos_lab/arcticdb
        >>> # ARCTICDB_DEFAULT_LIBRARY_NAME=uscomp
        >>> # IB_GATEWAY_HOST=127.0.0.1
        >>> # IB_GATEWAY_PORT=4001
        >>> # IB_GATEWAY_ACCOUNT=DU1234567
        >>> # HAMILTON_CACHE_PATH=~/.chronos_lab/cache

Important Notes:
    - Settings are cached using @lru_cache in get_settings() for performance
    - The get_settings() function returns a singleton instance across all calls
    - Changes to .env file or environment variables require restarting the Python process
    - Thread-safe due to lru_cache implementation
    - Unknown settings in .env file are ignored (extra="ignore")
    - All filesystem paths support tilde expansion (~)
    - S3 backend requires additional AWS configuration (see arcdb module docs)
    - IB integration requires IB Gateway or TWS running and configured (see ib module docs)
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path


class Settings(BaseSettings):
    """Application configuration settings loaded from ~/.chronos_lab/.env file.

    Pydantic Settings model that manages all application configuration with automatic
    loading from configuration file and environment variable override support. Settings
    are validated on instantiation.

    Attributes:
        arcticdb_default_backend: Default ArcticDB backend type. Options: 'LMDB' or 'S3'.
            Defaults to 'LMDB'.
        arcticdb_default_library_name: Default ArcticDB library name used when none
            is specified. Defaults to 'uscomp'.
        arcticdb_local_path: Filesystem path for ArcticDB LMDB backend storage.
            Supports tilde expansion (~). Defaults to None. Example: '~/.chronos_lab/arcticdb'
        arcticdb_s3_bucket: S3 bucket name for ArcticDB S3 backend storage.
            Defaults to None.
        dataset_local_path: Filesystem path for local dataset JSON file storage.
            Supports tilde expansion (~). Defaults to None. Example: '~/.chronos_lab/datasets'
        dataset_ddb_table_name: DynamoDB table name for dataset storage. Required for
            DynamoDB-backed datasets (names starting with 'ddb_'). Defaults to None.
        dataset_ddb_map: JSON string mapping dataset names to DynamoDB key structure.
            Maps dataset names to partition key (pk) and sort key (sk) patterns. Defaults to None.
            Example: '{"ddb_securities": {"pk": "DATASET#securities", "sk": "ticker"}}'
        hamilton_cache_path: Filesystem path for Hamilton Driver caching. Supports tilde
            expansion (~). Defaults to None. Example: '~/.chronos_lab/cache'
        ib_gateway_host: Interactive Brokers Gateway or TWS hostname/IP address.
            Defaults to '127.0.0.1' (localhost).
        ib_gateway_port: Interactive Brokers Gateway or TWS port number.
            Defaults to 4001. Common ports: 4001 (IB Gateway paper), 4002 (IB Gateway live),
            7496 (TWS paper), 7497 (TWS live).
        ib_gateway_readonly: Read-only connection mode for IB. When True, prevents order
            placement and account modifications. Defaults to True.
        ib_gateway_client_id: Client ID for IB connection. Must be unique per connection.
            Defaults to None. Required for establishing connection.
        ib_gateway_account: IB account identifier (e.g., 'DU1234567' for paper trading).
            Defaults to None.
        ib_ref_data_concurrency: Maximum number of concurrent reference data requests
            (contract details lookups) to IB API. Controls rate limiting for async operations.
            Defaults to 20.
        ib_historical_data_concurrency: Maximum number of concurrent historical data requests
            to IB API. Controls rate limiting for async operations. Defaults to 20.
        intrinio_api_key: Intrinio API key for accessing financial data. Required for
            using Intrinio data sources. Defaults to None.
        log_level: Logging level for the application. Valid values: 'DEBUG', 'INFO',
            'WARNING', 'ERROR', 'CRITICAL'. Defaults to 'WARNING'.
        store_local_path: Filesystem path for generic local storage. Supports tilde
            expansion (~). Defaults to None.
        store_s3_bucket: S3 bucket name for generic storage. Defaults to None.

    Configuration:
        - Loads from: ~/.chronos_lab/.env
        - Encoding: UTF-8
        - Extra fields: Ignored (allows forward compatibility)
        - Environment variables: All settings can be overridden using uppercase env vars
    """
    model_config = SettingsConfigDict(
        env_file=str(Path.home() / ".chronos_lab" / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    arcticdb_default_backend: Optional[str] = "LMDB"
    arcticdb_default_library_name: Optional[str] = "uscomp"
    arcticdb_local_path: Optional[str] = None
    arcticdb_s3_bucket: Optional[str] = None

    dataset_local_path: Optional[str] = None
    dataset_ddb_table_name: Optional[str] = None
    dataset_ddb_map: Optional[str] = None

    hamilton_cache_path: Optional[str] = None

    ib_gateway_host: Optional[str] = "127.0.0.1"
    ib_gateway_port: Optional[int] = 4001
    ib_gateway_readonly: Optional[bool] = True
    ib_gateway_client_id: Optional[int] = None
    ib_gateway_account: Optional[str] = None
    ib_ref_data_concurrency: int = 20
    ib_historical_data_concurrency: int = 20

    intrinio_api_key: Optional[str] = None
    log_level: str = "WARNING"

    store_local_path: Optional[str] = None
    store_s3_bucket: Optional[str] = None


@lru_cache
def get_settings() -> Settings:
    """Get cached singleton instance of application settings.

    Returns a cached Settings object loaded from ~/.chronos_lab/.env with environment
    variable overrides. Uses @lru_cache to ensure only one Settings instance is created
    per Python process.

    Returns:
        Singleton Settings instance with loaded configuration.
    """
    return Settings()
