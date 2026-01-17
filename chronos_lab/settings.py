"""Application settings and configuration management using Pydantic Settings.

This module provides centralized configuration management for chronos-lab using Pydantic
Settings. Configuration is loaded from ~/.chronos_lab/.env file with support for
environment variable overrides.

Configuration File:
    On first import of chronos_lab, the package automatically creates ~/.chronos_lab/.env
    from the bundled .env.example template if it doesn't exist. This file contains:
        - API keys (Intrinio)
        - ArcticDB storage configuration (local path, S3 bucket, default library)
        - Logging level

Environment Variable Overrides:
    All settings can be overridden using environment variables with uppercase names:
        - INTRINIO_API_KEY: Intrinio API key
        - LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR)
        - ARCTICDB_DEFAULT_LIBRARY_NAME: Default ArcticDB library name
        - ARCTICDB_LOCAL_PATH: Local filesystem path for ArcticDB LMDB backend
        - ARCTICDB_S3_BUCKET: S3 bucket name for ArcticDB S3 backend

Typical Usage:
    Access configuration settings:
        >>> from chronos_lab.settings import get_settings
        >>>
        >>> settings = get_settings()
        >>> api_key = settings.intrinio_api_key
        >>> library_name = settings.arcticdb_default_library_name

    Override with environment variables:
        >>> import os
        >>> os.environ['INTRINIO_API_KEY'] = 'my_custom_key'
        >>> os.environ['LOG_LEVEL'] = 'DEBUG'
        >>> # Settings will reflect environment variable values

    Edit configuration file directly:
        >>> # Edit ~/.chronos_lab/.env
        >>> # INTRINIO_API_KEY=your_api_key_here
        >>> # ARCTICDB_LOCAL_PATH=~/.chronos_lab/arcticdb
        >>> # ARCTICDB_DEFAULT_LIBRARY_NAME=uscomp

Note:
    - Settings are cached using @lru_cache for performance
    - Changes to .env file or environment variables require restarting the Python process
    - Unknown settings in .env file are ignored (extra="ignore")
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
        intrinio_api_key: Intrinio API key for accessing financial data. Required for
            using Intrinio data sources. Defaults to None.
        log_level: Logging level for the application. Valid values: 'DEBUG', 'INFO',
            'WARNING', 'ERROR', 'CRITICAL'. Defaults to 'INFO'.
        arcticdb_default_library_name: Default ArcticDB library name used when none
            is specified. Defaults to 'uscomp'.
        arcticdb_local_path: Filesystem path for ArcticDB LMDB backend storage.
            Supports tilde expansion (~). Defaults to None. Example: '~/.chronos_lab/arcticdb'
        arcticdb_s3_bucket: S3 bucket name for ArcticDB S3 backend storage. Takes
            precedence over local_path when both are configured. Defaults to None.

    Configuration:
        - Loads from: ~/.chronos_lab/.env
        - Encoding: UTF-8
        - Extra fields: Ignored (allows forward compatibility)
        - Environment variables: All settings can be overridden using uppercase env vars

    Examples:
        Access settings programmatically:
            >>> from chronos_lab.settings import get_settings
            >>>
            >>> settings = get_settings()
            >>> print(settings.intrinio_api_key)
            >>> print(settings.arcticdb_default_library_name)
            >>> print(settings.log_level)

        Configuration file format (~/.chronos_lab/.env):
            >>> # Intrinio API Settings
            >>> INTRINIO_API_KEY=your_api_key_here
            >>>
            >>> # ArcticDB Settings
            >>> ARCTICDB_LOCAL_PATH=~/.chronos_lab/arcticdb
            >>> ARCTICDB_DEFAULT_LIBRARY_NAME=uscomp
            >>> # ARCTICDB_S3_BUCKET=my-bucket  # Uncomment for S3 backend
            >>>
            >>> # Logging
            >>> LOG_LEVEL=INFO

        Override with environment variables:
            >>> import os
            >>> os.environ['INTRINIO_API_KEY'] = 'custom_key'
            >>> os.environ['ARCTICDB_DEFAULT_LIBRARY_NAME'] = 'my_library'
            >>> settings = get_settings()  # Will use environment values

    Note:
        - Settings are cached via @lru_cache in get_settings()
        - Restart Python process for changes to take effect
        - S3 backend requires additional AWS configuration (see arcdb module docs)
    """
    model_config = SettingsConfigDict(
        env_file=str(Path.home() / ".chronos_lab" / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    intrinio_api_key: Optional[str] = None
    log_level: str = "INFO"

    arcticdb_default_library_name: Optional[str] = "uscomp"
    arcticdb_local_path: Optional[str] = None
    arcticdb_s3_bucket: Optional[str] = None


@lru_cache
def get_settings() -> Settings:
    """Get cached singleton instance of application settings.

    Returns a cached Settings object loaded from ~/.chronos_lab/.env with environment
    variable overrides. Uses @lru_cache to ensure only one Settings instance is created
    per Python process, improving performance and ensuring consistency.

    Returns:
        Settings: Singleton Settings instance with loaded configuration.

    Examples:
        Basic usage:
            >>> from chronos_lab.settings import get_settings
            >>>
            >>> settings = get_settings()
            >>> api_key = settings.intrinio_api_key
            >>> library = settings.arcticdb_default_library_name

        Multiple calls return same instance:
            >>> settings1 = get_settings()
            >>> settings2 = get_settings()
            >>> assert settings1 is settings2  # Same object

        Use in modules:
            >>> # In chronos_lab/sources.py
            >>> from chronos_lab.settings import get_settings
            >>>
            >>> def ohlcv_from_intrinio(**kwargs):
            ...     settings = get_settings()
            ...     api_key = kwargs.get('api_key') or settings.intrinio_api_key

    Note:
        - Cached with @lru_cache for performance
        - Returns same instance across all calls in same Python process
        - Changes to .env or environment variables require process restart
        - Thread-safe due to lru_cache implementation
    """
    return Settings()
