# Settings API

Configuration management using Pydantic Settings.

## Overview

The `chronos_lab.settings` module provides centralized configuration management with automatic loading from `~/.chronos_lab/.env` and environment variable override support.

## Classes and Functions

::: chronos_lab.settings.Settings
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - arcticdb_default_backend
        - arcticdb_default_library_name
        - arcticdb_local_path
        - arcticdb_s3_bucket
        - dataset_local_path
        - dataset_ddb_table_name
        - dataset_ddb_map
        - hamilton_cache_path
        - ib_gateway_host
        - ib_gateway_port
        - ib_gateway_readonly
        - ib_gateway_client_id
        - ib_gateway_account
        - ib_ref_data_concurrency
        - ib_historical_data_concurrency
        - intrinio_api_key
        - log_level
        - store_local_path
        - store_s3_bucket

::: chronos_lab.settings.get_settings
    options:
      show_root_heading: true
      heading_level: 3
