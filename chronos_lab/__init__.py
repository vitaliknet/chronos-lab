"""chronos-lab: A lightweight Python library for time series financial data analysis.

This package provides modular tools for fetching, storing, and analyzing financial
time series data with support for multiple data sources and high-performance storage.

Key Features:
    - Data Sources: Yahoo Finance (yfinance) and Intrinio API integration
    - Storage: High-performance ArcticDB time series database with versioning
    - MCP Server: Model Context Protocol server capabilities
    - Modular Design: Install only the features you need via optional extras

Installation:
    Core package (minimal dependencies):
        pip install chronos-lab

    With optional features:
        pip install chronos-lab[yfinance]      # Yahoo Finance integration
        pip install chronos-lab[intrinio]      # Intrinio API integration
        pip install chronos-lab[arcticdb]      # ArcticDB storage backend
        pip install chronos-lab[mcp]           # MCP server capabilities

Configuration:
    On first import, chronos-lab automatically creates ~/.chronos_lab/.env with
    default settings. Edit this file to configure API keys, storage paths, and
    logging levels.

Quick Start:
    >>> from chronos_lab.sources import ohlcv_from_yfinance
    >>> from chronos_lab.storage import ohlcv_to_arcticdb
    >>>
    >>> # Fetch data
    >>> prices = ohlcv_from_yfinance(symbols=['AAPL', 'MSFT'], period='1y')
    >>>
    >>> # Store for later use
    >>> ohlcv_to_arcticdb(ohlcv=prices, library_name='yfinance')

Modules:
    sources: Data fetching from Yahoo Finance, Intrinio, and ArcticDB
    storage: Persistent storage operations using ArcticDB
    settings: Configuration management via Pydantic Settings
    arcdb: Low-level ArcticDB wrapper class
    intrinio: Low-level Intrinio API wrapper
    mcp_server: FastMCP server for Model Context Protocol
"""

import logging
from pathlib import Path
import shutil
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s | %(funcName)s | %(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _init_config():
    try:
        config_dir = Path.home() / ".chronos_lab"
        env_file = config_dir / ".env"

        if not env_file.exists():
            config_dir.mkdir(parents=True, exist_ok=True)

            package_dir = Path(__file__).parent
            env_example = package_dir / ".env.example"

            if env_example.exists():
                shutil.copy(env_example, env_file)
                print(f"✓ Chronos Lab: Created config at {env_file}", file=sys.stderr)

    except Exception as e:
        print(f"Warning: Could not initialize Chronos Lab config: {e}", file=sys.stderr)


_init_config()

del _init_config
