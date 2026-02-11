# Changelog

All notable changes to chronos-lab will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-02-11

### Added
- **Interactive Brokers Integration**: Complete market data support with `IBMarketData` API for real-time ticks and historical bars, plus high-level `ohlcv_from_ib` and `ohlcv_from_ib_async` functions for seamless OHLCV data retrieval
- `TimeSeriesCollection` class for multi-symbol data orchestration (experimental, undocumented, subject to change)

### Documentation
- New Interactive Brokers market data tutorial and API reference

## [0.2.0] – 2026-02-02

### Added
- **Configurable OHLCV data sources in DAGs**:  
  Analysis calculations can now pull data directly from multiple sources (`yfinance`, `intrinio`, or `arcticdb`) using a unified configuration interface, making it easy to switch between a variable input and external providers.
- **Source-aware anomaly detection**:  
  `AnalysisDriver.detect_anomalies` now supports both preloaded OHLCV DataFrames and automatic retrieval from configured data sources.
- **Dynamic dataset and ArcticDB outputs**:  
  Analysis results can be flexibly routed to different datasets/backends, enabling easier experimentation and storage control.

### Changed
- **`AnalysisDriver` improvements**:  
  Streamlined configuration, clearer parameters, and a more modular design for building composable analysis workflows.
- **OHLCV standardization**:  
  `standardize_ohlcv` updated to work consistently across dynamically selected data sources.
- **Validation and robustness**:  
  Added parameter validation and safer defaults for more predictable behavior.
- **Documentation refresh**:  
  Expanded guides and examples reflecting the new source configuration and `AnalysisDriver`-based workflows.

### Removed
- Legacy `MCP` dependencies and related modules.
- The deprecated `analysis.calculation` module and outdated APIs.
- Plot module is no longer a part of the official documented API.   

### Deprecated
- Remaining legacy anomaly detection functions and parameters. Use `AnalysisDriver` going forward.


## [0.1.8] – 2026-01-29

### Added
- **Configurable ArcticDB backends**:  
  `ohlcv_from_arcticdb()` and `ohlcv_to_arcticdb()` now support explicit selection of the ArcticDB backend (`LMDB`, `S3`, or `MEM`), allowing the same code to run against local, in-memory, or cloud-backed stores.
- **`AnalysisDriver` API**:  
  A new interface intended to serve as the foundation for multiple analysis calculations, with caching and Hamilton-based DAG execution.
- **Updated documentation and examples** covering backend configuration and `AnalysisDriver` API.

### Changed
- **Anomaly detection interface**:  
  `detect_ohlcv_anomalies` is deprecated in favor of `AnalysisDriver`-based analysis, aligning anomaly detection with the new unified analysis API.
- **Plot rendering behavior**:  
  Plotting logic was refined to ensure figures render correctly in notebooks and scripts without accumulating open figures.
- **Logging defaults**:  
  The global log level now defaults to `WARNING` for a quieter out-of-the-box experience.

### Deprecated
- The `analysis.calculation` module and related anomaly detection helpers. Migration guidance is available in the documentation.

## [0.1.7] - 2026-01-27

### Added
- **New Tutorial**: Getting Started with Chronos Lab 
- Anomaly executor configuration

### Changed
- Dependency updates
- Documentation

## [0.1.6] - 2026-01-26

### Changed
- Documentation cleanup and enhancements

## [0.1.5] - 2026-01-25

### Added
- **Anomaly Detection System**: Complete ML-powered pipeline for detecting anomalies in OHLCV data using Isolation Forest, with Hamilton DAG-based architecture for scalable symbol-level processing
- **Visualization**: Anomaly plots with `mplfinance` integration, featuring customizable styling, human-readable axis formatting (1K, 1M, 1B), and flexible date range filtering
- **Dataset Export**: Export anomaly results to DynamoDB or local storage with configurable TTL support
- **File Storage**: Save plots and data locally or to S3 with the new `to_store` utility
- **Interactive Documentation**: Jupyter notebook support in documentation via `mkdocs-jupyter`, with comprehensive tutorials and API reference sections

### Changed
- Enhanced plotting with modular `plot_ohlcv_anomalies` function for reusability
- Improved error handling for edge cases in anomaly collection

### Dependencies
- Added `scikit-learn` and `sf-hamilton[visualization]` for ML and DAG execution
- Added `mplfinance`, `matplotlib`, and visualization support packages
- Added `mkdocs-jupyter` for interactive documentation

## [0.1.4] - 2026-01-18

### Added
- **AWS Integration**: Comprehensive utilities for SSM parameters, Secrets Manager, S3, and DynamoDB operations
- **Dataset Management**: Store and retrieve structured datasets locally or in DynamoDB with flexible mapping and serialization
- Enhanced ArcticDB setup with shared AWS session for S3 backend

### Documentation
- New documentation modules: `aws.md` for AWS utilities and `dataset.md` for dataset handling
- Expanded `sources.md` and `storage.md` with dataset examples

## [0.1.3] - 2026-01-18

### Added
- `to_dataset` function for saving structured datasets with DynamoDB or local storage support

## [0.1.2] - 2026-01-17

### Added
- **Automated Documentation**: GitHub Actions workflow for deploying documentation on version tags
- MkDocs site with Material theme
- Comprehensive docstrings and usage examples for core modules

## [0.1.1] - 2026-01-17

### Changed
- Updated Python requirement to `>= 3.12` with improved cross-platform support

## [0.1.0] - 2026-01-17

### Added
- **Core Data Access**: Functions for reading and writing OHLCV data to ArcticDB (`ohlcv_from_arcticdb`, `ohlcv_to_arcticdb`)
- **Multi-Source Support**: Fetch data from Intrinio (`ohlcv_from_intrinio`) and Yahoo Finance (`ohlcv_from_yfinance`)
- **Securities Discovery**: Retrieve securities lists from Intrinio via `securities_from_intrinio`
- **Flexible Storage**: Support for local and S3-backed ArcticDB instances
- **Auto-Configuration**: Automatic `.env` file generation on first import
- GitHub Actions workflow for PyPI publishing

### Documentation
- Complete README with installation, configuration, and usage examples

## [0.0.1] - 2026-01-14

### Added
- Initial project structure with Intrinio SDK integration
- ArcticDB for time-series data storage
- MCP server implementation
- Docker support and environment configuration

[Unreleased]: https://github.com/vitaliknet/chronos-lab/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/vitaliknet/chronos-lab/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.8...v0.2.0
[0.1.8]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/vitaliknet/chronos-lab/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/vitaliknet/chronos-lab/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/vitaliknet/chronos-lab/releases/tag/v0.0.1
