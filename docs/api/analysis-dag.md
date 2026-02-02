# Analysis DAG Modules API

DAG (Directed Acyclic Graph) modules for data processing workflows.

## Overview

The `chronos_lab.analysis.dag` modules provide pipeline functions used to build Apache Hamilton graphs. These functions are composable and follow DAG patterns for data transformations.

## Data Input/Output

::: chronos_lab.analysis.dag.io.ohlcv_from__yfinance
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.ohlcv_from__intrinio
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.ohlcv_from__arcticdb
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.ohlcv_from__disabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.analysis_to_dataset__enabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.analysis_to_dataset__disabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.analysis_to_arcticdb__enabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.io.analysis_to_arcticdb__disabled
    options:
      show_root_heading: true
      heading_level: 3

## Data Standardization

::: chronos_lab.analysis.dag.standardize.standardize_ohlcv
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.standardize.validate_ohlcv
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.standardize.split_ohlcv_by_symbol
    options:
      show_root_heading: true
      heading_level: 3

## Feature Engineering

::: chronos_lab.analysis.dag.features.ohlcv_features
    options:
      show_root_heading: true
      heading_level: 3

## Anomaly Detection

::: chronos_lab.analysis.dag.anomaly.detect_ohlcv_features_anomalies
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.ohlcv_by_symbol_with_features_anomalies
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.analysis_result
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.analysis_result_dataset__enabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.analysis_result_dataset__disabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.analysis_result_arcticdb__enabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.analysis_result_arcticdb__disabled
    options:
      show_root_heading: true
      heading_level: 3


