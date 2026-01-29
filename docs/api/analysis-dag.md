# Analysis DAG Modules API

DAG (Directed Acyclic Graph) modules for data processing workflows.

## Overview

The `chronos_lab.analysis.dag` modules provide pipeline functions used to build Apache Hamilton graphs. These functions are composable and follow DAG patterns for data transformations.

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

## Anomaly Detection Pipeline

::: chronos_lab.analysis.dag.anomaly.detect_ohlcv_features_anomalies
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.ohlcv_by_symbol_with_features_anomalies
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.filter_anomalies
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.anomalies_collect
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.plot_anomalies__enabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.plot_anomalies__disabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.anomalies_to_dataset__enabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.anomalies_to_dataset__disabled
    options:
      show_root_heading: true
      heading_level: 3

::: chronos_lab.analysis.dag.anomaly.anomalies_complete
    options:
      show_root_heading: true
      heading_level: 3
