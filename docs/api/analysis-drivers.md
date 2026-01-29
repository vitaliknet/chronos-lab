# Analysis Driver API

Hamilton Driver wrapper for composable analysis calculations.

## Overview

The `chronos_lab.analysis.driver` module provides the `AnalysisDriver` class, a wrapper around Apache Hamilton's Driver that simplifies running analysis calculations with shared configuration, caching, and execution management.

**Key Features:**

- **Zero-config defaults** - `AnalysisDriver()` works out of the box
- **Flexible execution** - Multithreading or multiprocessing for symbol-level parallelization
- **Persistent caching** - Hamilton's cache for expensive computations


## API Reference

::: chronos_lab.analysis.driver.AnalysisDriver
    options:
      show_root_heading: true
      heading_level: 3
      members:
        - __init__
        - detect_anomalies

