# Analysis Calculation API

!!! warning "Deprecated"
    This module is **deprecated** and will be removed in a future version. Please migrate to the new [Analysis Drivers API](analysis-drivers.md) using `AnalysisDriver` for better performance, composability, and caching support.

Time series calculation functions for feature engineering and anomaly detection.

## Overview

The `chronos_lab.analysis.calculation` module provides computational functions for analyzing OHLCV time series data.

**This API is deprecated.** Use `chronos_lab.analysis.driver.AnalysisDriver` instead:

```python
# ❌ Old (deprecated)
from chronos_lab.analysis.calculation.anomaly import detect_ohlcv_anomalies
results = detect_ohlcv_anomalies(ohlcv_df, contamination=0.02)

# ✅ New (recommended)
from chronos_lab.analysis import AnalysisDriver
driver = AnalysisDriver()
results = driver.detect_anomalies(ohlcv_df, contamination=0.02)
```

[View migration guide →](analysis-drivers.md#migration-from-legacy-api)

## Anomaly Detection

::: chronos_lab.analysis.calculation.anomaly.detect_ohlcv_anomalies
    options:
      show_root_heading: true
      heading_level: 3
