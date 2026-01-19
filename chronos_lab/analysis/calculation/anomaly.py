import pandas as pd
from typing import List, Dict, Optional
from hamilton import driver, telemetry
from chronos_lab.analysis.dag import standardize, features, anomaly
from chronos_lab.analysis._utils import process_by_symbol


def detect_ohlcv_anomalies(
    ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
    *,
    ohlcv_features_list: List[str] = None,
    contamination: float = 0.1,
    use_adjusted: bool = True,
    parallel: bool = True,
    max_workers: Optional[int] = None,
    driver_config: Optional[Dict] = None,
    **sklearn_kwargs
) -> pd.DataFrame | Dict[str, pd.DataFrame]:

    if ohlcv_features_list is None:
        ohlcv_features_list = ['returns', 'volume_change', 'high_low_range']

    config = {
        'use_adjusted': use_adjusted,
        'ohlcv_features_list': ohlcv_features_list,
        'contamination': contamination,
        'sklearn_kwargs': sklearn_kwargs if sklearn_kwargs else {}
    }

    dr_config = driver_config or {}
    telemetry.disable_telemetry()
    dr = driver.Driver(config, standardize, features, anomaly, **dr_config)

    def execute_for_symbol(ohlcv: pd.DataFrame) -> pd.DataFrame:
        result = dr.execute(
            final_vars=['ohlcv_with_features_anomalies'],
            inputs={'source_ohlcv': ohlcv}
        )
        if isinstance(result, dict):
            return result['ohlcv_with_features_anomalies']
        return result

    results = process_by_symbol(
        ohlcv_input=ohlcv,
        executor_fn=execute_for_symbol,
        parallel=parallel,
        max_workers=max_workers
    )

    return results
