import pandas as pd
from typing import List, Dict, Optional
from hamilton import driver, telemetry
from hamilton.execution import executors
from chronos_lab.analysis.dag import standardize, features, anomaly


def detect_ohlcv_anomalies(
        ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
        *,
        ohlcv_features_list: List[str] = None,
        contamination: float = 0.02,
        use_adjusted: bool = True,
        output_dict: Optional[bool] = False,
        max_tasks: Optional[int] = 5,
        **sklearn_kwargs
) -> pd.DataFrame | Dict[str, pd.DataFrame]:
    if ohlcv_features_list is None:
        ohlcv_features_list = ['returns', 'volume_change', 'high_low_range']

    config = {
        'use_adjusted': use_adjusted,
        'ohlcv_features_list': ohlcv_features_list,
        'contamination': contamination,
        'sklearn_kwargs': sklearn_kwargs if sklearn_kwargs else {
            'n_estimators': 200,
            'max_samples': 256
        }
    }

    telemetry.disable_telemetry()
    dr = (
        driver.Builder()
        .with_config(config)
        .with_modules(standardize, features, anomaly)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=max_tasks))
    ).build()

    result = dr.execute(
        final_vars=['anomalies_calculation_complete'],
        inputs={'source_ohlcv': ohlcv}
    )

    if output_dict:
        result['display_all_functions'] = dr.display_all_functions()
        return result
    else:
        return result['anomalies_calculation_complete']
