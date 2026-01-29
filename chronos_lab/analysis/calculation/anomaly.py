import pandas as pd
from typing import List, Dict, Optional, Any
from chronos_lab.analysis.driver import AnalysisDriver


def detect_ohlcv_anomalies(
        ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
        *,
        ohlcv_features_list: List[str] = None,
        contamination: float = 0.02,
        use_adjusted: bool = True,
        generate_plots: str = 'disabled',
        plot_to_store_kwargs=None,
        to_dataset: str = 'enabled',
        dataset_name: str = 'ohlcv_anomalies',
        ddb_dataset_ttl: int = 7,
        anomaly_period_filter='1m',
        return_ohlcv_df=False,
        return_dag: Optional[bool] = False,
        local_executor_type: Optional[str] = 'synchronous',
        remote_executor_type: Optional[str] = 'multithreading',
        max_tasks: Optional[int] = 5,
        **sklearn_kwargs
) -> Dict[str, Any]:
    """ Deprecated. Use `AnalysisDriver.detect_anomalies()` instead.
    This function is deprecated and will be removed in version 0.2.0

    Detect anomalies in OHLCV time series data using Isolation Forest algorithm.

    Args:
        ohlcv: OHLCV data as MultiIndex DataFrame (date, symbol) or dict of DataFrames by symbol.
        ohlcv_features_list: List of features to compute for anomaly detection. Options: 'returns',
            'volume_change', 'high_low_range', 'volatility'. Defaults to ['returns', 'volume_change',
            'high_low_range'].
        contamination: Expected proportion of anomalies in the data. Lower values detect fewer,
            more extreme anomalies. Defaults to 0.02 (2%).
        use_adjusted: Whether to use adjusted OHLCV columns (adj_close, etc.) if available.
            Defaults to True.
        generate_plots: Whether to generate anomaly plots. Options: 'enabled', 'disabled'.
            Defaults to 'disabled'.
        plot_to_store_kwargs: Additional kwargs passed to plot storage function when
            generate_plots='enabled'. Defaults to None.
        to_dataset: Whether to save anomalies to a dataset. Options: 'enabled', 'disabled'.
            Defaults to 'enabled'.
        dataset_name: Name of the dataset to save anomalies. Use 'ddb_' prefix for DynamoDB
            datasets. Defaults to 'ohlcv_anomalies'.
        ddb_dataset_ttl: TTL in days for DynamoDB datasets (ignored for other dataset types).
            Defaults to 7.
        anomaly_period_filter: Period string to filter recent anomalies (e.g., '1m', '7d', '2w').
            None returns all detected anomalies. Defaults to '1m'.
        return_ohlcv_df: Whether to include full OHLCV DataFrame with features in the result.
            Defaults to False.
        return_dag: Whether to return Apache Hamilton Driver along with results. Defaults to False.
        local_executor_type: Hamilton Driver local executor type. Options: 'synchronous'.
            Defaults to 'synchronous'.
        remote_executor_type: Hamilton Driver remote executor type. Options: 'multithreading',
            'multiprocessing'. Defaults to 'multithreading'.
        max_tasks: Maximum number of parallel tasks for Hamilton Driver remote executor.
            Defaults to 5.
        **sklearn_kwargs: Additional kwargs passed to sklearn's IsolationForest. Common options:
            n_estimators (default 200), max_samples (default 250), bootstrap, max_features.

    Returns:
        Dictionary containing 'filtered_anomalies_df' and optionally 'ohlcv_df' (if return_ohlcv_df=True)
        and 'driver' (if return_dag=True).
    """

    driver = AnalysisDriver()

    result = driver.detect_anomalies(
        ohlcv,
        ohlcv_features_list=ohlcv_features_list,
        contamination=contamination,
        use_adjusted=use_adjusted,
        generate_plots=generate_plots,
        plot_to_store_kwargs=plot_to_store_kwargs,
        to_dataset=to_dataset,
        dataset_name=dataset_name,
        ddb_dataset_ttl=ddb_dataset_ttl,
        anomaly_period_filter=anomaly_period_filter,
        return_ohlcv_df=return_ohlcv_df,
        local_executor_type=local_executor_type,
        remote_executor_type=remote_executor_type,
        max_parallel_tasks=max_tasks,
        **sklearn_kwargs
    )

    if return_dag:
        result['driver'] = driver.drivers['detect_anomalies']

    return result
