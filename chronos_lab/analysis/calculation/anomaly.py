import pandas as pd
from typing import List, Dict, Optional, Any
from hamilton import driver, telemetry
from hamilton.execution import executors
from chronos_lab.analysis.dag import standardize, features, anomaly


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
        max_tasks: Optional[int] = 5,
        **sklearn_kwargs
) -> Dict[str, Any]:
    """Detect anomalies in OHLCV time series data using Isolation Forest algorithm.

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
        max_tasks: Maximum number of parallel tasks for remote executor. Defaults to 5.
        **sklearn_kwargs: Additional kwargs passed to sklearn's IsolationForest. Common options:
            n_estimators (default 200), max_samples (default 250), bootstrap, max_features.

    Returns:
        Dictionary containing 'filtered_anomalies_df' and optionally 'ohlcv_df' (if return_ohlcv_df=True)
        and 'driver' (if return_dag=True).
    """
    if plot_to_store_kwargs is None:
        plot_to_store_kwargs = {}
    if ohlcv_features_list is None:
        ohlcv_features_list = ['returns', 'volume_change', 'high_low_range']

    config = {
        'use_adjusted': use_adjusted,
        'ohlcv_features_list': ohlcv_features_list,
        'contamination': contamination,
        'sklearn_kwargs': sklearn_kwargs if sklearn_kwargs else {
            'n_estimators': 200,
            'max_samples': 250
        },
        'generate_plots': generate_plots,
        'plot_to_store_kwargs': plot_to_store_kwargs,
        'anomaly_period_filter': anomaly_period_filter,
        'return_ohlcv_df': return_ohlcv_df,
        'to_dataset': to_dataset,
        'dataset_name': dataset_name,
        'ddb_dataset_ttl': ddb_dataset_ttl
    }

    telemetry.disable_telemetry()
    dr = (
        driver.Builder()
        .with_config(config)
        .with_modules(standardize, features, anomaly)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=max_tasks))
    ).build()

    result = dr.execute(
        final_vars=['anomalies_complete'],
        inputs={'source_ohlcv': ohlcv}
    )

    if return_dag:
        result['anomalies_complete']['driver'] = dr

    return result['anomalies_complete']