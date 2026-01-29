"""Hamilton Driver wrapper for analysis calculations."""

import pandas as pd
from types import ModuleType
from typing import Dict, Any, List, Optional
from hamilton import driver, telemetry
from hamilton.execution import executors
from chronos_lab.settings import get_settings


class AnalysisDriver:
    """Hamilton Driver wrapper for composable analysis calculations.

    Manages Hamilton Driver instances for different calculation types with shared
    caching and execution configuration. Each calculation type gets its own Driver
    (built once, reused on subsequent calls). All calculations share the same cache
    directory for maximum efficiency.

    Examples:
        Simple usage with defaults:
            >>> from chronos_lab.analysis.driver import AnalysisDriver
            >>>
            >>> driver = AnalysisDriver()
            >>> results = driver.detect_anomalies(ohlcv_df)

        Custom configuration:
            >>> driver = AnalysisDriver(
            ...     enable_cache=True,
            ...     max_parallel_tasks=10,
            ...     executor_type='multiprocessing'
            ... )
            >>> anomalies = driver.detect_anomalies(ohlcv_df, contamination=0.02)

        Per-calculation executor override:
            >>> driver = AnalysisDriver()
            >>> results = driver.detect_anomalies(
            ...     ohlcv_df,
            ...     executor_type='multiprocessing',
            ...     max_parallel_tasks=20
            ... )
    """

    def __init__(
            self,
            *,
            enable_cache: bool = True,
            cache_path: str = None,
            local_executor_type: Optional[str] = 'synchronous',
            remote_executor_type: str = 'multithreading',
            max_parallel_tasks: int = 5,
            enable_telemetry: bool = False
    ):
        """Initialize AnalysisDriver with shared configuration.

        Args:
            enable_cache: Enable Hamilton caching for expensive computations.
                Defaults to True.
            cache_path: Directory path for cache storage. If None, uses HAMILTON_CACHE_PATH from settings.
                If the setting is not set, raises ValueError.
            local_executor_type: Local executor type.
            remote_executor_type: Remote executor type for parallel processing. Options:
                'multithreading' or 'multiprocessing'. Defaults to 'multithreading'.
            max_parallel_tasks: Maximum number of parallel tasks for symbol-level
                processing. Defaults to 5.
            enable_telemetry: Enable Hamilton telemetry data collection.
                Defaults to False.
        """
        settings = get_settings()

        self._enable_cache = enable_cache
        self._cache_path = cache_path or settings.hamilton_cache_path

        if not self._cache_path:
            raise ValueError(
                "No cache path configured. Set HAMILTON_CACHE_PATH in settings or pass cache_path parameter to AnalysisDriver.")

        self.drivers: Dict[str, driver.Driver] = {}

        self._local_executor_type = local_executor_type
        self._remote_executor_type = remote_executor_type
        self._max_parallel_tasks = max_parallel_tasks

        if not enable_telemetry:
            telemetry.disable_telemetry()

    def detect_anomalies(
            self,
            ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
            *,
            ohlcv_features_list: List[str] = None,
            contamination: float = 0.02,
            use_adjusted: bool = True,
            generate_plots: str = 'disabled',
            plot_to_store_kwargs: dict = None,
            to_dataset: str = 'enabled',
            dataset_name: str = 'ohlcv_anomalies',
            ddb_dataset_ttl: int = 7,
            anomaly_period_filter: str = '1m',
            return_ohlcv_df: bool = False,
            local_executor_type: str = None,
            remote_executor_type: str = None,
            max_parallel_tasks: int = None,
            **sklearn_kwargs
    ) -> Dict[str, Any]:
        """Detect anomalies in OHLCV data using Isolation Forest algorithm.

        Analyzes OHLCV time series data to identify anomalous patterns using scikit-learn's
        Isolation Forest. The analysis pipeline: validates data, computes features (returns,
        volume changes, etc.), trains the model, and filters results by time period.

        Args:
            ohlcv: OHLCV data as MultiIndex DataFrame with (date, symbol) levels or
                dictionary mapping symbols to DataFrames. Required columns: open, high,
                low, close, volume.
            ohlcv_features_list: List of features to compute for anomaly detection. Options:
                'returns' (log returns), 'volume_change' (percentage change),
                'high_low_range' (daily range), 'volatility' (20-day rolling std).
                Defaults to ['returns', 'volume_change', 'high_low_range'].
            contamination: Expected proportion of anomalies in the data (0.0-0.5).
                Lower values detect fewer, more extreme anomalies. Defaults to 0.02 (2%).
            use_adjusted: Whether to use adjusted OHLCV columns (adj_close, etc.) if
                available. Defaults to True.
            generate_plots: Whether to generate anomaly visualization plots. Options:
                'enabled' or 'disabled'. Defaults to 'disabled'.
            plot_to_store_kwargs: Additional keyword arguments passed to the plot
                storage function when generate_plots='enabled'. Accepts 'path' for local
                storage or S3 configuration. Defaults to None.
            to_dataset: Whether to save detected anomalies to a dataset. Options:
                'enabled' or 'disabled'. Defaults to 'enabled'.
            dataset_name: Name of the dataset for storing anomalies. Prefix with 'ddb_'
                for DynamoDB storage (e.g., 'ddb_ohlcv_anomalies'). Defaults to
                'ohlcv_anomalies'.
            ddb_dataset_ttl: Time-to-live in days for DynamoDB dataset entries (ignored
                for non-DynamoDB datasets). Defaults to 7.
            anomaly_period_filter: Period string to filter recent anomalies for the result.
                Supports formats like '1m' (1 month), '7d' (7 days), '4w' (4 weeks),
                '1y' (1 year). If None, returns all detected anomalies. Defaults to '1m'.
            return_ohlcv_df: Whether to include the full OHLCV DataFrame with computed
                features and anomaly scores in the result. Defaults to False.
            local_executor_type: Override the default local executor type for this calculation only.
            remote_executor_type: Override the default remote executor type for this calculation only.
                Options: 'multithreading' or 'multiprocessing'. If None, uses the
                instance default. Defaults to None.
            max_parallel_tasks: Override the default maximum parallel tasks for this
                calculation only. If None, uses the instance default. Defaults to None.
            **sklearn_kwargs: Additional keyword arguments passed to scikit-learn's
                IsolationForest constructor. Common options: n_estimators (number of trees,
                default 200), max_samples (samples per tree, default 250), bootstrap,
                max_features, random_state.

        Returns:
            Dictionary containing:
                - 'filtered_anomalies_df': DataFrame with detected anomalies filtered by
                  period_filter, including columns for anomaly scores and rankings.
                - 'ohlcv_df' (optional): Full OHLCV DataFrame with features and anomaly
                  information, included only if return_full_data=True.

        Examples:
            Basic anomaly detection:
                >>> driver = AnalysisDriver()
                >>> results = driver.detect_anomalies(ohlcv_df)
                >>> anomalies = results['filtered_anomalies_df']

            Custom contamination and features:
                >>> results = driver.detect_anomalies(
                ...     ohlcv_df,
                ...     ohlcv_features_list=['returns', 'volatility'],
                ...     contamination=0.01,
                ...     period_filter='7d'
                ... )

            With plots and full data:
                >>> results = driver.detect_anomalies(
                ...     ohlcv_df,
                ...     generate_plots='enabled',
                ...     return_full_data=True
                ... )
                >>> full_data = results['ohlcv_df']
        """
        from chronos_lab.analysis.dag import standardize, features, anomaly

        if ohlcv_features_list is None:
            ohlcv_features_list = ['returns', 'volume_change', 'high_low_range']
        if plot_to_store_kwargs is None:
            plot_to_store_kwargs = {}
        if not sklearn_kwargs:
            sklearn_kwargs = {'n_estimators': 200, 'max_samples': 250}

        config = {
            'use_adjusted': use_adjusted,
            'ohlcv_features_list': ohlcv_features_list,
            'contamination': contamination,
            'sklearn_kwargs': sklearn_kwargs,
            'generate_plots': generate_plots,
            'plot_to_store_kwargs': plot_to_store_kwargs,
            'anomaly_period_filter': anomaly_period_filter,
            'return_ohlcv_df': return_ohlcv_df,
            'to_dataset': to_dataset,
            'dataset_name': dataset_name,
            'ddb_dataset_ttl': ddb_dataset_ttl
        }

        driver_name = 'detect_anomalies'
        if driver_name not in self.drivers:
            self.drivers[driver_name] = self._build_default_driver(
                modules=[standardize, features, anomaly],
                config=config,
                local_executor_type=local_executor_type,
                remote_executor_type=remote_executor_type,
                max_parallel_tasks=max_parallel_tasks
            )

        result = self.drivers[driver_name].execute(
            final_vars=['anomalies_complete'],
            inputs={'source_ohlcv': ohlcv}
        )

        return result['anomalies_complete']

    def _build_default_driver(
            self,
            modules: List[ModuleType],
            config: Dict[str, Any] = None,
            local_executor_type: str = None,
            remote_executor_type: str = None,
            max_parallel_tasks: int = None,
            cache_config: Dict[str, Any] = None,
    ) -> driver.Driver:
        """Build Hamilton Driver with standard configuration.

        Creates a Hamilton Driver configured with the specified DAG modules and execution
        parameters. Handles executor setup, caching configuration, and dynamic execution
        enablement for Parallelizable constructs.

        Args:
            modules: List of Python modules containing Hamilton function definitions
                (DAG nodes). Modules are loaded in order and can reference functions
                from earlier modules.
            config: Configuration dictionary passed to Hamilton's Builder. Defaults to None.
            remote_executor_type: Override the instance's default executor type. Options:
                'multithreading' or 'multiprocessing'. If None, uses self._executor_type.
                Defaults to None.
            max_parallel_tasks: Override the instance's default maximum parallel tasks.
                If None, uses self._max_parallel_tasks. Defaults to None.
            cache_config: Configuration dictionary passed to Hamilton's Builder.with_cache().
                Defaults to None.

        Returns:
            Configured Hamilton Driver instance ready for execution.

        Raises:
            ValueError: If an unknown executor_type is provided.
        """

        local_executor_type = local_executor_type or self._local_executor_type
        remote_executor_type = remote_executor_type or self._remote_executor_type
        max_tasks = max_parallel_tasks or self._max_parallel_tasks

        if local_executor_type == 'synchronous':
            local_executor = executors.SynchronousLocalTaskExecutor()
        else:
            raise ValueError(
                f"Unknown local executor type: {local_executor_type}."
                f"Expected 'synchronous'."
            )

        if remote_executor_type == 'multithreading':
            remote_executor = executors.MultiThreadingExecutor(max_tasks=max_tasks)
        elif remote_executor_type == 'multiprocessing':
            remote_executor = executors.MultiProcessingExecutor(max_tasks=max_tasks)
        else:
            raise ValueError(
                f"Unknown executor type: {remote_executor_type}. "
                f"Expected 'multithreading' or 'multiprocessing'."
            )

        builder = driver.Builder().with_modules(*modules)

        if config:
            builder = builder.with_config(config)

        builder = (
            builder
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_local_executor(local_executor)
            .with_remote_executor(remote_executor)
        )

        if self._enable_cache:
            from pathlib import Path

            builder = builder.with_cache(path=Path(self._cache_path).expanduser(),
                                         **cache_config or {})

        return builder.build()
