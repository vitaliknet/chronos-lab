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
    """

    def __init__(
            self,
            *,
            enable_cache: bool = False,
            cache_path: str = None,
            local_executor_type: Optional[str] = 'synchronous',
            remote_executor_type: str = 'multithreading',
            max_parallel_tasks: int = 5,
            enable_telemetry: bool = False
    ):
        """Initialize AnalysisDriver with shared configuration.

        Args:
            enable_cache: Enable Hamilton caching for expensive computations.
                Defaults to False.
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
            ohlcv: Optional[pd.DataFrame] = None,
            ohlcv_from_source: str = 'disabled',
            ohlcv_from_config: Dict[str, Any] = None,
            ohlcv_features_list: List[str] = None,
            use_adjusted: bool = True,
            isolation_forest_config: Dict[str, Any] = None,
            to_dataset: str = 'disabled',
            to_dataset_config: Dict[str, Any] = None,
            to_arcticdb: str = 'disabled',
            to_arcticdb_config: Dict[str, Any] = None,
            driver_config: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Detect anomalies in OHLCV time series data using Isolation Forest.

        Executes a Hamilton DAG that standardizes OHLCV data, computes features,
        applies Isolation Forest anomaly detection, and optionally persists results
        to datasets or ArcticDB. Supports multiple data sources (Yahoo Finance,
        Intrinio, ArcticDB) or direct DataFrame input.

        Args:
            ohlcv: Pre-loaded OHLCV DataFrame with MultiIndex (date, symbol).
                Required if ohlcv_from_source is 'disabled'. Defaults to None.
            ohlcv_from_source: Data source for OHLCV retrieval. Options: 'disabled'
                (use ohlcv parameter), 'yfinance', 'intrinio', or 'arcticdb'.
                Defaults to 'disabled'.
            ohlcv_from_config: Configuration dictionary passed to the data source
                function. Required when ohlcv_from_source is not 'disabled'.
                Defaults to None.
            ohlcv_features_list: List of feature names to compute from OHLCV data.
                Options: 'returns', 'volume_change', 'high_low_range', 'volatility'.
                Defaults to ['returns', 'volume_change', 'high_low_range'].
            use_adjusted: Whether to use adjusted OHLCV columns (adj_close, etc.)
                if available. Defaults to True.
            isolation_forest_config: Configuration dictionary for sklearn's
                IsolationForest. Defaults to {'contamination': 0.02,
                'random_state': 42, 'n_estimators': 200, 'max_samples': 250}.
            to_dataset: Whether to save anomaly results to a dataset. Options:
                'disabled' or 'enabled'. Defaults to 'disabled'.
            to_dataset_config: Configuration for dataset output. Defaults to
                {'dataset_name': 'ohlcv_anomalies', 'ddb_dataset_ttl': 7}.
            to_arcticdb: Whether to save results to ArcticDB. Options: 'disabled'
                or 'enabled'. Defaults to 'disabled'.
            to_arcticdb_config: Configuration for ArcticDB output. Defaults to
                {'backend': 'LMDB', 'library_name': 'analysis',
                'symbol_prefix': '', 'symbol_suffix': '_ohlcv_anomaly'}.
            driver_config: Additional configuration passed to the Hamilton Driver
                builder. Defaults to {}.

        Returns:
            Dictionary containing execution results with keys 'analysis_result'
            (DataFrame with anomaly scores and flags), 'analysis_to_dataset'
            (dataset save status), and 'analysis_to_arcticdb' (ArcticDB save status).

        Raises:
            ValueError: If neither ohlcv nor ohlcv_from_source is provided, or if
                ohlcv_from_source is unsupported, or if ohlcv_from_config is missing
                when required.
        """
        from chronos_lab.analysis.dag import standardize, features, anomaly, io

        if ohlcv is None and ohlcv_from_source is None:
            raise ValueError("Either ohlcv or ohlcv_from_source must be provided.")

        if ohlcv_from_source not in ['disabled', 'yfinance', 'intrinio', 'arcticdb']:
            raise ValueError(f"Unsupported ohlcv_from_source: {ohlcv_from_source}")

        if ohlcv_from_source != 'disabled' and ohlcv_from_config is None:
            raise ValueError("ohlcv_from_config must be provided when ohlcv_from_source is not disabled.")

        if ohlcv_features_list is None:
            ohlcv_features_list = ['returns', 'volume_change', 'high_low_range']

        if to_dataset_config is None:
            to_dataset_config = {
                'dataset_name': 'ohlcv_anomalies',
                'ddb_dataset_ttl': 7,
            }

        if to_arcticdb_config is None:
            to_arcticdb_config = {
                'backend': 'LMDB',
                'library_name': 'analysis',
                'symbol_prefix': '',
                'symbol_suffix': '_ohlcv_anomaly',
            }

        if driver_config is None:
            driver_config = {
            }

        if isolation_forest_config is None:
            isolation_forest_config = {
                'contamination': 0.02,
                'random_state': 42,
                'n_estimators': 200,
                'max_samples': 250
            }

        config = {
            'ohlcv_from_source': ohlcv_from_source,
            'ohlcv_from_config': ohlcv_from_config,
            'use_adjusted': use_adjusted,
            'ohlcv_features_list': ohlcv_features_list,
            'to_dataset': to_dataset,
            'to_dataset_config': to_dataset_config,
            'to_arcticdb': to_arcticdb,
            'to_arcticdb_config': to_arcticdb_config,
            'isolation_forest_config': isolation_forest_config,
        }

        driver_name = 'detect_anomalies'
        if driver_name not in self.drivers:
            self.drivers[driver_name] = self._build_default_driver(
                modules=[standardize, features, anomaly, io],
                config=config,
                **driver_config
            )

        result = self.drivers[driver_name].execute(
            final_vars=['analysis_result', 'analysis_to_dataset', 'analysis_to_arcticdb'],
            inputs={'source_ohlcv': ohlcv}
        )

        return result

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
