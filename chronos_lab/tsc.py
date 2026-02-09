"""Time Series Collection for multi-symbol, multi-series data orchestration.

Provides flexible ingestion from multiple DataFrame formats with automatic detection,
efficient storage, and unified retrieval with configurable alignment strategies.
"""

from dataclasses import asdict, dataclass, field
from io import StringIO
from typing import Literal

import pandas as pd

from chronos_lab import logger


@dataclass
class SeriesMetadata:
    """Metadata for a single time series.

    Attributes:
        symbol: Ticker symbol or identifier
        name: Series name (e.g., 'close', 'volume', 'sma_20')
        frequency: Pandas frequency string ('1d', '1h', '5min', etc.)
        source: Data source identifier ('yfinance', 'arcticdb', 'calculated', etc.)
        last_update: Timestamp of most recent data point
        forecast_origin: Boundary between historical and forecasted data
        color: Display color hint for visualization
        line_style: Line style hint ('solid', 'dash', 'dot')
        opacity: Opacity hint (0.0 to 1.0)
        display_axis: Subplot assignment (1=price, 2=volume, 3=indicators, etc.)
        custom: User-defined metadata key-value pairs
    """

    symbol: str
    name: str
    frequency: str
    source: str
    last_update: pd.Timestamp
    forecast_origin: pd.Timestamp | None = None
    color: str | None = None
    line_style: str = "solid"
    opacity: float = 1.0
    display_axis: int = 1
    custom: dict = field(default_factory=dict)

    @property
    def is_forecast(self) -> bool:
        """Check if this series includes forecast data."""
        return self.forecast_origin is not None


class TimeSeriesCollection:
    """Multi-symbol, multi-series time series orchestration engine.

    Handles heterogeneous time series data with different symbols, frequencies, and sources.
    Provides flexible ingestion from multiple DataFrame formats, efficient storage keyed by
    (symbol, name), and unified retrieval with configurable alignment strategies.

    Design principles:
        - Native frequency preservation (no forced resampling)
        - Forward-fill alignment when joining different frequencies
        - Efficient streaming updates (upsert mode)
        - Memory management via rolling time windows
        - Rich metadata support (display hints + custom fields)

    Storage model:
        Internal: dict[(symbol, name)] -> DataFrame with single column
        External: MultiIndex DataFrame with configurable column order
    """

    # Known metadata fields that map to SeriesMetadata attributes
    _KNOWN_METADATA_FIELDS = {
        'source', 'forecast_origin', 'color', 'line_style', 'opacity', 'display_axis'
    }

    def __init__(
            self,
            alignment: Literal["ffill", "strict", "none"] = "ffill",
            column_order: Literal["symbol_first", "series_first"] = "symbol_first",
            max_window: str | None = None,
    ):
        """Initialize TimeSeriesCollection.

        Args:
            alignment: Alignment strategy for mixed frequencies:
                - 'ffill': Forward-fill lower frequencies onto unified timeline (default)
                - 'strict': Reject series with different frequencies
                - 'none': No alignment, keep NaNs where frequencies don't match
            column_order: MultiIndex column structure for get_series():
                - 'symbol_first': (symbol, series_name) - group by symbol
                - 'series_first': (series_name, symbol) - group by metric
            max_window: Rolling window size for memory management:
                - '30d': Keep last 30 days
                - '1000': Keep last 1000 bars
                - None: No limit (default)
        """
        self._data: dict[tuple[str, str], pd.DataFrame] = {}
        self._metadata: dict[tuple[str, str], SeriesMetadata] = {}
        self._alignment = alignment
        self._column_order = column_order
        self._max_window = max_window
        self._primary_frequency: str | None = None

    def add_series(
            self,
            data: pd.DataFrame,
            symbol: str | None = None,
            frequency: str | None = None,
            metadata: dict | None = None,
            metadata_series: dict[str, dict] | None = None,
            mode: Literal["add", "upsert"] = "add",
            **fallback_metadata,
    ) -> None:
        """Add or update one or more time series with automatic format detection.

        Supported formats (auto-detected):
            1. Tall: MultiIndex[(date, symbol/id)] with data columns
            2. Wide multi: DatetimeIndex with MultiIndex[(symbol, series)] columns
            3. Wide single: DatetimeIndex with single-level columns (requires symbol arg)

        Args:
            data: DataFrame in any supported format
            symbol: Symbol identifier (required for wide single format, ignored otherwise)
            frequency: Pandas frequency string (inferred from index if None, defaults to 'B')
            metadata: Common metadata applied to all series in this call
            metadata_series: Per-series-name metadata (e.g., {'volume': {'display_axis': 2}})
            mode: Storage mode:
                - 'add': Raise error if series exists (default, safe for initial loads)
                - 'upsert': Insert new or update existing series (idempotent, for updates)
            **fallback_metadata: Lowest-priority metadata defaults

        Metadata precedence: metadata_series[name] > metadata > fallback_metadata
        Known fields: source, forecast_origin, color, line_style, opacity, display_axis
        Unknown fields: Stored in SeriesMetadata.custom dict

        Raises:
            ValueError: Invalid format, missing required symbol parameter, or mode='add' with existing series

        Examples:
            # Initial load (safe mode)
            collection.add_series(
                historical_df,
                symbol='AAPL',
                frequency='1d',
                metadata={'source': 'yfinance'},
                mode='add'  # Will error if run twice
            )

            # Daily updates (idempotent)
            collection.add_series(
                daily_bars,
                symbol='AAPL',
                mode='upsert'  # Updates existing + appends new
            )

            # Tall format with multiple symbols
            collection.add_series(
                multi_symbol_df,  # MultiIndex: (date, symbol)
                frequency='1d',
                metadata={'source': 'yfinance'},
                mode='upsert'
            )

            # Wide format with multi-index columns
            collection.add_series(
                pivot_df,  # Columns: MultiIndex[(AAPL, close), (AAPL, volume), ...]
                metadata_series={'volume': {'display_axis': 2}},
                mode='upsert'
            )
        """
        if not isinstance(data.index, (pd.DatetimeIndex, pd.MultiIndex)):
            raise ValueError("Data must have DatetimeIndex or MultiIndex")

        if isinstance(data.index, pd.MultiIndex):
            if symbol is not None:
                logger.warning(
                    f"Ignoring symbol parameter '{symbol}' for tall format. "
                    f"Symbols extracted from index level 1."
                )
            self._add_series_tall(
                data, frequency, metadata, metadata_series, fallback_metadata, mode
            )
        elif isinstance(data.columns, pd.MultiIndex):
            if symbol is not None:
                logger.warning(
                    f"Ignoring symbol parameter '{symbol}' for wide multi-index format. "
                    f"Symbols extracted from column index."
                )
            self._add_series_wide_multi(
                data, frequency, metadata, metadata_series, fallback_metadata, mode
            )
        else:
            if symbol is None:
                raise ValueError(
                    "symbol parameter required for single-level columns. "
                    "Use MultiIndex format to avoid this requirement."
                )
            self._add_series_wide_single(
                data, symbol, frequency, metadata, metadata_series, fallback_metadata, mode
            )

    def _infer_and_validate_frequency(
            self,
            data: pd.DataFrame,
            frequency: str | None,
            is_tall_format: bool = False,
    ) -> str:
        """Infer frequency from data and validate against alignment mode.

        Args:
            data: Input DataFrame
            frequency: User-provided frequency or None
            is_tall_format: If True, infer from first symbol's dates

        Returns:
            Validated frequency string

        Raises:
            ValueError: If alignment='strict' and frequency doesn't match primary frequency
        """
        if frequency is None:
            if is_tall_format:
                # Tall format: infer from first symbol
                first_symbol = data.index.get_level_values(1)[0]
                first_symbol_dates = data.index.get_level_values(0)[
                    data.index.get_level_values(1) == first_symbol
                ]
                frequency = pd.infer_freq(first_symbol_dates)
            else:
                # Wide formats: infer from index directly
                frequency = pd.infer_freq(data.index)

            if frequency is None:
                logger.warning(
                    "Could not infer frequency. Defaulting to 'B' (business day). "
                    "Pass explicit frequency if incorrect."
                )
                frequency = "B"

        # Validate against alignment mode
        if self._alignment == "strict":
            if self._primary_frequency is None:
                self._primary_frequency = frequency
            elif self._primary_frequency != frequency:
                raise ValueError(
                    f"Frequency mismatch: {frequency} != {self._primary_frequency}. "
                    "Use alignment='ffill' or 'none' for mixed frequencies."
                )

        return frequency

    def _prepare_metadata(
            self,
            metadata: dict | None,
            metadata_series: dict[str, dict] | None,
            fallback_metadata: dict,
    ) -> tuple[dict, dict[str, dict]]:
        """Prepare metadata dictionaries for processing.

        Args:
            metadata: Common metadata for all series
            metadata_series: Per-series metadata overrides
            fallback_metadata: Default metadata values

        Returns:
            Tuple of (common_meta, metadata_series_dict)
        """
        common_meta = {**(metadata or {}), **fallback_metadata}
        metadata_series = metadata_series or {}
        return common_meta, metadata_series

    def _split_metadata(self, series_meta: dict) -> tuple[dict, dict]:
        """Split metadata into known and custom fields.

        Args:
            series_meta: Combined metadata dict

        Returns:
            Tuple of (known_meta, custom_meta)
        """
        known_meta = {
            k: v for k, v in series_meta.items()
            if k in self._KNOWN_METADATA_FIELDS
        }
        custom_meta = {
            k: v for k, v in series_meta.items()
            if k not in self._KNOWN_METADATA_FIELDS
        }
        return known_meta, custom_meta

    def _create_metadata(
            self,
            symbol: str,
            name: str,
            frequency: str,
            series_data: pd.DataFrame,
            known_meta: dict,
            custom_meta: dict,
    ) -> SeriesMetadata:
        """Create SeriesMetadata from parsed metadata fields.

        Args:
            symbol: Symbol identifier
            name: Series name
            frequency: Pandas frequency string
            series_data: DataFrame with the time series data
            known_meta: Known metadata fields (source, forecast_origin, etc.)
            custom_meta: Custom user-defined metadata

        Returns:
            SeriesMetadata instance
        """
        return SeriesMetadata(
            symbol=symbol,
            name=name,
            frequency=frequency,
            source=known_meta.get("source", "unknown"),
            last_update=series_data.index[-1],
            forecast_origin=known_meta.get("forecast_origin"),
            color=known_meta.get("color"),
            line_style=known_meta.get("line_style", "solid"),
            opacity=known_meta.get("opacity", 1.0),
            display_axis=known_meta.get("display_axis", 1),
            custom=custom_meta,
        )

    def _add_series_tall(
            self,
            data: pd.DataFrame,
            frequency: str | None,
            metadata: dict | None,
            metadata_series: dict[str, dict] | None,
            fallback_metadata: dict,
            mode: str,
    ) -> None:
        """Process tall format: MultiIndex[(date, symbol/id)] with data columns."""
        # Validate format
        if len(data.index.names) != 2:
            raise ValueError(
                f"Tall format requires 2-level MultiIndex, got {len(data.index.names)}"
            )

        level_name = data.index.names[1]
        if level_name not in ["symbol", "id"]:
            raise ValueError(
                f"Index level 1 must be named 'symbol' or 'id', got '{level_name}'"
            )

        # Common setup
        frequency = self._infer_and_validate_frequency(data, frequency, is_tall_format=True)
        common_meta, metadata_series = self._prepare_metadata(
            metadata, metadata_series, fallback_metadata
        )

        # Process each column x symbol combination
        level_1_values = data.index.get_level_values(1)
        unique_symbols = level_1_values.unique()

        for col in data.columns:
            series_meta = {**common_meta, **metadata_series.get(col, {})}
            known_meta, custom_meta = self._split_metadata(series_meta)

            for symbol in unique_symbols:
                mask = level_1_values == symbol
                series_data = data.loc[mask, col].to_frame()
                series_data.index = series_data.index.droplevel(1)

                key = (symbol, col)
                metadata_obj = self._create_metadata(
                    symbol, col, frequency, series_data, known_meta, custom_meta
                )
                self._store_series(key, series_data, metadata_obj, mode)

    def _add_series_wide_multi(
            self,
            data: pd.DataFrame,
            frequency: str | None,
            metadata: dict | None,
            metadata_series: dict[str, dict] | None,
            fallback_metadata: dict,
            mode: str,
    ) -> None:
        """Process wide format: DatetimeIndex with MultiIndex[(symbol/id, series)] columns."""
        # Validate format and determine symbol level
        if not isinstance(data.columns, pd.MultiIndex):
            raise ValueError("Wide multi format requires MultiIndex columns")

        names = data.columns.names
        if "symbol" in names:
            symbol_level = names.index("symbol")
        elif "id" in names:
            symbol_level = names.index("id")
        else:
            raise ValueError(
                f"Column MultiIndex must have 'symbol' or 'id' at level 0 or 1. "
                f"Got names: {names}"
            )
        series_level = 1 - symbol_level

        # Common setup
        frequency = self._infer_and_validate_frequency(data, frequency)
        common_meta, metadata_series = self._prepare_metadata(
            metadata, metadata_series, fallback_metadata
        )

        # Process each column
        for col_tuple in data.columns:
            symbol = col_tuple[symbol_level]
            name = col_tuple[series_level] or "value"

            series_meta = {**common_meta, **metadata_series.get(name, {})}
            known_meta, custom_meta = self._split_metadata(series_meta)

            key = (symbol, name)
            series_data = data[col_tuple].to_frame()

            metadata_obj = self._create_metadata(
                symbol, name, frequency, series_data, known_meta, custom_meta
            )
            self._store_series(key, series_data, metadata_obj, mode)

    def _add_series_wide_single(
            self,
            data: pd.DataFrame,
            symbol: str,
            frequency: str | None,
            metadata: dict | None,
            metadata_series: dict[str, dict] | None,
            fallback_metadata: dict,
            mode: str,
    ) -> None:
        """Process wide format: DatetimeIndex with single-level columns and explicit symbol."""
        # Common setup
        frequency = self._infer_and_validate_frequency(data, frequency)
        common_meta, metadata_series = self._prepare_metadata(
            metadata, metadata_series, fallback_metadata
        )

        # Process each column
        for col in data.columns:
            if col == 'symbol':  # Skip reserved column name
                continue

            series_meta = {**common_meta, **metadata_series.get(col, {})}
            known_meta, custom_meta = self._split_metadata(series_meta)

            key = (symbol, col)
            series_data = data[[col]]

            metadata_obj = self._create_metadata(
                symbol, col, frequency, series_data, known_meta, custom_meta
            )
            self._store_series(key, series_data, metadata_obj, mode)

    def _store_series(
            self,
            key: tuple[str, str],
            series_data: pd.DataFrame,
            metadata_obj: SeriesMetadata,
            mode: str,
    ) -> None:
        """Store or upsert a single series based on mode.

        Args:
            key: (symbol, name) tuple
            series_data: DataFrame with single column
            metadata_obj: SeriesMetadata instance
            mode: 'add' or 'upsert'

        Raises:
            ValueError: If mode='add' and series already exists
        """
        if mode == "add":
            if key in self._data:
                raise ValueError(
                    f"Series {key} already exists. Use mode='upsert' to update it."
                )
            self._data[key] = series_data
            self._metadata[key] = metadata_obj

        elif mode == "upsert":
            if key not in self._data:
                # New series - just add it
                self._data[key] = series_data
                self._metadata[key] = metadata_obj
            else:
                # Existing series - update existing timestamps + append new ones
                self._data[key].update(series_data)
                new_timestamps = series_data.index.difference(self._data[key].index)
                if len(new_timestamps) > 0:
                    self._data[key] = pd.concat([
                        self._data[key],
                        series_data.loc[new_timestamps]
                    ])
                self._metadata[key].last_update = series_data.index[-1]

        if self._max_window:
            self._apply_window(*key)

    def remove_series(
            self,
            symbol: str | None = None,
            name: str | None = None,
    ) -> None:
        """Remove one or more series from the collection.

        Args:
            symbol: Symbol identifier (if None, removes across all symbols)
            name: Series name (if None, removes all series for symbol)

        Behavior:
            - Both provided: Remove specific (symbol, name) series
            - Only symbol: Remove all series for that symbol
            - Only name: Remove all series with that name across all symbols
            - Neither: Remove all series from collection

        Examples:
            # Remove specific series
            collection.remove_series('AAPL', 'close')

            # Remove all 'volume' series across symbols
            collection.remove_series(name='volume')

            # Remove all series for AAPL
            collection.remove_series(symbol='AAPL')

            # Remove everything
            collection.remove_series()
        """
        # Case 1: Remove all series
        if symbol is None and name is None:
            count = len(self._data)
            if count > 0:
                logger.warning(f"Removing all {count} series from collection")
            self._data.clear()
            self._metadata.clear()
            return

        # Case 2: Remove specific series
        if symbol is not None and name is not None:
            key = (symbol, name)
            removed = self._data.pop(key, None) is not None
            self._metadata.pop(key, None)
            if not removed:
                logger.warning(f"Series {key} not found, nothing removed")
            return

        # Case 3: Remove all series for a symbol
        if symbol is not None:
            keys_to_remove = [k for k in self._data.keys() if k[0] == symbol]
            if not keys_to_remove:
                logger.warning(f"No series found for symbol '{symbol}', nothing removed")
            for key in keys_to_remove:
                self._data.pop(key)
                self._metadata.pop(key)
            return

        # Case 4: Remove all series with a given name (across symbols)
        if name is not None:
            keys_to_remove = [k for k in self._data.keys() if k[1] == name]
            if not keys_to_remove:
                logger.warning(f"No series found with name '{name}', nothing removed")
            for key in keys_to_remove:
                self._data.pop(key)
                self._metadata.pop(key)
            return

    def get_series(
            self,
            start: pd.Timestamp | None = None,
            end: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """Get all series aligned on a unified timeline.

        Creates a unified timeline from the union of all series timestamps (including
        forecast dates), then aligns all series onto this timeline according to the
        alignment mode set during initialization.

        Args:
            start: Start date filter (inclusive)
            end: End date filter (inclusive)

        Returns:
            DataFrame with MultiIndex columns based on column_order:
                - 'symbol_first': MultiIndex[(symbol, series_name)]
                - 'series_first': MultiIndex[(series_name, symbol)]

            Alignment behavior:
                - 'ffill': Lower frequencies forward-filled onto unified timeline
                - 'strict': All series must share same frequency
                - 'none': NaNs where frequencies don't align
        """
        if not self._data:
            return pd.DataFrame()

        filtered = {}
        for (symbol, name), df in self._data.items():
            mask = pd.Series(True, index=df.index)
            if start:
                mask &= df.index >= start
            if end:
                mask &= df.index <= end

            if mask.any():
                key = (
                    (symbol, name)
                    if self._column_order == "symbol_first"
                    else (name, symbol)
                )
                filtered[key] = df[mask].iloc[:, 0]

        if not filtered:
            return pd.DataFrame()

        all_timestamps = pd.DatetimeIndex([])
        for series in filtered.values():
            all_timestamps = all_timestamps.union(series.index)
        all_timestamps = all_timestamps.sort_values()

        if self._alignment == "none":
            result = pd.DataFrame(
                {key: series.reindex(all_timestamps) for key, series in filtered.items()}
            )
        else:
            aligned = {}
            for key, series in filtered.items():
                reindexed = series.reindex(all_timestamps)
                if self._alignment == "ffill":
                    reindexed = reindexed.ffill()
                aligned[key] = reindexed
            result = pd.DataFrame(aligned)

        result.columns = pd.MultiIndex.from_tuples(
            result.columns,
            names=(
                ["symbol", "series"]
                if self._column_order == "symbol_first"
                else ["series", "symbol"]
            ),
        )
        result = result.sort_index(axis=1)

        return result

    def list_series(self) -> list[SeriesMetadata]:
        """List all series with metadata.

        Returns:
            List of SeriesMetadata for all stored series
        """
        return list(self._metadata.values())

    def get_metadata(self, symbol: str, name: str) -> SeriesMetadata:
        """Get metadata for a specific series.

        Args:
            symbol: Symbol identifier
            name: Series name

        Returns:
            SeriesMetadata for the requested series

        Raises:
            KeyError: If series not found
        """
        return self._metadata[(symbol, name)]

    def get_forecast_origins(self) -> set[pd.Timestamp]:
        """Get all unique forecast origin timestamps.

        Useful for visualizing boundaries between historical and forecasted data.

        Returns:
            Set of forecast origin timestamps
        """
        return {
            meta.forecast_origin
            for meta in self._metadata.values()
            if meta.forecast_origin is not None
        }

    def _apply_window(self, symbol: str, name: str) -> None:
        """Trim old data based on max_window setting.

        Args:
            symbol: Symbol identifier
            name: Series name
        """
        if not self._max_window:
            return

        key = (symbol, name)
        df = self._data[key]
        window_str = self._max_window

        if window_str.endswith("d"):
            days = int(window_str[:-1])
            cutoff = df.index[-1] - pd.Timedelta(days=days)
            self._data[key] = df[df.index >= cutoff]
        elif window_str.isdigit():
            max_bars = int(window_str)
            self._data[key] = df.iloc[-max_bars:]

    def to_dict(self) -> dict:
        """Serialize to dict for caching or persistence.

        Returns:
            Dict with keys: 'data', 'metadata', 'config'
        """
        return {
            "data": {str(k): df.to_json(orient="table") for k, df in self._data.items()},
            "metadata": {str(k): asdict(v) for k, v in self._metadata.items()},
            "config": {
                "alignment": self._alignment,
                "column_order": self._column_order,
                "max_window": self._max_window,
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TimeSeriesCollection":
        """Deserialize from dict.

        Args:
            data: Dict from to_dict()

        Returns:
            Reconstructed TimeSeriesCollection
        """
        config = data["config"]
        collection = cls(
            alignment=config["alignment"],
            column_order=config["column_order"],
            max_window=config["max_window"],
        )

        for key_str, df_json in data["data"].items():
            key = eval(key_str)
            df = pd.read_json(StringIO(df_json), orient="table")
            df.index = pd.to_datetime(df.index)
            collection._data[key] = df

        for key_str, meta_dict in data["metadata"].items():
            key = eval(key_str)
            meta_dict["last_update"] = pd.Timestamp(meta_dict["last_update"])
            if meta_dict["forecast_origin"]:
                meta_dict["forecast_origin"] = pd.Timestamp(meta_dict["forecast_origin"])
            collection._metadata[key] = SeriesMetadata(**meta_dict)

        return collection