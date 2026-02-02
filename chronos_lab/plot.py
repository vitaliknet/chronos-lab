"""Plotting utilities for visualizing OHLCV data with anomaly detection results.

IMPORTANT: This module is NOT part of the official documented API of chronos_lab.
It is provided as a support module for tutorial notebooks and examples only.
This module may be modified, moved, or removed from chronos_lab at any time
without warning or deprecation notice.

DO NOT use this module in production code. If you need plotting functionality,
copy the relevant functions to your own codebase.

This module provides:
    - plot_ohlcv_anomalies(): Generate Bloomberg-style candlestick charts with anomaly highlights
    - human_format(): Format large numbers with K/M/B/T suffixes for axis labels
    - bloomberg_style: Predefined mplfinance style with black background

Dependencies:
    - mplfinance: For candlestick charting
    - matplotlib: For plot customization
    - Requires optional dependencies not included in core chronos_lab installation
"""

import pandas as pd
from typing import Dict, Any, Optional
from chronos_lab.storage import to_store
from chronos_lab import logger
import mplfinance as mpf
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter, FuncFormatter
from io import BytesIO

bloomberg_style = mpf.make_mpf_style(
    base_mpf_style='charles',
    rc={
        'figure.facecolor': '#000000',
        'axes.facecolor': '#000000',
        'axes.edgecolor': '#404040',
        'axes.labelcolor': '#CCCCCC',
        'xtick.color': '#CCCCCC',
        'ytick.color': '#CCCCCC',
        'grid.color': '#404040',
        'grid.alpha': 0.3,
    },
    marketcolors=mpf.make_marketcolors(
        up='#00ff00',
        down='#ff0000',
        edge='inherit',
        wick='inherit',
        volume={'up': '#00ff00', 'down': '#ff0000'},
        alpha=0.8
    )
)


def human_format(x, pos):
    """Format large numbers with K, M, B, T suffixes for chart axis labels.

    Args:
        x: Numeric value to format.
        pos: Position on the axis (unused, required by matplotlib).

    Returns:
        Formatted string with appropriate suffix (K, M, B, T) or raw number if less than 1000.
    """
    abs_x = abs(x)
    if abs_x >= 1_000_000_000_000:
        return f'{x/1_000_000_000_000:.1f}T'
    elif abs_x >= 1_000_000_000:
        return f'{x/1_000_000_000:.1f}B'
    elif abs_x >= 1_000_000:
        return f'{x/1_000_000:.1f}M'
    elif abs_x >= 1_000:
        return f'{x/1_000:.1f}K'
    else:
        return f'{x:.0f}'

def plot_ohlcv_anomalies(
        ohlcv_anomalies_df: pd.DataFrame,
        anomaly_period_filter: Optional[str] = None,
        plot_to_store: Optional[bool] = False,
        to_store_kwargs=None
) -> Dict[str, Any]:
    """Generate Bloomberg-style candlestick chart with highlighted anomalies.

    Creates a three-panel visualization showing OHLCV data with anomalies marked as orange
    markers and bars. The chart includes: (1) candlestick price panel with anomaly markers,
    (2) volume bars panel with anomaly highlights, and (3) returns line/bar panel with
    anomaly highlights. Uses black background with green (up) / red (down) coloring
    similar to Bloomberg Terminal.

    Args:
        ohlcv_anomalies_df: DataFrame with MultiIndex (date, symbol) containing OHLCV data
            and anomaly detection results. Must include columns: 'open', 'high', 'low',
            'close', 'volume', 'returns', 'is_anomaly'. Typically output from
            AnalysisDriver.detect_anomalies().
        anomaly_period_filter: Optional period string to filter which anomalies to highlight
            on the chart relative to the latest date in the DataFrame (e.g., '1m', '7d',
            '2w'). If None, all anomalies in the DataFrame are highlighted. Defaults to None.
        plot_to_store: Whether to save the plot to storage using the to_store function.
            If True, saves to configured storage backend. If False, displays plot and returns
            raw plot data. Defaults to False.
        to_store_kwargs: Additional keyword arguments passed to the to_store function when
            plot_to_store=True. Common options: 'stores' (list: ['local'] or ['s3'] or both),
            'folder' (subdirectory/prefix), 's3_metadata' (dict). Defaults to None.

    Returns:
        If plot_to_store=False: Dictionary with keys 'file_name' (str), 'content' (bytes),
            'mime_type' ('image/png').
        If plot_to_store=True: Dictionary with storage result, typically includes
            'local_statusCode', 'file_path' (local) or 's3_statusCode', 's3_client_response' (S3).
        Returns empty dict {} if no anomalies found after filtering.

    Examples:
        Generate plot without saving to storage:
            >>> from chronos_lab.sources import ohlcv_from_yfinance
            >>> from chronos_lab.analysis.driver import AnalysisDriver
            >>> from chronos_lab.plot import plot_ohlcv_anomalies
            >>>
            >>> # Fetch and detect anomalies
            >>> ohlcv = ohlcv_from_yfinance(symbols=['AAPL'], period='1y')
            >>> driver = AnalysisDriver()
            >>> result = driver.detect_anomalies(ohlcv=ohlcv)
            >>>
            >>> # Generate plot for AAPL, highlighting recent month
            >>> aapl_data = result['analysis_result'].xs('AAPL', level='symbol')
            >>> plot_data = plot_ohlcv_anomalies(
            ...     aapl_data,
            ...     anomaly_period_filter='1m',
            ...     plot_to_store=False
            ... )
            >>>
            >>> # Save to file manually
            >>> with open(plot_data['file_name'], 'wb') as f:
            ...     f.write(plot_data['content'])

        Generate and save plot to local storage:
            >>> from chronos_lab.sources import ohlcv_from_arcticdb
            >>> from chronos_lab.analysis.driver import AnalysisDriver
            >>> from chronos_lab.plot import plot_ohlcv_anomalies
            >>>
            >>> # Detect anomalies from stored data
            >>> ohlcv = ohlcv_from_arcticdb(symbols=['TSLA'], period='6m')
            >>> driver = AnalysisDriver()
            >>> result = driver.detect_anomalies(ohlcv=ohlcv)
            >>>
            >>> # Generate plot and save to local storage
            >>> tsla_data = result['analysis_result'].xs('TSLA', level='symbol')
            >>> store_result = plot_ohlcv_anomalies(
            ...     tsla_data,
            ...     anomaly_period_filter='2w',
            ...     plot_to_store=True,
            ...     to_store_kwargs={'stores': ['local'], 'folder': 'anomaly_charts'}
            ... )
            >>> print(f"Chart saved to: {store_result['file_path']}")

        Generate and save plot to S3:
            >>> store_result = plot_ohlcv_anomalies(
            ...     tsla_data,
            ...     anomaly_period_filter='2w',
            ...     plot_to_store=True,
            ...     to_store_kwargs={
            ...         'stores': ['s3'],
            ...         'folder': 'anomalies',
            ...         's3_metadata': {'symbol': 'TSLA', 'type': 'anomaly_chart'}
            ...     }
            ... )
            >>> if store_result['s3_statusCode'] == 0:
            ...     print("Successfully saved to S3")

    Note:
        - Requires mplfinance and matplotlib (not included in core chronos_lab dependencies)
        - Chart filename format: {symbol}_anomaly_{start_date}-{end_date}.png
        - Returns empty dict if no anomalies found (warning logged)
        - Plot shows anomalies as orange markers/bars overlaid on normal data
        - Volume axis uses human-readable format (K, M, B, T suffixes)
        - Returns axis uses percentage format
    """
    if to_store_kwargs is None:
        to_store_kwargs = {}
    symbol = ohlcv_anomalies_df.index.get_level_values('symbol').unique()[0]
    ohlcv_anomalies_df = ohlcv_anomalies_df.copy().reset_index(level=1)

    dates = ohlcv_anomalies_df.index.get_level_values('date')

    if anomaly_period_filter:
        from chronos_lab._utils import _period

        _start_date, _end_date = _period(
            anomaly_period_filter,
            as_of=dates.max()
        )

        logger.info(f"Filtering anomalies plot to period {_start_date} - {_end_date}")

        anomaly_mask = (
                ohlcv_anomalies_df['is_anomaly']
                & (dates >= _start_date)
                & (dates <= _end_date)
        )
    else:
        anomaly_mask = ohlcv_anomalies_df['is_anomaly']

    if not anomaly_mask.any():
        logger.warning(f"No anomalies found for symbol {symbol}. Skipping plot generation.")
        return {}

    start_date = ohlcv_anomalies_df.index.get_level_values('date').min().strftime('%Y%m%d')
    end_date = ohlcv_anomalies_df.index.get_level_values('date').max().strftime('%Y%m%d')
    file_name = f"{symbol}_anomaly_{start_date}-{end_date}.png"

    logger.info(f"Generating anomalies plot for {symbol}")

    apds = [
        mpf.make_addplot(
            # (ohlcv_anomalies_df['high'] - ohlcv_anomalies_df['low']).where(anomaly_mask),
            # bottom=ohlcv_anomalies_df['low'].where(anomaly_mask),
            # type='bar',
            ohlcv_anomalies_df['close'].where(anomaly_mask),
            type='scatter',
            panel=0,
            marker='o',
            markersize=30,
            color='orange'
        ),

        mpf.make_addplot(
            ohlcv_anomalies_df['returns'],
            type='line',
            panel=2,
            color='white',
            secondary_y=False,
            ylim=(ohlcv_anomalies_df['returns'].min(), ohlcv_anomalies_df['returns'].max()),
            ylabel='Return'
        ),

        mpf.make_addplot(
            ohlcv_anomalies_df['returns'].where(anomaly_mask),
            type='bar',
            panel=2,
            color='orange',
            secondary_y=False,
            ylim=(ohlcv_anomalies_df['returns'].min(), ohlcv_anomalies_df['returns'].max()),
        ),

        mpf.make_addplot(
            ohlcv_anomalies_df['volume'].where(anomaly_mask),
            type='bar',
            panel=1,
            color='orange'
        ),
    ]

    fig, axes = mpf.plot(
        ohlcv_anomalies_df,
        type='candle',
        style=bloomberg_style,
        volume=True,
        addplot=apds,
        figsize=(16, 12),
        panel_ratios=(3, 2, 2),
        returnfig=True,
        ylabel='Price',
        ylabel_lower='Volume',
        datetime_format='%Y-%m-%d',
        xrotation=0
    )

    fig.suptitle(
        f"{symbol}",
        color='white',
        fontsize=20,
        y=0.92
    )

    axes[2].yaxis.set_major_formatter(FuncFormatter(human_format))
    axes[4].yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    axes[4].axhline(y=0, color='#C0C0C0', linestyle='--', linewidth=0.8)

    for ax in axes:
        ax.set_facecolor('#000000')
        # ax.yaxis.set_label_position('left')
        # ax.yaxis.tick_left()

        ax.grid(True, which='major', linestyle=':', linewidth=1, color='#C0C0C0')

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#000000')
    buf.seek(0)
    content = buf.read()
    buf.close()

    if plot_to_store:
        plt.close(fig)
        return to_store(file_name=file_name,
                        content=content,
                        **to_store_kwargs)
    else:
        plt.show(fig)
        plt.close(fig)
        return {'file_name': file_name, 'content': content, 'mime_type': 'image/png'}
