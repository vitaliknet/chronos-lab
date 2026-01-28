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

    Creates a multi-panel visualization showing OHLCV data with anomalies marked as orange
    markers/bars. The chart includes three panels: price (candlestick), volume, and returns.
    Uses black background with green/red coloring similar to Bloomberg Terminal.

    Args:
        ohlcv_anomalies_df: DataFrame with MultiIndex (date, symbol) containing OHLCV data
            and anomaly detection results. Must include columns: 'open', 'high', 'low',
            'close', 'volume', 'returns', 'is_anomaly'.
        anomaly_period_filter: Optional period string to filter which anomalies to highlight
            on the chart (e.g., '1m', '7d', '2w'). If None, all anomalies in the DataFrame
            are highlighted. Defaults to None.
        plot_to_store: Whether to save the plot to storage using the to_store function.
            If True, saves to S3 or local storage based on configuration. If False, returns
            raw plot data. Defaults to False.
        to_store_kwargs: Additional keyword arguments passed to the to_store function when
            plot_to_store=True. Common options include 'bucket', 'prefix', 'local_path'.
            Defaults to None.

    Returns:
        Dictionary with plot metadata. If plot_to_store=False, returns {'file_name': str,
        'content': bytes, 'mime_type': 'image/png'}. If plot_to_store=True, returns result
        from to_store function (typically includes 's3_path' or 'local_path'). Returns empty
        dict if no anomalies found.

    Examples:
        Generate plot without saving to storage:

        ```python
        from chronos_lab.sources import ohlcv_from_yfinance
        from chronos_lab.analysis import detect_ohlcv_anomalies
        from chronos_lab.plot import plot_ohlcv_anomalies

        # Detect anomalies
        ohlcv = ohlcv_from_yfinance(symbols=['AAPL'], period='1y')
        anomalies = detect_ohlcv_anomalies(ohlcv, output_dict=True)

        # Generate plot for Apple, highlighting recent month
        plot_data = plot_ohlcv_anomalies(
            anomalies['AAPL'],
            anomaly_period_filter='1m',
            plot_to_store=False
        )

        # Save to file manually
        with open(plot_data['file_name'], 'wb') as f:
            f.write(plot_data['content'])
        ```

        Generate and save plot to S3:

        ```python
        from chronos_lab.sources import ohlcv_from_arcticdb
        from chronos_lab.analysis import detect_ohlcv_anomalies
        from chronos_lab.plot import plot_ohlcv_anomalies

        # Detect anomalies from stored data
        ohlcv = ohlcv_from_arcticdb(symbols=['TSLA'], period='6m')
        anomalies = detect_ohlcv_anomalies(
            ohlcv,
            generate_plots='disabled',
            to_dataset='disabled',
            output_dict=True
        )

        # Generate plot and save to S3
        result = plot_ohlcv_anomalies(
            anomalies['TSLA'],
            anomaly_period_filter='2w',
            plot_to_store=True,
            to_store_kwargs={'bucket': 'my-charts-bucket', 'prefix': 'anomalies/'}
        )

        print(f"Chart saved to: {result['s3_path']}")
        ```
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
