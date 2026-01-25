import pandas as pd
from typing import Dict, Any, Optional
from chronos_lab.storage import to_store
from chronos_lab import logger
import mplfinance as mpf
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
    """
    Format numbers as 1K, 1M, 1B, 1T, ...
    1_000      -> 1.0K
    2_000_000  -> 2.0M
    3_000_000_000 -> 3.0B
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
        return to_store(file_name=file_name,
                        content=content,
                        **to_store_kwargs)
    else:
        return {'file_name': file_name, 'content': content, 'mime_type': 'image/png'}
