import pandas as pd
from typing import Dict, Any, List, Optional
from chronos_lab.storage import to_store
from chronos_lab import logger
from hamilton.htypes import Collect
from hamilton.function_modifiers import config


def detect_ohlcv_features_anomalies(
        ohlcv_features: pd.DataFrame,
        ohlcv_features_list: List[str],
        contamination: float,
        sklearn_kwargs: Dict[str, Any] = None
) -> pd.DataFrame:
    from sklearn.ensemble import IsolationForest

    if sklearn_kwargs is None:
        sklearn_kwargs = {}

    model = IsolationForest(
        contamination=contamination,
        random_state=42,
        **sklearn_kwargs
    )
    ohlcv_features_df = ohlcv_features[ohlcv_features_list]

    X = ohlcv_features_df.values
    model.fit(X)

    anomaly_score = -model.score_samples(X)
    is_anomaly = model.predict(X) == -1

    result_df = pd.DataFrame(index=ohlcv_features_df.index)
    result_df['anomaly_score'] = anomaly_score
    result_df['is_anomaly'] = is_anomaly

    ranks = anomaly_score.argsort().argsort() + 1
    result_df['anomaly_rank'] = ranks

    return result_df


def ohlcv_by_symbol_with_features_anomalies(
        ohlcv_features: pd.DataFrame,
        detect_ohlcv_features_anomalies: pd.DataFrame
) -> pd.DataFrame:
    result = ohlcv_features.join(detect_ohlcv_features_anomalies, how='left')

    result['anomaly_score'] = result['anomaly_score'].fillna(0.0)
    result['is_anomaly'] = result['is_anomaly'].astype('boolean').fillna(False)
    result['anomaly_rank'] = result['anomaly_rank'].fillna(0).astype(int)

    return result

@config.when(generate_plots="enabled")
def anomalies_plot__enabled(
        ohlcv_by_symbol_with_features_anomalies: pd.DataFrame,
        anomaly_period_filter: Optional[str] = None,
        plot_to_store_kwargs: Optional[dict] = None
) -> Dict[str, Any]:
    import mplfinance as mpf
    from io import BytesIO

    ohlcv_anomalies_df = ohlcv_by_symbol_with_features_anomalies.copy().reset_index(level=1)
    symbol = ohlcv_by_symbol_with_features_anomalies.index.get_level_values('symbol').unique()[0]

    response = {
        'ohlcv_anomalies_df': ohlcv_by_symbol_with_features_anomalies
    }

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
        logger.info(f"No anomalies found for symbol {symbol}. Skipping plot generation.")
        return response

    start_date = ohlcv_anomalies_df.index.get_level_values('date').min().strftime('%Y%m%d')
    end_date = ohlcv_anomalies_df.index.get_level_values('date').max().strftime('%Y%m%d')
    file_name = f"{symbol}_anomaly_{start_date}-{end_date}.png"

    logger.info(f"Generating anomalies plot for {symbol}")

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

    apds = [
        mpf.make_addplot(
            ohlcv_anomalies_df['close'].where(anomaly_mask),
            type='scatter',
            panel=0,
            marker='o',
            markersize=30,
            color='orange'
        ),

        mpf.make_addplot(
            ohlcv_anomalies_df['returns'],
            type='bar',
            panel=2,
            color='white',
            ylim=(ohlcv_anomalies_df['returns'].min(), ohlcv_anomalies_df['returns'].max()),
            ylabel='Daily return'
        ),

        mpf.make_addplot(
            ohlcv_anomalies_df['returns'].where(anomaly_mask),
            type='bar',
            panel=2,
            color='orange',
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
        figsize=(14, 10),
        panel_ratios=(3, 2, 2),
        returnfig=True,
        ylabel='Price (USD)',
        ylabel_lower='Volume',
        datetime_format='%Y-%m-%d',
        xrotation=0
    )

    for ax in axes:
        ax.set_facecolor('#000000')
        ax.yaxis.set_label_position('left')
        ax.yaxis.tick_left()

    axes[2].axhline(y=0, color='#808080', linestyle='--', linewidth=0.8)

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#000000')
    buf.seek(0)
    content = buf.read()
    buf.close()

    response['plot_to_store'] = to_store(file_name=file_name,
                                         content=content,
                                         **plot_to_store_kwargs)
    return response


@config.when(generate_plots="disabled")
def anomalies_plot__disabled(
        ohlcv_by_symbol_with_features_anomalies: pd.DataFrame,
) -> Dict[str, Any]:
    symbol = ohlcv_by_symbol_with_features_anomalies.index.get_level_values('symbol').unique()[0]
    logger.info(f"Skipping anomalies plot for {symbol}")
    return {'ohlcv_anomalies_df': ohlcv_by_symbol_with_features_anomalies}


def anomaly_events(anomalies_plot: Dict[str, Any],
                   anomaly_period_filter: Optional[str] = None,
                   return_ohlcv_anomalies_df: Optional[bool] = False
                   ) -> dict[str, Any]:
    ohlcv_anomalies_df = anomalies_plot['ohlcv_anomalies_df'].copy()
    symbol = ohlcv_anomalies_df.index.get_level_values('symbol').unique()[0]

    if anomaly_period_filter:
        from chronos_lab._utils import _period

        dates = ohlcv_anomalies_df.index.get_level_values('date')
        start_date, end_date = _period(anomaly_period_filter,
                                       as_of=dates.max())
        anomalies = ohlcv_anomalies_df[ohlcv_anomalies_df['is_anomaly'] & (dates >= start_date) & (dates <= end_date)]
    else:
        anomalies = ohlcv_anomalies_df[ohlcv_anomalies_df['is_anomaly']]

    if len(anomalies) > 0:
        anomalies_plot['anomaly_events'] = anomalies

    if return_ohlcv_anomalies_df:
        return {symbol: anomalies_plot}
    else:
        del anomalies_plot['ohlcv_anomalies_df']
        return {symbol: anomalies_plot}

def anomalies_complete(
        anomaly_events: Collect[Dict[str, Any]]) -> pd.DataFrame:
    return anomaly_events

