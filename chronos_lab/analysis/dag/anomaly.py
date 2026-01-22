import pandas as pd
from typing import Dict, Any, List
from chronos_lab.storage import to_store
from hamilton.htypes import Collect


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


def anomalies_plot(
        ohlcv_by_symbol_with_features_anomalies: pd.DataFrame,
) -> Dict[str, Any]:
    import matplotlib.pyplot as plt
    from io import BytesIO

    ohlcv_anomalies_df = ohlcv_by_symbol_with_features_anomalies.copy()
    anomalies = ohlcv_anomalies_df[ohlcv_anomalies_df['is_anomaly']]

    fig, axes = plt.subplots(3, 1, figsize=(14, 10))

    bar_width = 0.8
    axes[0].bar(ohlcv_anomalies_df.index.get_level_values('date'),
                ohlcv_anomalies_df['high'] - ohlcv_anomalies_df['low'],
                bottom=ohlcv_anomalies_df['low'], color='#2d2d2d', width=bar_width, alpha=0.6)
    axes[0].bar(anomalies.index.get_level_values('date'),
                anomalies['high'] - anomalies['low'],
                bottom=anomalies['low'], color='red', width=bar_width, alpha=0.8)
    axes[0].scatter(anomalies.index.get_level_values('date'),
                    anomalies['close'], color='red', s=50,
                    zorder=5, label=f'Anomalies ({len(anomalies)})')
    axes[0].set_ylabel('Price (USD)', fontsize=10)
    axes[0].legend(loc='upper left')
    axes[0].grid(alpha=0.3)

    axes[1].plot(ohlcv_anomalies_df.index.get_level_values('date'),
                 ohlcv_anomalies_df['returns'], color='#2d2d2d', linewidth=0.8)
    axes[1].scatter(anomalies.index.get_level_values('date'), anomalies['returns'], color='red', s=50, zorder=5)
    axes[1].axhline(y=0, color='gray', linestyle='--', linewidth=0.8)
    axes[1].set_ylabel('Daily return', fontsize=10)
    axes[1].grid(alpha=0.3)

    axes[2].bar(ohlcv_anomalies_df.index.get_level_values('date'), ohlcv_anomalies_df['volume'],
                color='#2d2d2d', width=1, alpha=0.6)
    axes[2].bar(anomalies.index.get_level_values('date'), anomalies['volume'], color='red', width=1, alpha=0.8)
    axes[2].set_ylabel('Volume', fontsize=10)
    axes[2].set_xlabel('Date', fontsize=10)
    axes[2].grid(alpha=0.3)

    plt.tight_layout()

    symbol = ohlcv_anomalies_df.index.get_level_values('symbol').unique()[0]
    start_date = ohlcv_anomalies_df.index.get_level_values('date').min().strftime('%Y%m%d')
    end_date = ohlcv_anomalies_df.index.get_level_values('date').max().strftime('%Y%m%d')
    file_name = f"{symbol}_anomaly_{start_date}-{end_date}.png"

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    content = buf.read()
    buf.close()
    plt.close(fig)

    return {
        'file_name': file_name,
        'content': content
    }

def anomalies_plot_to_store(anomalies_plot: Dict[str, Any]) -> Dict[str, Any]:
    return to_store(file_name=anomalies_plot['file_name'],
                    content=anomalies_plot['content'])

def anomalies_calculation_complete(
        anomalies_plot_to_store: Collect[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return anomalies_plot_to_store

#
#
# def create_anomalies_plot_mpf(
#         ohlcv_by_symbol_with_features_anomalies: pd.DataFrame,
# ) -> Dict[str, Any]:
#     import mplfinance as mpf
#     from io import BytesIO
#
#     ohlcv_anomalies_df = ohlcv_by_symbol_with_features_anomalies.copy()
#     dates = ohlcv_anomalies_df.index.get_level_values('date')
#     daily_return = ohlcv_anomalies_df['close'].pct_change()
#
#     anomalies = ohlcv_anomalies_df[ohlcv_anomalies_df['is_anomaly']]
#     anomaly_dates = anomalies.index.get_level_values('date')
#     anomaly_return = anomalies['close'].pct_change()
#
#     bloomberg_style = mpf.make_mpf_style(
#         base_mpf_style='charles',
#         rc={
#             'figure.facecolor': '#000000',
#             'axes.facecolor': '#000000',
#             'axes.edgecolor': '#404040',
#             'axes.labelcolor': '#CCCCCC',
#             'xtick.color': '#CCCCCC',
#             'ytick.color': '#CCCCCC',
#             'grid.color': '#404040',
#             'grid.alpha': 0.3,
#         },
#         marketcolors=mpf.make_marketcolors(
#             up='#00ff00',
#             down='#ff0000',
#             edge='inherit',
#             wick='inherit',
#             volume={'up': '#00ff00', 'down': '#ff0000'},
#             alpha=0.8
#         )
#     )
#
#     apds = [
#         mpf.make_addplot(anomalies['close'], type='scatter',
#                          markersize=50, marker='o', color='red',
#                          panel=0, secondary_y=False),
#         mpf.make_addplot(daily_return, type='line', color='white',
#                          width=0.8, panel=1, secondary_y=False, ylabel='Daily return'),
#         mpf.make_addplot(anomaly_markers_return['anomaly_return'], type='scatter',
#                          markersize=50, marker='o', color='red',
#                          panel=1, secondary_y=False),
#         mpf.make_addplot(anomaly_markers_volume['anomaly_volume'], type='scatter',
#                          markersize=50, marker='o', color='red',
#                          panel=2, secondary_y=False),
#     ]
#
#     fig, axes = mpf.plot(
#         ohlcv_anomalies_df,
#         type='candle',
#         style=bloomberg_style,
#         volume=True,
#         addplot=apds,
#         figsize=(14, 10),
#         panel_ratios=(3, 2, 2),
#         returnfig=True,
#         ylabel='Price (USD)',
#         ylabel_lower='Volume',
#         datetime_format='%Y-%m',
#         xrotation=0
#     )
#
#     axes[0].legend([f'Anomalies ({len(anomalies)})'], loc='upper left',
#                    facecolor='#000000', edgecolor='#404040', labelcolor='#CCCCCC')
#
#     for ax in axes:
#         ax.set_facecolor('#000000')
#         ax.yaxis.set_label_position('left')
#         ax.yaxis.tick_left()
#
#     axes[2].axhline(y=0, color='#808080', linestyle='--', linewidth=0.8)
#
#     symbol = ohlcv_anomalies_df['symbol'].unique()[0]
#     start_date = ohlcv_anomalies_df.index.min().strftime('%Y%m%d')
#     end_date = ohlcv_anomalies_df.index.max().strftime('%Y%m%d')
#     file_name = f"{symbol}_anomaly_{start_date}-{end_date}.png"
#
#     buf = BytesIO()
#     fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#000000')
#     buf.seek(0)
#     content = buf.read()
#     buf.close()
#
#     import matplotlib.pyplot as plt
#     plt.close(fig)
#
#     return {
#         'file_name': file_name,
#         'content': content
#     }