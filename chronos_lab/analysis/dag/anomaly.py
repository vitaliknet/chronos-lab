import pandas as pd
from typing import Dict, Any, List, Optional
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
def plot_anomalies__enabled(
        ohlcv_by_symbol_with_features_anomalies: pd.DataFrame,
        anomaly_period_filter: Optional[str] = None,
        plot_to_store_kwargs: Optional[dict] = None
) -> Dict[str, Any]:
    from chronos_lab.plot import plot_ohlcv_anomalies

    return {'ohlcv_anomalies_df': ohlcv_by_symbol_with_features_anomalies,
            'plot_to_store': plot_ohlcv_anomalies(ohlcv_anomalies_df=ohlcv_by_symbol_with_features_anomalies,
                                                  anomaly_period_filter=anomaly_period_filter,
                                                  plot_to_store=True,
                                                  to_store_kwargs=plot_to_store_kwargs)}


@config.when(generate_plots="disabled")
def plot_anomalies__disabled(
        ohlcv_by_symbol_with_features_anomalies: pd.DataFrame,
) -> Dict[str, Any]:
    return {'ohlcv_anomalies_df': ohlcv_by_symbol_with_features_anomalies}


def filter_anomalies(plot_anomalies: Dict[str, Any],
                     anomaly_period_filter: Optional[str] = None,
                     return_ohlcv_df: Optional[bool] = False
                     ) -> Dict[str, Any]:
    ohlcv_anomalies_df = plot_anomalies['ohlcv_anomalies_df'].copy()
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
        plot_anomalies['anomalies'] = anomalies

    if return_ohlcv_df:
        return plot_anomalies
    else:
        del plot_anomalies['ohlcv_anomalies_df']
        return plot_anomalies


def anomalies_complete(
        filter_anomalies: Collect[Dict[str, Any]]) -> Dict[str, Any]:

    response = {}

    anomalies_list = [d['anomalies'] for d in filter_anomalies if d and 'anomalies' in d]
    ohlcv_list = [d['ohlcv_anomalies_df'] for d in filter_anomalies if d and 'ohlcv_anomalies_df' in d]

    _anomalies = pd.concat(anomalies_list, ignore_index=False).sort_index(level=['symbol', 'date']) if anomalies_list else pd.DataFrame()
    _ohlcv = pd.concat(ohlcv_list, ignore_index=False).sort_index(level=['symbol', 'date']) if ohlcv_list else pd.DataFrame()

    if len(_anomalies) > 0:
        response['filtered_anomalies_df'] = _anomalies

    if len(_ohlcv) > 0:
        response['ohlcv_df'] = _ohlcv

    return response
