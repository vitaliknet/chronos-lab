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
    plot_to_store = plot_ohlcv_anomalies(ohlcv_anomalies_df=ohlcv_by_symbol_with_features_anomalies,
                                         anomaly_period_filter=anomaly_period_filter,
                                         plot_to_store=True,
                                         to_store_kwargs=plot_to_store_kwargs)
    return {'ohlcv_anomalies_df': ohlcv_by_symbol_with_features_anomalies}


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


def anomalies_collect(
        filter_anomalies: Collect[Dict[str, Any]]) -> Dict[str, Any]:
    response = {}

    anomalies_list = [d['anomalies'] for d in filter_anomalies if d and 'anomalies' in d]
    ohlcv_list = [d['ohlcv_anomalies_df'] for d in filter_anomalies if d and 'ohlcv_anomalies_df' in d]

    _anomalies = pd.concat(anomalies_list, ignore_index=False).sort_index(
        level=['symbol', 'date']) if anomalies_list else pd.DataFrame()
    _ohlcv = pd.concat(ohlcv_list, ignore_index=False).sort_index(
        level=['symbol', 'date']) if ohlcv_list else pd.DataFrame()

    if len(_anomalies) > 0:
        response['filtered_anomalies_df'] = _anomalies

    if len(_ohlcv) > 0:
        response['ohlcv_df'] = _ohlcv

    return response


@config.when(to_dataset="enabled")
def anomalies_to_dataset__enabled(anomalies_collect: Dict[str, Any],
                                  dataset_name: str,
                                  ddb_dataset_ttl: int) -> Dict[str, Any]:
    from chronos_lab.storage import to_dataset

    if isinstance(anomalies_collect.get('filtered_anomalies_df'), pd.DataFrame):
        anomalies_df = anomalies_collect.get('filtered_anomalies_df').copy()

        logger.info(f"Saving anomalies to {dataset_name} dataset")

        anomalies_df['id'] = anomalies_df.index.get_level_values('date').strftime(
            '%Y-%m-%dT%H:%M:%S.000Z') + '#' + anomalies_df.index.get_level_values('symbol')

        if not dataset_name.startswith('ddb_'):
            anomalies_dict = anomalies_df.reset_index().set_index(['id']).to_dict(orient='index')
        else:
            import json
            from decimal import Decimal
            from datetime import datetime, timedelta, timezone

            if ddb_dataset_ttl:
                anomalies_df['ttl'] = int((datetime.now(timezone.utc) + timedelta(days=ddb_dataset_ttl)).timestamp())

            anomalies_dict = json.loads(
                anomalies_df.reset_index().set_index(['id']).to_json(date_format="iso", orient='index'),
                parse_float=Decimal)

        anomalies_collect['to_dataset'] = to_dataset(dataset_name=dataset_name, dataset=anomalies_dict)
    else:
        logger.warning("No anomalies found to save to dataset.")

    return anomalies_collect


@config.when(to_dataset="disabled")
def anomalies_to_dataset__disabled(anomalies_collect: Dict[str, Any]) -> Dict[str, Any]:
    return anomalies_collect


def anomalies_complete(anomalies_to_dataset: Dict[str, Any]) -> Dict[str, Any]:
    return anomalies_to_dataset
