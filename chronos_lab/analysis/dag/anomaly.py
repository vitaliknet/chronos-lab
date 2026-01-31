import pandas as pd
from typing import Dict, Any, List, Optional
from chronos_lab import logger
from hamilton.htypes import Collect
from hamilton.function_modifiers import config


def detect_ohlcv_features_anomalies(
        ohlcv_features: pd.DataFrame,
        ohlcv_features_list: List[str],
        isolation_forest_config: Dict[str, Any] = None
) -> pd.DataFrame:
    """Run Isolation Forest on computed features to detect anomalies and assign scores and ranks."""
    from sklearn.ensemble import IsolationForest

    model = IsolationForest(**isolation_forest_config)
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
    """Join anomaly detection results with original OHLCV features DataFrame."""
    result = ohlcv_features.join(detect_ohlcv_features_anomalies, how='left')

    result['anomaly_score'] = result['anomaly_score'].fillna(0.0)
    result['is_anomaly'] = result['is_anomaly'].astype(bool).fillna(False)
    result['anomaly_rank'] = result['anomaly_rank'].fillna(0).astype(int)

    return result


def analysis_result(
        ohlcv_by_symbol_with_features_anomalies: Collect[pd.DataFrame]) -> pd.DataFrame:
    """Collect and concatenate anomaly results from parallel symbol processing into unified DataFrames."""

    return pd.concat(ohlcv_by_symbol_with_features_anomalies)


@config.when(to_dataset="enabled")
def analysis_result_dataset__enabled(analysis_result: pd.DataFrame
                                     ) -> pd.DataFrame:
    anomalies_df = analysis_result[analysis_result['is_anomaly']].copy()

    if len(anomalies_df) > 0:
        anomalies_df['id'] = anomalies_df.index.get_level_values('date').strftime(
            '%Y-%m-%dT%H:%M:%S.000Z') + '#' + anomalies_df.index.get_level_values('symbol')
        return anomalies_df
    else:
        return pd.DataFrame()


@config.when(to_dataset="disabled")
def analysis_result_dataset__disabled(analysis_result: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame()


@config.when(to_arcticdb="enabled")
def analysis_result_arcticdb__enabled(analysis_result: pd.DataFrame,
                                      to_arcticdb_config: Dict[str, Any]
                                      ) -> Dict[str, Any]:
    level_1_name = analysis_result.index.names[1]
    analysis_result_dict = dict(tuple(analysis_result.reset_index(level=1).groupby(level_1_name)))

    symbol_prefix = to_arcticdb_config.get('symbol_prefix', '')
    symbol_suffix = to_arcticdb_config.get('symbol_suffix', '')

    return {f"{symbol_prefix}{k}{symbol_suffix}": v for k, v in analysis_result_dict.items()}


@config.when(to_arcticdb="disabled")
def analysis_result_arcticdb__disabled(analysis_result: pd.DataFrame) -> Dict[str, Any]:
    return {}
