import pandas as pd
from typing import Dict, Any


def detect_ohlcv_features_anomalies(
        ohlcv_features: pd.DataFrame,
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

    X = ohlcv_features.values
    model.fit(X)

    anomaly_score = -model.score_samples(X)
    is_anomaly = model.predict(X) == -1

    result_df = pd.DataFrame(index=ohlcv_features.index)
    result_df['anomaly_score'] = anomaly_score
    result_df['is_anomaly'] = is_anomaly

    ranks = anomaly_score.argsort().argsort() + 1
    result_df['anomaly_rank'] = ranks

    return result_df


def ohlcv_with_features_anomalies(
        validate_ohlcv: pd.DataFrame,
        detect_ohlcv_features_anomalies: pd.DataFrame
) -> pd.DataFrame:
    result = validate_ohlcv.join(detect_ohlcv_features_anomalies, how='left')

    result['anomaly_score'] = result['anomaly_score'].fillna(0.0)
    result['is_anomaly'] = result['is_anomaly'].astype('boolean').fillna(False)
    result['anomaly_rank'] = result['anomaly_rank'].fillna(0).astype(int)

    return result
