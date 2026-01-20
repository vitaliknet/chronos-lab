import pandas as pd
import numpy as np
from typing import List


def ohlcv_features(
        split_ohlcv_by_symbol: pd.DataFrame,
        ohlcv_features_list: List[str]
) -> pd.DataFrame:
    df = split_ohlcv_by_symbol.copy()
    feature_df = pd.DataFrame(index=df.index)

    if 'returns' in ohlcv_features_list:
        feature_df['returns'] = np.log(df['close'] / df['close'].shift(1))

    if 'volume_change' in ohlcv_features_list:
        feature_df['volume_change'] = df['volume'].pct_change()

    if 'high_low_range' in ohlcv_features_list:
        feature_df['high_low_range'] = (df['high'] - df['low']) / df['close']

    if 'volatility' in ohlcv_features_list:
        returns = np.log(df['close'] / df['close'].shift(1))
        feature_df['volatility'] = returns.rolling(window=20).std()

    feature_df = feature_df.dropna()

    return feature_df
