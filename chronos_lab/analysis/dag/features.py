import pandas as pd
import numpy as np
from typing import List, Dict, Any


def ohlcv_features(
        split_ohlcv_by_symbol: pd.DataFrame,
        ohlcv_features_list: List[str]
) -> pd.DataFrame:
    """Compute features from OHLCV data including returns, volume changes, and volatility."""
    df = split_ohlcv_by_symbol.copy()

    if 'returns' in ohlcv_features_list:
        df['returns'] = np.log(df['close'] / df['close'].shift(1))

    if 'volume_change' in ohlcv_features_list:
        df['volume_change'] = df['volume'].pct_change()

    if 'high_low_range' in ohlcv_features_list:
        df['high_low_range'] = (df['high'] - df['low']) / df['close']

    if 'volatility' in ohlcv_features_list:
        returns = np.log(df['close'] / df['close'].shift(1))
        df['volatility'] = returns.rolling(window=20).std()

    return df
