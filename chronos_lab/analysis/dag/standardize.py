import pandas as pd


def standardize_ohlcv_columns(
        source_ohlcv: pd.DataFrame,
        use_adjusted: bool = True
) -> pd.DataFrame:
    columns = set(source_ohlcv.columns)

    has_adj = 'adj_close' in columns
    has_regular = 'close' in columns

    if has_adj and use_adjusted:
        df = source_ohlcv[['adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume']].copy()
        df = df.rename(columns={
            'adj_open': 'open',
            'adj_high': 'high',
            'adj_low': 'low',
            'adj_close': 'close',
            'adj_volume': 'volume'
        })
        adjusted = True
    elif has_regular:
        df = source_ohlcv[['open', 'high', 'low', 'close', 'volume']].copy()
        adjusted = False
    else:
        raise ValueError("DataFrame must contain either OHLCV or adj_OHLCV columns")

    df.attrs['adjusted'] = adjusted

    return df


def validate_ohlcv(standardize_ohlcv_columns: pd.DataFrame) -> pd.DataFrame:
    required_columns = ['open', 'high', 'low', 'close', 'volume']

    missing = set(required_columns) - set(standardize_ohlcv_columns.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if not isinstance(standardize_ohlcv_columns.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be DatetimeIndex")

    critical_columns = ['close', 'volume']
    for col in critical_columns:
        if standardize_ohlcv_columns[col].isna().any():
            raise ValueError(f"Column '{col}' contains NaN values")

    if (standardize_ohlcv_columns['high'] < standardize_ohlcv_columns['low']).any():
        raise ValueError("High prices must be >= low prices")

    if (standardize_ohlcv_columns['volume'] < 0).any():
        raise ValueError("Volume cannot be negative")

    return standardize_ohlcv_columns
