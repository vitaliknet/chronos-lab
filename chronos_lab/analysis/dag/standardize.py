import pandas as pd
from hamilton.htypes import Parallelizable, Collect


def standardize_ohlcv(
        source_ohlcv: pd.DataFrame,
        use_adjusted: bool = True
) -> pd.DataFrame:

    if isinstance(source_ohlcv, pd.DataFrame):
        if source_ohlcv.index.nlevels != 2:
            raise ValueError(f"Expected MultiIndex with 2 levels, got {source_ohlcv.index.nlevels}")

        level_0_name = source_ohlcv.index.names[0]
        level_1_name = source_ohlcv.index.names[1]

        if level_0_name != 'date' or not isinstance(source_ohlcv.index.get_level_values(0), pd.DatetimeIndex):
            raise ValueError(f"Index level 0 must be 'date' of type DatetimeIndex, got '{level_0_name}'")

        if level_1_name not in ['id', 'symbol']:
            raise ValueError(f"Index level 1 must be 'id' or 'symbol', got '{level_1_name}'")

        if level_1_name == 'id':
            source_ohlcv.index = source_ohlcv.index.set_names('symbol', level=1)
    else:
        raise ValueError("source_ohlcv must be a pandas DataFrame")

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


def validate_ohlcv(standardize_ohlcv: pd.DataFrame) -> pd.DataFrame:
    required_columns = ['open', 'high', 'low', 'close', 'volume']

    missing = set(required_columns) - set(standardize_ohlcv.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    critical_columns = ['close', 'volume']
    for col in critical_columns:
        if standardize_ohlcv[col].isna().any():
            raise ValueError(f"Column '{col}' contains NaN values")

    if (standardize_ohlcv['high'] < standardize_ohlcv['low']).any():
        raise ValueError("High prices must be >= low prices")

    if (standardize_ohlcv['volume'] < 0).any():
        raise ValueError("Volume cannot be negative")

    return standardize_ohlcv


def split_ohlcv_by_symbol(validate_ohlcv: pd.DataFrame) -> Parallelizable[pd.DataFrame]:
    symbols = validate_ohlcv.index.get_level_values(1).unique()

    for symbol in symbols:
        symbol_df = validate_ohlcv.xs(symbol, level=1, drop_level=False).copy()

        yield symbol_df
