from chronos_lab import logger
from chronos_lab.settings import get_settings
from typing import Optional, Dict
import pandas as pd


def ohlcv_to_arcticdb(
        *,
        ohlcv: pd.DataFrame | Dict[str, pd.DataFrame],
        library_name: Optional[str] = None,
        adb_mode: str = 'write'
) -> Dict[str, int]:
    """
    Store OHLCV data to ArcticDB library.

    Accepts either a MultiIndex DataFrame with ('date', 'id'/'symbol') levels
    or a dictionary of DataFrames keyed by symbol/id. Splits MultiIndex DataFrames
    by symbol before storage.

    Args:
        ohlcv: DataFrame with 2-level MultiIndex or dict of DataFrames by symbol
        library_name: ArcticDB library name (default from ~/.chronos_lab/.env)
        adb_mode: Storage mode for ArcticDB (default: 'write')

    Returns:
        Dict with 'statusCode': 0 on success, -1 on failure
    """
    from chronos_lab.arcdb import ArcDB

    response = {
        'statusCode': 0,
    }

    if library_name is None:
        settings = get_settings()
        library_name = settings.arcticdb_default_library_name

    if isinstance(ohlcv, pd.DataFrame):
        if ohlcv.index.nlevels != 2:
            logger.error(f"Expected MultiIndex with 2 levels, got {ohlcv.index.nlevels}")
            response['statusCode'] = -1
            return response

        level_0_name = ohlcv.index.names[0]
        level_1_name = ohlcv.index.names[1]

        if level_0_name != 'date' or level_1_name not in ['id', 'symbol']:
            logger.error(
                f"Index levels are ('{level_0_name}', '{level_1_name}'), expected ('date', 'id') or ('date', 'symbol')")
            response['statusCode'] = -1
            return response

        ohlcv_dict = dict(tuple(ohlcv.reset_index(level=1).groupby(level_1_name)))
    else:
        ohlcv_dict = ohlcv

    try:
        ac = ArcDB(library_name=library_name)
        ac_res = ac.batch_store(data_dict=ohlcv_dict, mode=adb_mode, prune_previous_versions=True)

        if ac_res['statusCode'] == 0:
            logger.info("Successfully stored prices for %s symbols in ArcticDB", len(ohlcv_dict))
        else:
            logger.error("Failed to store data in ArcticDB")
            response['statusCode'] = -1
    except Exception as e:
        logger.error("Exception while storing in ArcticDB: %s", str(e))
        response['statusCode'] = -1

    return response


__all__ = [
    'ohlcv_to_arcticdb'
]
