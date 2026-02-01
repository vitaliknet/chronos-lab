import pandas as pd
from typing import Dict, Any, List, Optional
from chronos_lab import logger
from hamilton.function_modifiers import config


@config.when(ohlcv_from_source="yfinance")
def ohlcv_from__yfinance(ohlcv_from_config: Dict[str, Any]) -> pd.DataFrame:
    from chronos_lab.sources import ohlcv_from_yfinance
    return ohlcv_from_yfinance(**ohlcv_from_config)


@config.when(ohlcv_from_source="intrinio")
def ohlcv_from__intrinio(ohlcv_from_config: Dict[str, Any]) -> pd.DataFrame:
    from chronos_lab.sources import ohlcv_from_intrinio
    return ohlcv_from_intrinio(**ohlcv_from_config)


@config.when(ohlcv_from_source="arcticdb")
def ohlcv_from__arcticdb(ohlcv_from_config: Dict[str, Any]) -> pd.DataFrame:
    from chronos_lab.sources import ohlcv_from_arcticdb
    return ohlcv_from_arcticdb(**ohlcv_from_config)


@config.when(ohlcv_from_source="disabled")
def ohlcv_from__disabled(source_ohlcv: pd.DataFrame) -> pd.DataFrame:
    return source_ohlcv


@config.when(to_dataset="enabled")
def analysis_to_dataset__enabled(analysis_result_dataset: pd.DataFrame,
                                 to_dataset_config: Dict[str, Any]
                                 ) -> Dict[str, Any]:
    from chronos_lab.storage import to_dataset

    dataset_name = to_dataset_config['dataset_name']
    ddb_dataset_ttl = to_dataset_config.get('ddb_dataset_ttl', None)

    if len(analysis_result_dataset) > 0:
        logger.info(f"Saving {dataset_name} dataset")

        if not dataset_name.startswith('ddb_'):
            dataset_dict = analysis_result_dataset.reset_index().set_index(['id']).to_dict(orient='index')
        else:
            import json
            from decimal import Decimal
            from datetime import datetime, timedelta, timezone

            if ddb_dataset_ttl:
                analysis_result_dataset['ttl'] = int(
                    (datetime.now(timezone.utc) + timedelta(days=ddb_dataset_ttl)).timestamp())

            dataset_dict = json.loads(
                analysis_result_dataset.reset_index().set_index(['id']).to_json(date_format="iso", orient='index'),
                parse_float=Decimal)

        _to_dataset_res = to_dataset(dataset_name=dataset_name, dataset=dataset_dict)

        if _to_dataset_res['statusCode'] == 0:
            logger.info(f"Dataset {dataset_name} saved successfully.")
            return _to_dataset_res
        else:
            logger.warning(f"Failed to save dataset {dataset_name} .")
    else:
        logger.warning("No data found to save to dataset.")

    return {}


@config.when(to_dataset="disabled")
def analysis_to_dataset__disabled(analysis_result_dataset: Optional[pd.DataFrame]) -> Dict[str, Any]:
    return {}


@config.when(to_arcticdb="enabled")
def analysis_to_arcticdb__enabled(analysis_result_arcticdb: Dict[str, Any],
                                  to_arcticdb_config: Dict[str, Any]
                                  ) -> Dict[str, Any]:
    if len(analysis_result_arcticdb) > 0:
        from chronos_lab.storage import ohlcv_to_arcticdb
        ohlcv_to_arcticdb_kwargs = {k: v for k, v in to_arcticdb_config.items() if
                                    k not in ['symbol_prefix', 'symbol_suffix']}
        logger.info(f"Saving analysis results to ArcticDB, configuration: {to_arcticdb_config}")

        _to_arcticdb_res = ohlcv_to_arcticdb(ohlcv=analysis_result_arcticdb, **ohlcv_to_arcticdb_kwargs)

        if _to_arcticdb_res['statusCode'] == 0:
            logger.info(f"Analysis results saved successfully to ArcticDB.")
            return _to_arcticdb_res
        else:
            logger.warning(f"Failed to save analysis results to ArcticDB.")
    else:
        logger.warning("No data found to save to ArcticDB.")

    return {}


@config.when(to_arcticdb="disabled")
def analysis_to_arcticdb__disabled(analysis_result_arcticdb: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {}
