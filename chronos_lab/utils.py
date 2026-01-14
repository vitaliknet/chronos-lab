from chronos_lab import logger
from typing import List, Optional
import pandas as pd


def get_seclist_intrinio(*,
                         api_key=None,
                         composite_mic='USCOMP',
                         codes=None,
                         ):
    from chronos_lab.intrinio import Intrinio

    if codes is None:
        codes = ['EQS']

    intr = Intrinio(api_key=api_key)

    securitiesList = []

    for code in codes:
        intr_ret = intr.get_all_securities(active=True, delisted=False, code=code, composite_mic=composite_mic,
                                           include_non_figi=False,
                                           page_size=100, primary_listing=True)
        if intr_ret['statusCode'] == 0 and len(intr_ret['payload']) > 0:
            securitiesList += intr_ret['payload']
        else:
            logger.error('Failed to retrieve security list for code %s', code)
            return None

    securities = pd.DataFrame(securitiesList).set_index(['id'])
    securities.rename(columns={'figi': 'sec_figi'}, inplace=True)

    return securities


def save_ohlcv_intrinio(*,
                        sec_list,
                        start_date,
                        api_key=None,
                        interval=False,
                        adb_mode='write',
                        **kwargs
                        ):
    from chronos_lab.intrinio import Intrinio
    from chronos_lab.arcdb import ArcDB

    response = {
        'statusCode': 0,
    }

    intr = Intrinio(api_key=api_key)
    secs_prices = []
    secs_prices_dict = {}
    cols_interval = ['id', 'date', 'open', 'high', 'low', 'close', 'volume', 'interval']

    sec_count = len(sec_list)
    i = 0
    for id in sec_list:
        logger.info('Processing item %s (%s/%s)', id, i, sec_count)
        sec_prices = intr.get_security_stock_prices(page_size=100,
                                                    identifier=id,
                                                    start_date=start_date,
                                                    output_df=False,
                                                    interval=interval,
                                                    **kwargs
                                                    )
        sp_df = pd.DataFrame(sec_prices['stockPrices'])
        if len(sp_df) != 0:
            sp_df['id'] = id
            if interval:
                sp_df['date'] = pd.to_datetime(sp_df['close_time'], errors='coerce', utc=True)
                sp_df = sp_df[cols_interval].dropna(subset=['close'])

                if len(sp_df) == 0:
                    logger.warning('No data for item %s in the interval', id)
                    continue
            else:
                sp_df['date'] = pd.to_datetime(sp_df['date'], errors='coerce', utc=True)

            if sec_prices['security']['ticker'] != id:
                sp_df['symbol'] = sec_prices['security']['ticker']
                secs_prices_dict[sec_prices['security']['ticker']] = sp_df.set_index('date').sort_index()

            secs_prices.append(sp_df.set_index(['id', 'date']))
        else:
            logger.warning('Failed to request prices for item %s.', id)
        i += 1

    ac = ArcDB(library_name='uscomp_id' if interval else 'uscomp')
    ac_res = ac.batch_store(data_dict=secs_prices_dict, mode=adb_mode, prune_previous_versions=True)

    if ac_res['statusCode'] == 0:
        logger.info("Successfully stored prices in ArcticDB")
    else:
        logger.error("Failed to store snapshot in ArcticDB")
        response['statusCode'] = -1

    return response


def get_ohlcv(
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: Optional[str] = None,
        columns: Optional[List[str]] = None,
        library_name: Optional[str] = "uscomp",
        pivot: bool = False,
) -> str:
    """
    Retrieve historical or intraday stock price time series data from ArcticDB.

    The function returns a pandas DataFrame whose structure depends on the
    `library_name`, `columns`, and `pivot` arguments.

    By default, the data is returned in long format with a MultiIndex
    (date, symbol). If `pivot=True` and exactly one data column is present,
    the data is returned in wide format with one column per symbol.

    Args:
        symbols:
            List of stock ticker symbols (e.g., ['AAPL', 'MSFT']).

        start_date:
            Optional ISO date string (e.g., '2024-01-01').
            Required if `period` is not specified.

        end_date:
            Optional ISO date string.
            Defaults to current UTC time.

        period:
            Optional relative period string such as '5d', '3m', or '1y'.
            If provided, `start_date` is derived relative to current UTC time.

        columns:
            Optional list of columns to retrieve.
            If omitted, defaults to:
                - ['adj_close'] for library_name='uscomp'
                - ['close'] for library_name='uscomp_id'

        library_name:
            Data source to query:
                - 'uscomp'    : daily U.S. equity data
                - 'uscomp_id' : intraday 5-minute data

        pivot:
            If True and exactly one column is returned, reshapes the output so
            each symbol becomes a separate column named '<column>_<symbol>'.

    Returns:
        pandas.DataFrame or str:
            - A DataFrame indexed by (date, symbol) when pivot=False
            - A DataFrame indexed by DatetimeIndex with one column per symbol
              when pivot=True and exactly one column is present
            - A human-readable error message string if retrieval fails
    """

    from chronos_lab.arcdb import ArcDB

    if period is None and start_date is None:
        return "Either start_date or period must be specified"

    current_time = pd.Timestamp.now(tz='UTC')

    if period:
        end_dt = current_time
        value = int(period[:-1])
        unit = period[-1]
        if unit == 'd':
            start_dt = current_time - pd.DateOffset(days=value)
        elif unit == 'w':
            start_dt = current_time - pd.DateOffset(weeks=value)
        elif unit == 'm':
            start_dt = current_time - pd.DateOffset(months=value)
        elif unit == 'y':
            start_dt = current_time - pd.DateOffset(years=value)
        else:
            return f"Invalid period unit: {unit}. Use 'd', 'w', 'm', or 'y'"
    else:
        start_dt = pd.to_datetime(start_date, utc=True)
        end_dt = pd.to_datetime(end_date, utc=True) if end_date else current_time

    ac = ArcDB(library_name=library_name)

    read_kwargs = {
        'date_range': (start_dt, end_dt)
    }
    if columns is not None:
        read_kwargs['columns'] = list(set(columns) | {'symbol'})
    elif library_name == 'uscomp':
        read_kwargs['columns'] = ["symbol", "adj_close"]
    elif library_name == 'uscomp_id':
        read_kwargs['columns'] = ["symbol", "close"]

    result = ac.batch_read(symbol_list=symbols, **read_kwargs)

    if result['statusCode'] == 0:
        result_df = result['payload'].sort_values(['symbol', 'date']).set_index('symbol', append=True)

        if pivot and len(result_df.columns) == 1:
            result_df_output = result_df.unstack('symbol')
            result_df_output.columns = ['_'.join(col).strip() for col in result_df_output.columns.values]
        else:
            result_df_output = result_df

        return result_df_output
    else:
        return f"No data found for symbols {symbols} in date range {start_date} to {end_date}."
