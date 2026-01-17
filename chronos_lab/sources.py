from chronos_lab import logger
from chronos_lab.settings import get_settings
from typing import List, Optional, Dict, Union, Literal
from datetime import datetime
import pandas as pd


def securities_from_intrinio(
        *,
        api_key: Optional[str] = None,
        composite_mic: str = 'USCOMP',
        codes: List[Literal[
            'EQS', 'ETF', 'DR', 'PRF', 'WAR', 'RTS', 'UNT', 'CEF', 'ETN', 'ETC'
        ]] = ['EQS'],
) -> pd.DataFrame | None:
    """
    Retrieve securities list from Intrinio API.

    Args:
        api_key: Intrinio API key. If None, uses default from Intrinio class.
        composite_mic: Composite MIC code for the exchange. Defaults to 'USCOMP'.
        codes: List of security type codes. Common codes include:
            - 'EQS': Equity Shares (common stocks)
            - 'ETF': Exchange Traded Funds
            - 'DR': Depository Receipts (ADRs, GDRs, etc.)
            - 'PRF': Preference Shares (preferred stock)
            - 'WAR': Warrants
            - 'RTS': Rights
            - 'UNT': Units
            - 'CEF': Closed-Ended Funds
            - 'ETN': Exchange Traded Notes
            - 'ETC': Exchange Traded Commodities
            Defaults to ['EQS'] if None.

    Returns:
        DataFrame with securities indexed by 'id', or None on error.
    """
    from chronos_lab.intrinio import Intrinio

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


def ohlcv_from_intrinio(
        *,
        symbols: List[str],
        period: Optional[str] = None,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
        interval: Optional[Literal[
            '1m', '5m', '10m', '15m', '30m', '60m', '1h',
            'daily', 'weekly', 'monthly', 'quarterly', 'yearly']] = 'daily',
        api_key: Optional[str] = None,
        output_dict: Optional[bool] = False,
        **kwargs
) -> Dict[str, pd.DataFrame] | pd.DataFrame | None:
    """
    Download OHLCV data from Intrinio API.

    Args:
        symbols: List of security identifiers to download.
        period: Period to download (e.g., 1d, 5d, 1mo, 1y, max).
            Use either period or start_date.
        start_date: Start date (YYYY-MM-DD or datetime), inclusive.
        end_date: End date (YYYY-MM-DD or datetime), exclusive.
        interval: Data interval. Intraday: 1m, 5m, 10m, 15m, 30m, 60m, 1h.
            Non-intraday: daily, weekly, monthly, quarterly, yearly. Defaults to 'daily'.
        api_key: Intrinio API key. If None, read from .env file.
        output_dict: If True, return dict of DataFrames by symbol.
            If False, return MultiIndex DataFrame with ('date', 'id') levels.
        **kwargs: Additional arguments passed to Intrinio API.

    Returns:
        Dict of DataFrames (if output_dict=True), MultiIndex DataFrame (if False),
        or None on error.
    """
    from chronos_lab.intrinio import Intrinio

    if interval in ['1m', '5m', '10m', '15m', '30m', '60m', '1h']:
        kwargs['interval_size'] = interval
        interval = True
    else:
        kwargs['frequency'] = interval
        interval = False

    if period:
        start_date, end_date = _period(period)

    intr = Intrinio(api_key=api_key)

    secs_prices_dict = {}
    cols_interval = ['id', 'date', 'open', 'high', 'low', 'close', 'volume', 'interval']

    sec_count = len(symbols)
    i = 0
    for id in symbols:
        logger.info('Processing item %s (%s/%s)', id, i, sec_count)

        sec_prices = intr.get_security_stock_prices(page_size=100,
                                                    identifier=id,
                                                    start_date=start_date,
                                                    end_date=end_date,
                                                    output_df=False,
                                                    interval=interval,
                                                    **kwargs
                                                    )
        if sec_prices['statusCode'] == -1:
            logger.warning('Failed to request prices for item %s.', id)
            continue

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
        else:
            logger.warning('Failed to request prices for item %s.', id)
        i += 1

    if len(secs_prices_dict) == 0:
        logger.error('No data retrieved for any symbols')
        return None

    if output_dict:
        return secs_prices_dict
    else:
        return pd.concat(secs_prices_dict.values()).set_index(['id'], append=True)


def ohlcv_from_yfinance(
        *,
        symbols: List[str],
        period: Optional[str] = None,
        start_date: Optional[str | datetime] = None,
        end_date: Optional[str | datetime] = None,
        interval: Optional[str] = '1d',
        output_dict: Optional[bool] = False,
        **kwargs
) -> Dict[str, pd.DataFrame] | pd.DataFrame | None:
    """
    Download OHLCV data from Yahoo Finance.

    Args:
        symbols: List of symbols to download (max 100).
        period: Period to download (e.g., 1d, 5d, 1mo, 1y, max).
            Use either period or start_date.
        start_date: Start date (YYYY-MM-DD or datetime), inclusive.
        end_date: End date (YYYY-MM-DD or datetime), exclusive.
        interval: Data interval (e.g., 1m, 1h, 1d, 1wk). Defaults to '1d'.
        output_dict: If True, return dict of DataFrames by symbol.
            If False, return MultiIndex DataFrame with ('date', 'symbol') levels.
        **kwargs: Additional arguments passed to yfinance.download().

    Returns:
        Dict of DataFrames (if output_dict=True), MultiIndex DataFrame (if False),
        or None on error.
    """
    import yfinance as yf

    response = {
        'statusCode': 0,
    }

    if period is None and start_date is None:
        logger.error("Either start_date or period must be specified")
        response['statusCode'] = -1
        return None

    intraday_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h']
    intraday = interval in intraday_intervals

    if len(symbols) > 100:
        logger.error('symbols size exceeds 100 symbols. Please limit to 100 symbols per invocation.')
        response['statusCode'] = -1
        return None

    secs_prices_dict = {}
    sec_count = len(symbols)

    logger.info('Downloading data for %s symbols from Yahoo Finance', sec_count)

    try:
        yf_df = yf.download(
            tickers=symbols,
            period=period,
            start=start_date,
            end=end_date,
            interval=interval,
            group_by='ticker',
            threads=True,
            **kwargs
        )

        if yf_df.empty:
            logger.error('No data returned from Yahoo Finance')
            response['statusCode'] = -1
            return None

        for symbol in symbols:
            try:
                symbol_df = yf_df[symbol].copy()

                if symbol_df.empty or symbol_df.dropna(how='all').empty:
                    logger.warning('No data returned for symbol %s', symbol)
                    continue

                symbol_df = symbol_df.reset_index()

                column_mapping = {
                    'Date': 'date',
                    'Datetime': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume',
                }
                symbol_df = symbol_df.rename(columns=column_mapping)
                symbol_df['symbol'] = symbol

                if 'date' in symbol_df.columns:
                    symbol_df['date'] = pd.to_datetime(symbol_df['date'], utc=True)

                if intraday:
                    symbol_df['interval'] = interval
                    cols_to_keep = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'interval']
                else:
                    cols_to_keep = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']

                symbol_df = symbol_df[cols_to_keep].dropna(subset=['close'])
                symbol_df.columns.name = None

                if len(symbol_df) == 0:
                    logger.warning('No valid data for symbol %s after filtering', symbol)
                    continue

                symbol_df = symbol_df.set_index('date').sort_index()
                secs_prices_dict[symbol] = symbol_df

                logger.info('Successfully retrieved %s rows for %s', len(symbol_df), symbol)

            except Exception as e:
                logger.error('Failed to process data for symbol %s: %s', symbol, str(e))
                continue

    except Exception as e:
        logger.error('Failed to download data from Yahoo Finance: %s', str(e))
        response['statusCode'] = -1
        return None

    if len(secs_prices_dict) == 0:
        logger.error('No data retrieved for any symbols')
        response['statusCode'] = -1
        return None

    if output_dict:
        return secs_prices_dict
    else:
        return pd.concat(secs_prices_dict.values()).set_index(['symbol'], append=True)


def ohlcv_from_arcticdb(
        symbols: List[str],
        start_date: Optional[Union[str, pd.Timestamp]] = None,
        end_date: Optional[Union[str, pd.Timestamp]] = None,
        period: Optional[str] = None,
        columns: Optional[List[str]] = None,
        library_name: Optional[str] = None,
        pivot: bool = False,
        group_by: Optional[Literal["column", "symbol"]] = "column",
) -> Optional[pd.DataFrame]:
    """
    Retrieve historical or intraday stock price data from ArcticDB.

    Returns DataFrame with MultiIndex (date, symbol) by default, or wide format
    with pivoted symbols when pivot=True.

    Args:
        symbols: List of ticker symbols (e.g., ['AAPL', 'MSFT'])
        start_date: Start date (ISO string or pd.Timestamp). Mutually exclusive with period.
        end_date: End date (ISO string or pd.Timestamp). Defaults to current UTC time if not specified.
        period: Relative period ('5d', '3m', '1y'). Mutually exclusive with start_date/end_date.
        columns: Columns to retrieve. 'symbol' column is always included automatically.
        library_name: ArcticDB library name (default from ~/.chronos_lab/.env)
        pivot: If True, reshape to wide format with symbols unstacked
        group_by: When pivoting, controls column MultiIndex order:
                  'column' (default) - (column, symbol) ordering
                  'symbol' - (symbol, column) ordering

    Returns:
        DataFrame with MultiIndex (date, symbol) if pivot=False, or
        DataFrame with DatetimeIndex and MultiIndex columns if pivot=True.
        Returns None if no data found or if both period and start_date/end_date are specified.
    """

    from chronos_lab.arcdb import ArcDB

    if library_name is None:
        settings = get_settings()
        library_name = settings.arcticdb_default_library_name

    current_time = pd.Timestamp.now(tz='UTC')

    if period and (start_date or end_date):
        logger.error("Cannot specify both 'period' and 'start_date'/'end_date'. Use one or the other.")
        return None

    read_kwargs = {}

    if period:
        read_kwargs['date_range'] = _period(period=period)
    elif start_date or end_date:
        start_dt = None
        if start_date:
            start_dt = pd.to_datetime(start_date, utc=True) if isinstance(start_date, str) else start_date

        end_dt = current_time
        if end_date:
            end_dt = pd.to_datetime(end_date, utc=True) if isinstance(end_date, str) else end_date

        if start_dt:
            read_kwargs['date_range'] = (start_dt, end_dt)
        else:
            read_kwargs['date_range'] = (None, end_dt)

    if columns is not None:
        read_kwargs['columns'] = list(set(columns) | {'symbol'})

    ac = ArcDB(library_name=library_name)

    result = ac.batch_read(symbol_list=symbols, **read_kwargs)

    if result['statusCode'] == 0:
        result_df = result['payload'].sort_values(['symbol', 'date']).set_index('symbol', append=True)

        if pivot:
            result_df_output = result_df.unstack('symbol')
            if group_by == 'symbol':
                result_df_output.columns = result_df_output.columns.swaplevel(0, 1)
            result_df_output = result_df_output.sort_index(axis=1)

        else:
            result_df_output = result_df

        return result_df_output
    else:
        logger.warning(f"No data found for symbols {symbols} in date range {start_date} to {end_date}.")
        return None


def _period(period: str, as_of: Optional[pd.Timestamp] = None) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Convert period string to date range tuple.

    Args:
        period: Period string (e.g., '7d', '4w', '3mo', '1y')
        as_of: Reference timestamp (defaults to current UTC time)

    Returns:
        Tuple of (start_datetime, end_datetime)

    Raises:
        ValueError: If period unit is invalid
    """
    end_dt = as_of if as_of is not None else pd.Timestamp.now(tz='UTC')

    value = int(period[:-1]) if period[-1].isalpha() else int(period[:-2])
    unit = period[-1] if period[-1].isalpha() else period[-2:]

    offset_map = {
        'd': pd.DateOffset(days=value),
        'w': pd.DateOffset(weeks=value),
        'mo': pd.DateOffset(months=value),
        'm': pd.DateOffset(months=value),
        'y': pd.DateOffset(years=value)
    }

    if unit not in offset_map:
        raise ValueError(f"Invalid period unit: {unit}. Use 'd', 'w', 'mo'/'m', or 'y'")

    start_dt = end_dt - offset_map[unit]
    return (start_dt, end_dt)


__all__ = [
    'ohlcv_from_intrinio',
    'ohlcv_from_yfinance',
    'ohlcv_from_arcticdb',
    'securities_from_intrinio'
]
