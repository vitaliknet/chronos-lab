"""Data source connectors for fetching and retrieving OHLCV time series data.

This module provides unified interfaces for fetching OHLCV (Open, High, Low, Close, Volume)
data from multiple sources including Yahoo Finance, Intrinio API, and ArcticDB storage.
All functions return data in consistent formats with UTC timezone-aware timestamps.

Typical Usage:
    Fetch from Yahoo Finance and store in ArcticDB:

        >>> from chronos_lab.sources import ohlcv_from_yfinance
        >>> from chronos_lab.storage import ohlcv_to_arcticdb
        >>>
        >>> # Fetch data
        >>> prices = ohlcv_from_yfinance(
        ...     symbols=['AAPL', 'MSFT'],
        ...     period='1y'
        ... )
        >>>
        >>> # Store for later retrieval
        >>> ohlcv_to_arcticdb(ohlcv=prices, library_name='yfinance')

    Retrieve stored data with date filtering:

        >>> from chronos_lab.sources import ohlcv_from_arcticdb
        >>>
        >>> prices = ohlcv_from_arcticdb(
        ...     symbols=['AAPL', 'MSFT'],
        ...     period='3m',
        ...     library_name='yfinance'
        ... )
"""

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
    """Download OHLCV data from Intrinio API.

    Fetches institutional-quality historical or intraday price data using the Intrinio
    financial data platform. Requires an active Intrinio API subscription. Data is returned
    with UTC timezone-aware timestamps in a consistent format.

    Args:
        symbols: List of security identifiers (ticker symbols, CUSIPs, or Intrinio IDs).
        period: Relative time period (e.g., '1d', '5d', '1mo', '1y').
            Mutually exclusive with start_date/end_date.
        start_date: Start date as 'YYYY-MM-DD' string or datetime object (inclusive).
            Mutually exclusive with period.
        end_date: End date as 'YYYY-MM-DD' string or datetime object (exclusive).
            Defaults to current time if start_date is provided without end_date.
        interval: Data frequency interval. Options:
            - Intraday: '1m', '5m', '10m', '15m', '30m', '60m', '1h'
            - Historical: 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
            Defaults to 'daily'.
        api_key: Intrinio API key. If None, reads from INTRINIO_API_KEY in
            ~/.chronos_lab/.env configuration file.
        output_dict: If True, returns dict mapping symbols to DataFrames.
            If False, returns single MultiIndex DataFrame with (date, id) levels.
            Defaults to False.
        **kwargs: Additional keyword arguments passed to Intrinio SDK
            (e.g., frequency, sort_order).

    Returns:
        If output_dict=True: Dictionary mapping ticker symbols to DataFrames, where each
            DataFrame has DatetimeIndex and columns ['id', 'open', 'high', 'low', 'close',
            'volume', 'interval' (intraday only), 'symbol' (if ticker differs from id)].
        If output_dict=False: Single DataFrame with MultiIndex (date, id) and same columns.
        Returns None if no data could be retrieved or on error.

    Raises:
        None: Errors are logged but not raised. Check return value for None.

    Examples:
        Fetch daily data with API key:
            >>> prices = ohlcv_from_intrinio(
            ...     symbols=['AAPL', 'MSFT'],
            ...     start_date='2024-01-01',
            ...     interval='daily',
            ...     api_key='your_api_key_here'
            ... )
            >>> # Returns MultiIndex DataFrame with (date, id) levels

        Fetch data using configuration file:
            >>> # First set INTRINIO_API_KEY in ~/.chronos_lab/.env
            >>> prices = ohlcv_from_intrinio(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='1y',
            ...     interval='daily'
            ... )

        Get intraday 5-minute bars:
            >>> intraday = ohlcv_from_intrinio(
            ...     symbols=['SPY'],
            ...     start_date='2024-01-15',
            ...     end_date='2024-01-16',
            ...     interval='5m'
            ... )
            >>> # Includes 'interval' column for intraday data

        Get data as dictionary by symbol:
            >>> prices_dict = ohlcv_from_intrinio(
            ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
            ...     period='3mo',
            ...     interval='daily',
            ...     output_dict=True
            ... )
            >>> aapl_df = prices_dict['AAPL']

    Note:
        - Requires active Intrinio subscription with appropriate data access
        - API rate limits apply based on subscription tier
        - Intraday data availability depends on subscription level
        - All timestamps are converted to UTC timezone
        - Symbol identifiers can be tickers, CUSIPs, or Intrinio composite IDs
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
    """Download OHLCV data from Yahoo Finance.

    Fetches historical or intraday price data for multiple symbols using the yfinance library.
    Data is returned with UTC timezone-aware timestamps in a consistent format suitable for
    analysis or storage.

    Args:
        symbols: List of ticker symbols to download (max 100 symbols per call).
        period: Relative time period (e.g., '1d', '5d', '1mo', '3mo', '1y', 'max').
            Mutually exclusive with start_date/end_date.
        start_date: Start date as 'YYYY-MM-DD' string or datetime object (inclusive).
            Mutually exclusive with period.
        end_date: End date as 'YYYY-MM-DD' string or datetime object (exclusive).
            Defaults to current time if start_date is provided without end_date.
        interval: Data frequency interval. Options include:
            - Intraday: '1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h'
            - Daily+: '1d', '5d', '1wk', '1mo', '3mo'
            Defaults to '1d' (daily).
        output_dict: If True, returns dict mapping symbols to DataFrames.
            If False, returns single MultiIndex DataFrame with (date, symbol) levels.
            Defaults to False.
        **kwargs: Additional keyword arguments passed to yfinance.download().

    Returns:
        If output_dict=True: Dictionary mapping symbol strings to DataFrames, where each
            DataFrame has DatetimeIndex and columns ['open', 'high', 'low', 'close',
            'volume', 'symbol', 'interval' (intraday only)].
        If output_dict=False: Single DataFrame with MultiIndex (date, symbol) and same columns.
        Returns None if no data could be retrieved or on error.

    Raises:
        None: Errors are logged but not raised. Check return value for None.

    Examples:
        Basic daily data fetch:
            >>> prices = ohlcv_from_yfinance(
            ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
            ...     period='1y'
            ... )
            >>> print(prices.head())
            >>> # Returns MultiIndex DataFrame with (date, symbol) levels

        Fetch specific date range:
            >>> prices = ohlcv_from_yfinance(
            ...     symbols=['AAPL', 'MSFT'],
            ...     start_date='2024-01-01',
            ...     end_date='2024-12-31',
            ...     interval='1d'
            ... )

        Get 5-minute intraday bars:
            >>> intraday = ohlcv_from_yfinance(
            ...     symbols=['SPY', 'QQQ'],
            ...     period='1d',
            ...     interval='5m'
            ... )
            >>> # Includes 'interval' column for intraday data

        Get data as dictionary by symbol:
            >>> prices_dict = ohlcv_from_yfinance(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='6mo',
            ...     output_dict=True
            ... )
            >>> aapl_df = prices_dict['AAPL']
            >>> # Work with individual symbol DataFrames

    Note:
        - Yahoo Finance has rate limits; avoid excessive requests
        - Intraday data availability is limited (typically last 7-60 days depending on interval)
        - Max 100 symbols per call to avoid timeout issues
        - All timestamps are converted to UTC timezone
    """
    import yfinance as yf

    if period is None and start_date is None:
        logger.error("Either start_date or period must be specified")
        return None

    intraday_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h']
    intraday = interval in intraday_intervals

    if len(symbols) > 100:
        logger.error('symbols size exceeds 100 symbols. Please limit to 100 symbols per invocation.')
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
        return None

    if len(secs_prices_dict) == 0:
        logger.error('No data retrieved for any symbols')
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
    """Retrieve historical or intraday OHLCV data from ArcticDB storage.

    Queries previously stored time series data from ArcticDB with flexible date filtering
    and output formatting options. Supports both long format (MultiIndex) and wide format
    (pivoted) for different analysis workflows.

    Args:
        symbols: List of ticker symbols to retrieve (e.g., ['AAPL', 'MSFT', 'GOOGL']).
        start_date: Start date as 'YYYY-MM-DD' string or pd.Timestamp (inclusive).
            Mutually exclusive with period.
        end_date: End date as 'YYYY-MM-DD' string or pd.Timestamp (exclusive).
            Defaults to current UTC time if not specified.
        period: Relative time period (e.g., '5d', '3m', '1y').
            Mutually exclusive with start_date/end_date.
        columns: List of specific columns to retrieve (e.g., ['close', 'volume']).
            The 'symbol' column is always included automatically. If None, all columns
            are retrieved.
        library_name: ArcticDB library name to query. If None, uses
            ARCTICDB_DEFAULT_LIBRARY_NAME from ~/.chronos_lab/.env configuration.
        pivot: If True, reshape to wide format with symbols as columns.
            If False (default), return long format with MultiIndex (date, symbol).
        group_by: When pivot=True, controls column ordering in MultiIndex:
            - 'column' (default): Creates (column, symbol) ordering (e.g., close_AAPL, close_MSFT)
            - 'symbol': Creates (symbol, column) ordering (e.g., AAPL_close, AAPL_high)

    Returns:
        If pivot=False: DataFrame with MultiIndex (date, symbol) and columns
            ['open', 'high', 'low', 'close', 'volume', ...].
        If pivot=True: DataFrame with DatetimeIndex and MultiIndex columns
            organized by group_by parameter.
        Returns None if no data found, invalid parameters, or on error.

    Raises:
        None: Errors are logged but not raised. Check return value for None.

    Examples:
        Basic retrieval with relative period:
            >>> from chronos_lab.sources import ohlcv_from_arcticdb
            >>>
            >>> prices = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
            ...     period='3m',
            ...     library_name='yfinance'
            ... )
            >>> print(prices.head())
            >>> # Returns MultiIndex (date, symbol) DataFrame

        Specify exact date range:
            >>> prices = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT'],
            ...     start_date='2024-01-01',
            ...     end_date='2024-12-31',
            ...     library_name='yfinance'
            ... )

        Retrieve only specific columns:
            >>> closes = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
            ...     period='1y',
            ...     columns=['close'],
            ...     library_name='yfinance'
            ... )
            >>> # Returns only 'close' and 'symbol' columns

        Pivot to wide format for correlation analysis:
            >>> wide_prices = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
            ...     period='1y',
            ...     columns=['close'],
            ...     library_name='yfinance',
            ...     pivot=True,
            ...     group_by='column'
            ... )
            >>> # Creates columns: close_AAPL, close_MSFT, etc.
            >>> returns = wide_prices.pct_change()
            >>> correlation_matrix = returns.corr()

        Alternative pivot grouping by symbol:
            >>> wide_prices = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='6mo',
            ...     pivot=True,
            ...     group_by='symbol'
            ... )
            >>> # Creates MultiIndex: (AAPL, close), (AAPL, high), (MSFT, close), etc.

    Note:
        - Period strings: '7d' (days), '4w' (weeks), '3mo'/'3m' (months), '1y' (years)
        - All timestamps are UTC timezone-aware
        - Data must have been previously stored using ohlcv_to_arcticdb()
        - Empty result returns None with warning logged
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


def from_dataset(
        *,
        dataset_name: str,
        output_dict: Optional[bool] = False,
) -> Dict[str, pd.DataFrame] | pd.DataFrame | None:
    """Retrieve a dataset from local JSON file or DynamoDB table.

    Loads structured datasets stored locally or in DynamoDB. Returns data as either
    a pandas DataFrame (with automatic type inference) or as a dictionary.

    Args:
        dataset_name: Dataset identifier. Use 'ddb_' prefix for DynamoDB datasets,
            no prefix for local JSON files.
        output_dict: If True, returns dictionary format. If False (default), returns
            pandas DataFrame with type inference.

    Returns:
        If output_dict=False: pandas DataFrame with inferred datetime and numeric types
        If output_dict=True: Dictionary mapping keys to item attribute dicts
        Returns None on error.

    Examples:
        Load local dataset as DataFrame:
            >>> from chronos_lab.sources import from_dataset
            >>>
            >>> df = from_dataset(dataset_name='example')
            >>> print(df.head())
            >>> print(df.dtypes)

        Load DynamoDB dataset as DataFrame:
            >>> df = from_dataset(dataset_name='ddb_securities')
            >>> # Automatically infers datetime and numeric types

        Load as dictionary:
            >>> data_dict = from_dataset(
            ...     dataset_name='example',
            ...     output_dict=True
            ... )
            >>> for key, item in data_dict.items():
            ...     print(f"{key}: {item}")

    Note:
        - Local datasets: Loaded from {DATASET_LOCAL_PATH}/{name}.json
        - DynamoDB datasets: Require DATASET_DDB_TABLE_NAME and DATASET_DDB_MAP configuration
        - DataFrame index is the dataset keys (local) or sort keys (DynamoDB)
        - Datetime strings matching ISO format automatically converted to pandas datetime
        - Numeric strings automatically converted to numeric types
    """
    from chronos_lab.dataset import Dataset

    ds = Dataset()

    if output_dict:
        results = ds.get_dataset(dataset_name=dataset_name)
        if results['statusCode'] == 0:
            return results['payload']
        else:
            return None
    else:
        return ds.get_datasetDF(dataset_name=dataset_name)


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
    'from_dataset',
    'ohlcv_from_intrinio',
    'ohlcv_from_yfinance',
    'ohlcv_from_arcticdb',
    'securities_from_intrinio',
]
