"""Data source connectors for fetching and retrieving OHLCV time series data.

This module provides unified interfaces for fetching OHLCV (Open, High, Low, Close, Volume)
data from multiple sources including Yahoo Finance, Intrinio API, and ArcticDB storage.
All functions return data in consistent formats with UTC timezone-aware timestamps.

Data Sources:
    - Yahoo Finance (yfinance): Free historical and intraday market data via ohlcv_from_yfinance()
    - Intrinio API: Institutional-quality data requiring subscription via ohlcv_from_intrinio()
    - ArcticDB Storage: High-performance retrieval of stored data via ohlcv_from_arcticdb()
    - Dataset Storage: Structured datasets from local JSON or DynamoDB via from_dataset()
    - Securities List: Intrinio security metadata via securities_from_intrinio()

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

Important Notes:
    - All timestamps are UTC timezone-aware pandas DatetimeIndex
    - OHLCV data uses consistent column names: open, high, low, close, volume
    - Symbol identifiers vary by source: 'symbol' (Yahoo), 'id' (Intrinio)
    - MultiIndex format: (date, symbol) or (date, id) for efficient multi-symbol operations
    - Backend parameter available for ArcticDB operations (LMDB, S3, or MEM)
"""

from chronos_lab import logger
from chronos_lab.settings import get_settings
from chronos_lab._utils import _period
from typing import List, Optional, Dict, Union, Literal
from datetime import datetime, date
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
        backend: Optional[str] = None,
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
        backend: Storage backend type ('s3', 'lmdb', or 'mem', case-insensitive).
            If None, uses ARCTICDB_DEFAULT_BACKEND from ~/.chronos_lab/.env configuration.
            Overrides the default backend setting for this operation.
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
            >>> # Retrieve from default backend
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

        Retrieve from specific backend:
            >>> # Retrieve from S3 backend explicitly
            >>> prices = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='1y',
            ...     backend='s3',
            ...     library_name='yfinance'
            ... )
            >>>
            >>> # Retrieve from local LMDB backend explicitly
            >>> prices = ohlcv_from_arcticdb(
            ...     symbols=['AAPL', 'MSFT'],
            ...     period='1y',
            ...     backend='lmdb',
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
        - Backend parameter allows querying different storage backends (S3, LMDB, MEM)
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

    ac = ArcDB(library_name=library_name, backend=backend)

    result = ac.batch_read(symbol_list=symbols, **read_kwargs)

    if result['statusCode'] == 0:
        if 'symbol' in result['payload'].columns:
            result_df = result['payload'].sort_values(['symbol', 'date']).set_index('symbol', append=True)
        elif 'id' in result['payload'].columns:
            result_df = result['payload'].sort_values(['id', 'date']).set_index('id', append=True)
        else:
            logger.error("No 'symbol' or 'id' column found in batch read result. Check library configuration.")
            return None

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


def _prepare_ib_params(
        *,
        symbols: Optional[List[str]] = None,
        contracts: Optional[List] = None,
        period: Optional[str] = None,
        start_date: Optional[str | datetime | date] = None,
        end_date: Optional[str | datetime | date] = None,
        interval: str = '1d',
        what_to_show: str = 'ADJUSTED_LAST',
) -> Optional[Dict]:
    """Validate inputs and calculate IB API parameters.

    Returns:
        Dictionary with keys: barsize, start_dt, end_dt, duration, end_datetime, ib_params
        Returns None if validation fails.
    """
    from chronos_lab.ib import map_interval_to_barsize, calculate_ib_params

    if symbols is None and contracts is None:
        logger.error("Either symbols or contracts must be provided")
        return None

    if symbols is not None and contracts is not None:
        logger.error("Cannot specify both symbols and contracts")
        return None

    if period is None and start_date is None:
        logger.error("Either period or start_date must be specified")
        return None

    if period and start_date:
        logger.error("Cannot specify both 'period' and 'start_date'")
        return None

    if end_date and what_to_show == 'ADJUSTED_LAST':
        logger.error("Cannot specify end_date with what_to_show='ADJUSTED_LAST', use 'MIDPOINT' or 'TRADES' instead")
        return None

    try:
        barsize = map_interval_to_barsize(interval)
    except ValueError as e:
        logger.error(str(e))
        return None

    if period:
        start_dt, end_dt = _period(period,
                                   as_of=pd.to_datetime(end_date, utc=True) if isinstance(end_date, str) else end_date)
    else:
        if start_date:
            start_dt = pd.to_datetime(start_date, utc=True) if isinstance(start_date, str) else start_date
        else:
            logger.error("start_date must be provided when not using period")
            return None

        if end_date:
            end_dt = pd.to_datetime(end_date, utc=True) if isinstance(end_date, str) else end_date
        else:
            end_dt = pd.Timestamp.now(tz='UTC')

    try:
        ib_params = calculate_ib_params(start_dt, end_dt, barsize)
    except ValueError as e:
        logger.error(f"Failed to calculate IB API parameters: {str(e)}")
        return None

    duration = ib_params['duration_str']
    end_datetime = ib_params['end_datetime'] if end_date and what_to_show != 'ADJUSTED_LAST' else ""
    effective_start = ib_params['effective_start']

    if ib_params['will_overfetch']:
        logger.warning(
            f"IB API constraints require fetching {ib_params['overfetch_days']} extra days of data. "
            f"Requested: {start_dt.date()} to {end_dt.date()}, "
            f"will fetch from: {effective_start.date()}. "
            f"Results will be filtered to requested range."
        )

    return {
        'barsize': barsize,
        'start_dt': start_dt,
        'end_dt': end_dt,
        'duration': duration,
        'end_datetime': end_datetime,
        'ib_params': ib_params,
    }


def _format_ib_output(
        ohlcv: pd.DataFrame,
        ib_params: Dict,
        start_dt: pd.Timestamp,
        output_dict: bool
) -> Dict[str, pd.DataFrame] | pd.DataFrame:
    """Format OHLCV output with filtering and dict conversion.

    Args:
        ohlcv: OHLCV DataFrame with MultiIndex (date, symbol)
        ib_params: IB parameters dict containing 'will_overfetch' flag
        start_dt: Start datetime for filtering
        output_dict: If True, return dict mapping symbols to DataFrames

    Returns:
        Formatted DataFrame or dictionary of DataFrames
    """

    if ib_params['will_overfetch']:
        ohlcv_reset = ohlcv.reset_index()
        ohlcv_reset = ohlcv_reset[ohlcv_reset['date'] >= start_dt]
        ohlcv = ohlcv_reset.set_index(['date', 'symbol'])
        logger.info(f"Filtered results to requested date range: {len(ohlcv)} rows")

    if output_dict:
        result_dict = {}
        ohlcv_reset = ohlcv.reset_index()

        for symbol in ohlcv_reset['symbol'].unique():
            symbol_df = ohlcv_reset[ohlcv_reset['symbol'] == symbol].copy()
            symbol_df = symbol_df.set_index('date').sort_index()
            result_dict[symbol] = symbol_df

        return result_dict
    else:
        return ohlcv


def ohlcv_from_ib(
        *,
        symbols: Optional[List[str]] = None,
        contracts: Optional[List] = None,
        period: Optional[str] = None,
        start_date: Optional[str | datetime | date] = None,
        end_date: Optional[str | datetime | date] = None,
        interval: Optional[str] = '1d',
        what_to_show: Optional[str] = 'ADJUSTED_LAST',
        use_rth: Optional[bool] = True,
        output_dict: Optional[bool] = False
) -> Dict[str, pd.DataFrame] | pd.DataFrame | None:
    """Download OHLCV data from Interactive Brokers (synchronous version).

    Fetches historical price data for multiple symbols or contracts using the Interactive Brokers
    API via ib_async. This is the synchronous version - use ohlcv_from_ib_async for async contexts.

    Args:
        symbols: List of ticker symbols to download. Mutually exclusive with contracts.
        contracts: List of IB Contract objects. Mutually exclusive with symbols.
        period: Relative time period (e.g., '1d', '5d', '1mo', '1y').
            Mutually exclusive with start_date/end_date.
        start_date: Start date as 'YYYY-MM-DD' string or datetime object (inclusive).
            Mutually exclusive with period.
        end_date: End date as 'YYYY-MM-DD' string or datetime object (exclusive).
            Defaults to current time if start_date is provided without end_date.
        interval: Data frequency interval. Options include:
            - Intraday: '1m', '5m', '15m', '30m', '1h', '2h', '4h'
            - Daily+: '1d', '1w', '1M'
            Defaults to '1d' (daily).
        what_to_show: Type of data to retrieve. Options:
            - 'ADJUSTED_LAST': Split/dividend adjusted (default, no end_date allowed)
            - 'TRADES': Actual traded prices
            - 'MIDPOINT': Bid/ask midpoint
            - 'BID', 'ASK': Bid or ask prices only
        use_rth: If True, return only Regular Trading Hours data.
            If False, include extended hours. Defaults to True.
        output_dict: If True, returns dict mapping symbols to DataFrames.
            If False, returns single MultiIndex DataFrame with (date, symbol) levels.
            Defaults to False.

    Returns:
        If output_dict=True: Dictionary mapping symbol strings to DataFrames, where each
            DataFrame has DatetimeIndex and columns ['open', 'high', 'low', 'close', 'volume', 'symbol'].
        If output_dict=False: Single DataFrame with MultiIndex (date, symbol) and same columns.
        Returns None if no data could be retrieved or on error.

    """
    from chronos_lab.ib import get_ib, hist_to_ohlcv

    params = _prepare_ib_params(
        symbols=symbols,
        contracts=contracts,
        period=period,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        what_to_show=what_to_show
    )
    if params is None:
        return None

    try:
        ib = get_ib()
        if ib is None:
            logger.error("Failed to get IB connection")
            return None
    except Exception as e:
        logger.error(f"Failed to initialize IB connection: {str(e)}")
        return None

    if symbols is not None:
        try:
            contracts = ib.symbols_to_contracts(symbols=symbols)

            if not contracts:
                logger.error("Failed to create/qualify contracts from symbols")
                return None

        except Exception as e:
            logger.error(f"Failed to create contracts: {str(e)}")
            return None

    try:
        hist_data = ib.get_hist_data(
            contracts=contracts,
            duration=params['duration'],
            barsize=params['barsize'],
            datatype=what_to_show,
            end_datetime=params['end_datetime'],
            userth=use_rth
        )

        if hist_data is None or hist_data.empty:
            logger.error("No historical data returned from IB")
            return None

    except Exception as e:
        logger.error(f"Failed to fetch historical data: {str(e)}")
        return None

    try:
        ohlcv = hist_to_ohlcv(hist_data)

        if ohlcv.empty:
            logger.error("Failed to convert historical data to OHLCV format")
            return None

    except Exception as e:
        logger.error(f"Failed to convert to OHLCV format: {str(e)}")
        return None

    return _format_ib_output(ohlcv, params['ib_params'], params['start_dt'], output_dict)


async def ohlcv_from_ib_async(
        *,
        symbols: Optional[List[str]] = None,
        contracts: Optional[List] = None,
        period: Optional[str] = None,
        start_date: Optional[str | datetime | date] = None,
        end_date: Optional[str | datetime | date] = None,
        interval: Optional[str] = '1d',
        what_to_show: Optional[str] = 'ADJUSTED_LAST',
        use_rth: Optional[bool] = True,
        output_dict: Optional[bool] = False
) -> Dict[str, pd.DataFrame] | pd.DataFrame | None:
    """Download OHLCV data from Interactive Brokers (asynchronous version).

    Asynchronous version of ohlcv_from_ib. Fetches historical price data for multiple symbols
    or contracts using the Interactive Brokers API with concurrent requests for better performance.

    Args:
        symbols: List of ticker symbols to download. Mutually exclusive with contracts.
        contracts: List of IB Contract objects. Mutually exclusive with symbols.
        period: Relative time period (e.g., '1d', '5d', '1mo', '1y').
            Mutually exclusive with start_date/end_date.
        start_date: Start date as 'YYYY-MM-DD' string or datetime object (inclusive).
            Mutually exclusive with period.
        end_date: End date as 'YYYY-MM-DD' string or datetime object (exclusive).
            Defaults to current time if start_date is provided without end_date.
        interval: Data frequency interval. Options include:
            - Intraday: '1m', '5m', '15m', '30m', '1h', '2h', '4h'
            - Daily+: '1d', '1w', '1M'
            Defaults to '1d' (daily).
        what_to_show: Type of data to retrieve. Options:
            - 'ADJUSTED_LAST': Split/dividend adjusted (default, no end_date allowed)
            - 'TRADES': Actual traded prices
            - 'MIDPOINT': Bid/ask midpoint
            - 'BID', 'ASK': Bid or ask prices only
        use_rth: If True, return only Regular Trading Hours data.
            If False, include extended hours. Defaults to True.
        output_dict: If True, returns dict mapping symbols to DataFrames.
            If False, returns single MultiIndex DataFrame with (date, symbol) levels.
            Defaults to False.

    Returns:
        If output_dict=True: Dictionary mapping symbol strings to DataFrames, where each
            DataFrame has DatetimeIndex and columns ['open', 'high', 'low', 'close', 'volume', 'symbol'].
        If output_dict=False: Single DataFrame with MultiIndex (date, symbol) and same columns.
        Returns None if no data could be retrieved or on error.

    """
    from chronos_lab.ib import get_ib, hist_to_ohlcv

    params = _prepare_ib_params(
        symbols=symbols,
        contracts=contracts,
        period=period,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        what_to_show=what_to_show
    )
    if params is None:
        return None

    try:
        ib = get_ib()
        if ib is None:
            logger.error("Failed to get IB connection")
            return None
    except Exception as e:
        logger.error(f"Failed to initialize IB connection: {str(e)}")
        return None

    if symbols is not None:
        try:
            contracts = await ib.symbols_to_contracts_async(symbols=symbols)

            if not contracts:
                logger.error("Failed to create/qualify contracts from symbols")
                return None

        except Exception as e:
            logger.error(f"Failed to create contracts: {str(e)}")
            return None

    try:
        hist_data = await ib.get_hist_data_async(
            contracts=contracts,
            duration=params['duration'],
            barsize=params['barsize'],
            datatype=what_to_show,
            end_datetime=params['end_datetime'],
            userth=use_rth
        )

        if hist_data is None or hist_data.empty:
            logger.error("No historical data returned from IB")
            return None

    except Exception as e:
        logger.error(f"Failed to fetch historical data: {str(e)}")
        return None

    try:
        ohlcv = hist_to_ohlcv(hist_data)

        if ohlcv.empty:
            logger.error("Failed to convert historical data to OHLCV format")
            return None

    except Exception as e:
        logger.error(f"Failed to convert to OHLCV format: {str(e)}")
        return None

    return _format_ib_output(ohlcv, params['ib_params'], params['start_dt'], output_dict)


__all__ = [
    'from_dataset',
    'ohlcv_from_ib',
    'ohlcv_from_ib_async',
    'ohlcv_from_intrinio',
    'ohlcv_from_yfinance',
    'ohlcv_from_arcticdb',
    'securities_from_intrinio',
]
