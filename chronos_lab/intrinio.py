"""Low-level Intrinio SDK wrapper for institutional financial data access.

This module provides the Intrinio class, a low-level wrapper around the Intrinio SDK
for accessing institutional-quality financial data including securities, prices, company
information, and exchange data.

**IMPORTANT**: This is a low-level wrapper intended for internal use. For most use cases,
prefer the high-level functions in chronos_lab.sources module:
    - Use ohlcv_from_intrinio() for fetching OHLCV data
    - Use securities_from_intrinio() for fetching securities lists

Direct usage of this module should be limited to advanced scenarios requiring fine-grained
control over Intrinio API operations, custom pagination handling, or direct access to
underlying Intrinio SDK APIs.

API Configuration:
    Set your Intrinio API key in ~/.chronos_lab/.env:
        INTRINIO_API_KEY=your_api_key_here

    Or pass directly to the Intrinio class constructor.

Advanced API Access:
    The Intrinio class exposes underlying Intrinio SDK API objects for advanced operations:
        - _ApiClient: Base API client for configuration and request handling
        - _SecurityApi: Security-related endpoints (prices, fundamentals, etc.)
        - _CompanyApi: Company information endpoints
        - _StockExchangeApi: Exchange and market data endpoints

    For detailed API documentation and additional available APIs, refer to:
        https://docs.intrinio.com/documentation/python

Typical Usage:
    Basic operations (internal use):
        >>> from chronos_lab.intrinio import Intrinio
        >>>
        >>> # Initialize with API key
        >>> intr = Intrinio(api_key='your_key')
        >>>
        >>> # Fetch securities list
        >>> result = intr.get_all_securities(
        ...     active=True,
        ...     composite_mic='USCOMP',
        ...     code='EQS'
        ... )
        >>> securities = result['payload']
        >>>
        >>> # Fetch stock prices
        >>> result = intr.get_security_stock_prices(
        ...     identifier='AAPL',
        ...     start_date='2024-01-01',
        ...     frequency='daily'
        ... )
        >>> prices = result['stockPrices']

    Advanced direct SDK access:
        >>> intr = Intrinio(api_key='your_key')
        >>> # Access Security API directly
        >>> sec_api = intr._SecurityApi
        >>> # Use any Intrinio SDK method
        >>> fundamentals = sec_api.get_security_reported_fundamentals(
        ...     identifier='AAPL',
        ...     statement_code='income_statement'
        ... )
"""

from chronos_lab import logger
from chronos_lab.settings import get_settings
import intrinio_sdk as intrinio
from intrinio_sdk.rest import ApiException
import pandas as pd
import time
from datetime import datetime, timedelta



class Intrinio:
    """Low-level wrapper for Intrinio SDK financial data API operations.

    Provides methods for accessing institutional-quality financial data with automatic
    pagination handling, rate limit management, and error handling. Manages Intrinio
    SDK configuration and provides access to underlying API objects for advanced usage.

    **NOTE**: This is a low-level class. For typical use cases, prefer high-level
    functions in chronos_lab.sources module (ohlcv_from_intrinio, securities_from_intrinio).

    Attributes:
        _config: Intrinio SDK Configuration object
        _ApiClient: Base API client for request handling. See Intrinio SDK docs for
            advanced usage: https://docs.intrinio.com/documentation/python
        _SecurityApi: Security-related API endpoints (prices, fundamentals, ownership)
        _CompanyApi: Company information API endpoints (filings, news, data points)
        _StockExchangeApi: Stock exchange and market data API endpoints

    Examples:
        Basic initialization:
            >>> from chronos_lab.intrinio import Intrinio
            >>>
            >>> # With API key from config file
            >>> intr = Intrinio()
            >>>
            >>> # With explicit API key
            >>> intr = Intrinio(api_key='your_api_key_here')
            >>>
            >>> # With proxy configuration
            >>> intr = Intrinio(api_key='your_key', proxy='http://proxy:8080')

        Fetch securities list:
            >>> result = intr.get_all_securities(
            ...     active=True,
            ...     delisted=False,
            ...     code='EQS',
            ...     composite_mic='USCOMP'
            ... )
            >>> if result['statusCode'] == 0:
            ...     securities = result['payload']

        Fetch daily stock prices:
            >>> result = intr.get_security_stock_prices(
            ...     identifier='AAPL',
            ...     start_date='2024-01-01',
            ...     end_date='2024-12-31',
            ...     frequency='daily'
            ... )
            >>> prices = result['stockPrices']

        Advanced SDK access:
            >>> intr = Intrinio()
            >>> # Access Security API for fundamentals
            >>> fundamentals = intr._SecurityApi.get_security_reported_fundamentals(
            ...     identifier='AAPL',
            ...     statement_code='income_statement',
            ...     fiscal_year=2024
            ... )
            >>> # Access Company API for news
            >>> news = intr._CompanyApi.get_company_news(
            ...     identifier='AAPL',
            ...     page_size=10
            ... )
    """

    def __init__(self,
                 api_key=None,
                 proxy=None):
        """Initialize Intrinio SDK with API credentials and configuration.

        Sets up Intrinio SDK configuration and initializes API clients for accessing
        financial data endpoints.

        Args:
            api_key: Intrinio API key. If None, reads from INTRINIO_API_KEY in
                ~/.chronos_lab/.env configuration file.
            proxy: Optional HTTP proxy URL (e.g., 'http://proxy.example.com:8080').

        Examples:
            >>> # Use API key from configuration
            >>> intr = Intrinio()
            >>>
            >>> # Provide API key explicitly
            >>> intr = Intrinio(api_key='your_intrinio_api_key')
            >>>
            >>> # With proxy
            >>> intr = Intrinio(api_key='your_key', proxy='http://proxy:8080')
        """
        self._config = intrinio.Configuration()

        if not api_key:
            settings = get_settings()
            api_key = settings.intrinio_api_key

        self._config.api_key['api_key'] = api_key

        if proxy:
            self._config.proxy = proxy

        self._ApiClient = intrinio.ApiClient(configuration=self._config)
        self._SecurityApi = intrinio.SecurityApi(self._ApiClient)
        self._CompanyApi = intrinio.CompanyApi(self._ApiClient)
        self._StockExchangeApi = intrinio.StockExchangeApi(self._ApiClient)

    def get_all_securities(self,
                       max_number_pages_returned=100,
                       next_page=None,
                       **kwargs):
        """Fetch all securities with automatic pagination handling.

        Retrieves a comprehensive list of securities from Intrinio API with support for
        filtering by status, type, exchange, and other criteria. Automatically handles
        pagination to fetch all matching securities up to the specified page limit.

        Args:
            max_number_pages_returned: Maximum number of pages to retrieve (default: 100).
                Each page typically contains 100 securities.
            next_page: Optional pagination token for resuming from a specific page.
            **kwargs: Additional filter parameters passed to Intrinio API:
                - active: True/False to filter by active status
                - delisted: True/False to filter by delisted status
                - code: Security type code (e.g., 'EQS' for equity, 'ETF')
                - composite_mic: Composite MIC code (e.g., 'USCOMP' for US exchanges)
                - include_non_figi: Include securities without FIGI identifiers
                - primary_listing: Filter to primary listings only
                - page_size: Number of securities per page (default: 100)

        Returns:
            Dictionary with fetch results:
                - 'statusCode': 0 on success, -1 on error
                - 'payload': List of security dictionaries with metadata

        Examples:
            Fetch all US equity securities:
                >>> intr = Intrinio()
                >>> result = intr.get_all_securities(
                ...     active=True,
                ...     delisted=False,
                ...     code='EQS',
                ...     composite_mic='USCOMP',
                ...     primary_listing=True
                ... )
                >>> if result['statusCode'] == 0:
                ...     securities = result['payload']
                ...     print(f"Found {len(securities)} securities")

            Fetch ETFs with pagination limit:
                >>> result = intr.get_all_securities(
                ...     active=True,
                ...     code='ETF',
                ...     max_number_pages_returned=50
                ... )

        Note:
            - Automatically handles pagination; continues until no more pages or limit reached
            - Rate limits apply based on subscription tier
            - Returns all accumulated data from all pages in single payload
        """
        response = {
            'statusCode': 0,
            'payload': []
        }

        securitiesList = []

        logger.info('Calling SecurityApi->get_all_securities, args %s:', kwargs)
        while max_number_pages_returned > 0:
            try:
                api_response = self._SecurityApi.get_all_securities(next_page=next_page, **kwargs)

                next_page = api_response._next_page
                securitiesList += api_response.securities_dict

            except ApiException as e:
                logger.error("Exception when calling SecurityApi->get_all_securities: %s\r\n" % e)
                response['statusCode'] = -1
                return response

            if next_page == None:
                break
            max_number_pages_returned -= 1

        response['payload'] = securitiesList
        return response

    def get_security_stock_prices(self,
                                  max_number_pages_returned=100,
                                  next_page=None,
                                  dividend_only=False,
                                  split_ratio_only=False,
                                  output_df=True,
                                  interval=False,
                                  **kwargs):
        """Fetch historical or intraday stock prices with automatic pagination and rate limit handling.

        Retrieves OHLCV price data for a security with support for both historical (daily+)
        and intraday frequencies. Automatically handles pagination and rate limiting (429 errors)
        with intelligent retry logic.

        Args:
            max_number_pages_returned: Maximum number of pages to retrieve (default: 100).
                Each page typically contains 100 price bars.
            next_page: Optional pagination token for resuming from a specific page.
            dividend_only: If True, return only dividend information (default: False).
            split_ratio_only: If True, return only split ratio information (default: False).
            output_df: If True, return empty DataFrame on error; if False, return
                response dict (default: True).
            interval: If True, fetch intraday prices; if False, fetch daily+ prices
                (default: False).
            **kwargs: Required and optional parameters for Intrinio API:
                Required:
                    - identifier: Security identifier (ticker, FIGI, or Intrinio ID)
                For historical prices (interval=False):
                    - frequency: 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
                    - start_date: Start date (YYYY-MM-DD)
                    - end_date: End date (YYYY-MM-DD)
                For intraday prices (interval=True):
                    - interval_size: '1m', '5m', '10m', '15m', '30m', '60m', '1h'
                    - start_date: Start datetime
                    - end_date: End datetime
                Optional:
                    - page_size: Number of records per page (default: 100)
                    - sort_order: 'asc' or 'desc'

        Returns:
            If output_df=True: Empty pandas DataFrame on error
            If output_df=False: Dictionary with:
                - 'statusCode': 0 on success, -1 on error
                - 'stockPrices': List of price dictionaries
                - 'security': Security metadata dictionary

        Examples:
            Fetch daily prices:
                >>> intr = Intrinio()
                >>> result = intr.get_security_stock_prices(
                ...     identifier='AAPL',
                ...     start_date='2024-01-01',
                ...     end_date='2024-12-31',
                ...     frequency='daily',
                ...     output_df=False,
                ...     interval=False
                ... )
                >>> if result['statusCode'] == 0:
                ...     prices = result['stockPrices']
                ...     security = result['security']

            Fetch intraday 5-minute bars:
                >>> result = intr.get_security_stock_prices(
                ...     identifier='SPY',
                ...     start_date='2024-01-15 09:30:00',
                ...     end_date='2024-01-15 16:00:00',
                ...     interval_size='5m',
                ...     output_df=False,
                ...     interval=True
                ... )

            Fetch with automatic rate limit handling:
                >>> # Rate limits are automatically handled with retry logic
                >>> result = intr.get_security_stock_prices(
                ...     identifier='MSFT',
                ...     start_date='2023-01-01',
                ...     frequency='daily',
                ...     output_df=False
                ... )

        Note:
            - Automatically retries on 429 (rate limit) errors with intelligent wait
            - Wait time calculated to next minute boundary plus 5 second buffer
            - Historical and intraday data use different API endpoints
            - Pagination continues automatically until all data retrieved
        """
        response = {
            'statusCode': 0,
            'stockPrices': [],
            'security': None
        }
        stockPriceList = []

        logger.info('Calling SecurityApi->get_security_stock_prices/get_security_interval_prices, args %s:', kwargs)
        while max_number_pages_returned > 0:
            try:
                if interval:
                    api_response = self._SecurityApi.get_security_interval_prices(next_page=next_page, **kwargs)
                else:
                    api_response = self._SecurityApi.get_security_stock_prices(next_page=next_page, **kwargs)
            except ApiException as e:
                if e.status == 429:
                    now = datetime.now()
                    next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                    wait_seconds = int((next_minute - now).total_seconds()) + 5

                    logger.warning("Rate limit exceeded. Waiting %d seconds before retry", wait_seconds)
                    time.sleep(wait_seconds)
                    continue

                logger.error("Exception when calling SecurityApi->get_security_stock_prices/get_security_interval_prices: %s\r\n" % e)
                if output_df:
                    return pd.DataFrame()
                else:
                    response['statusCode'] = -1
                    return response

            if not response['security'] and hasattr(api_response, 'security_dict'):
                response['security'] = api_response.security_dict

            next_page = api_response._next_page
            if interval:
                stockPriceList += api_response.intervals_dict
            else:
                stockPriceList += api_response.stock_prices_dict

            if next_page == None:
                break
            max_number_pages_returned -= 1

        if output_df:
            sp_df = pd.DataFrame(stockPriceList)
            if len(sp_df) == 0:
                return sp_df

            sp_df['id'] = kwargs['identifier']

            if interval:
                sp_df['date'] = pd.to_datetime(sp_df['close_time'], errors='coerce', utc=True)
                sp_df = sp_df.drop(columns=['close_time'])
            else:
                sp_df['date'] = pd.to_datetime(sp_df['date'], errors='coerce', utc=True)

            if dividend_only and not interval:
                return sp_df[sp_df['dividend'] != 0][['id', 'date', 'dividend', 'frequency']].set_index(
                    ['id', 'date'])
            elif split_ratio_only and not interval:
                return sp_df[sp_df['split_ratio'] != 1][['id', 'date', 'split_ratio', 'frequency']].set_index(
                    ['id', 'date'])
            else:
                return sp_df.set_index(['id', 'date'])
        else:
            response['stockPrices'] = stockPriceList
            return response

    def get_all_companies_daily_metrics(self,
                                  max_number_pages_returned=100,
                                  next_page=None,
                                  **kwargs):

        response = {
            'statusCode': 0,
            'payload': []
        }
        metricsList = []

        logger.info('Calling CompanyApi->get_all_companies_daily_metrics, args %s:', kwargs)
        while max_number_pages_returned > 0:
            try:
                api_response = self._CompanyApi.get_all_companies_daily_metrics(next_page=next_page, **kwargs)

                next_page = api_response._next_page
                metricsList += api_response.daily_metrics_dict

            except ApiException as e:
                logger.error("Exception when calling CompanyApi->get_all_companies_daily_metrics: %s\r\n" % e)
                response['statusCode'] = -1
                return response

            if next_page == None:
                break
            max_number_pages_returned -= 1

        response['payload'] = metricsList
        return response

    def get_uscomp_securities(self,
                              *,
                              codes=None):
        if codes is None:
            codes = ['EQS', 'ETF', 'DR']

        response = {
            'statusCode': 0,
            'payload': [],
            'failedCodes': [],
            'successfulCodes': []
        }

        securitiesList = []
        for code in codes:
            code_ret = self.get_all_securities(active=True, delisted=False, code=code, composite_mic='USCOMP',
                                                include_non_figi=False,
                                                page_size=100, primary_listing=True)
            if code_ret['statusCode'] != 0:
                response['statusCode'] += 1
                response['failedCodes'].append(code)
            else:
                securitiesList += code_ret['payload']
                response['successfulCodes'].append(code)

        response['payload'] = securitiesList
        return response

    def get_stock_exchange_realtime_prices(self,
                                           identifier='USCOMP',
                                           max_number_pages_returned=1000,
                                           page_size=100,
                                           next_page=None,
                                           **kwargs):

        response = {
            'statusCode': 0,
            'payload': []
        }
        pricesList = []

        logger.info('Calling StockExchangeApi->get_stock_exchange_realtime_prices, args %s:', kwargs)
        while max_number_pages_returned > 0:
            try:
                api_response = self._StockExchangeApi.get_stock_exchange_realtime_prices(identifier=identifier,
                                                                                         next_page=next_page,
                                                                                         page_size=page_size,
                                                                                         active_only=True,
                                                                                         **kwargs)

            except ApiException as e:
                if e.status == 429:
                    now = datetime.now()
                    next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                    wait_seconds = int((next_minute - now).total_seconds()) + 1

                    logger.warning("Rate limit exceeded. Waiting %d seconds before retry", wait_seconds)
                    time.sleep(wait_seconds)
                    continue

                logger.error("Exception when calling StockExchangeApi->get_stock_exchange_realtime_prices: %s\r\n" % e)
                response['statusCode'] = -1
                return response

            next_page = api_response._next_page
            pricesList += api_response.stock_prices_dict

            if next_page == None:
                break
            max_number_pages_returned -= 1

        response['payload'] = pricesList
        return response

    def get_stock_exchange_quote(self,
                                 identifier='USCOMP',
                                 output_df=True,
                                 **kwargs):

        response = {
            'statusCode': 0,
            'payload': []
        }
        logger.info('Calling StockExchangeApi->get_stock_exchange_quote, args %s:', kwargs)

        try:
            api_response = self._StockExchangeApi.get_stock_exchange_quote(identifier=identifier,
                                                                                active_only=True,
                                                                                **kwargs)
        except ApiException as e:
            logger.error("Exception when calling StockExchangeApi->get_stock_exchange_quote: %s\r\n" % e)
            response['statusCode'] = -1
            return response

        if output_df:
            if len(api_response.quotes_dict) > 0:
                quotes_df = pd.json_normalize(api_response.quotes_dict).rename(
                    columns={
                        'security.figi': 'id',
                        'security.id': 'sec_id',
                        'last_time': 'date'
                    }
                ).set_index(['id', 'date'])
                quotes_df.columns = quotes_df.columns.str.replace(r'^security\.', '', regex=True)
                response['payload'] = quotes_df
            else:
                response['statusCode'] = -1
        else:
            response['payload'] = api_response.quotes_dict

        return response

    def get_stock_exchange_quote_batch(self,
                                       tickers,
                                       batch_size=100,
                                       pooltype='thread',
                                       **kwargs):
        from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

        response = {
            'statusCode': 0,
            'payload': pd.DataFrame()
        }
        batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]

        dfs = []
        with ProcessPoolExecutor() if pooltype == 'process' else ThreadPoolExecutor() as executor:
            future_to_key = {executor.submit(self.get_stock_exchange_quote, tickers=batch, **kwargs): batch for batch in batches}

            for future in as_completed(future_to_key):
                try:
                    df = future.result()
                    if df['statusCode'] == 0:
                        dfs.append(df['payload'])
                except Exception as e:
                    logger.error("Error reading %s: %s", future_to_key[future], e)
                    response['statusCode'] = -1
                    return response

        response['payload'] = pd.concat(dfs)
        return response

    def get_security_snapshots(self, at_datetime="", **kwargs):
        """
        Get security snapshots from Intrinio API and return as DataFrame.

        Args:
            at_datetime (str): Date/time for the snapshot (optional)
            **kwargs: Additional parameters for the API call

        Returns:
            dict: Response containing statusCode and payload (DataFrame)
        """

        import requests
        import gzip
        import io

        response = {
            'statusCode': 0,
            'payload': pd.DataFrame()
        }

        logger.info('Calling SecurityApi->get_security_snapshots, at_datetime: %s, args: %s', at_datetime, kwargs)

        try:
            # Call the Intrinio API
            api_response = self._SecurityApi.get_security_snapshots(at_datetime=at_datetime, **kwargs)

            # Convert API response to dictionary
            if hasattr(api_response, 'to_dict'):
                response_dict = api_response.to_dict()
            else:
                response_dict = api_response

            logger.info('API response structure: %s', response_dict)

            # Extract download URLs from the nested structure
            # Response format: {'snapshots': [{'files': [{'url': '...', 'part': 0, 'size': ...}], 'time': datetime}]}
            download_urls = []

            if 'snapshots' in response_dict:
                for snapshot in response_dict['snapshots']:
                    if 'files' in snapshot:
                        for file_info in snapshot['files']:
                            if 'url' in file_info:
                                download_urls.append({
                                    'url': file_info['url'],
                                    'part': file_info.get('part', 0),
                                    'size': file_info.get('size', 0),
                                    'time': snapshot.get('time')
                                })

            if not download_urls:
                logger.error("No download URLs found in API response")
                response['statusCode'] = -1
                return response

            logger.info('Found %d file(s) to download', len(download_urls))

            # Download and process all files (in case there are multiple parts)
            all_dataframes = []

            for file_info in sorted(download_urls, key=lambda x: x['part']):
                logger.info('Downloading file part %d from URL: %s', file_info['part'], file_info['url'])

                # Download the gzipped CSV file
                file_response = requests.get(file_info['url'], timeout=300)
                file_response.raise_for_status()

                # Decompress the gzipped content
                with gzip.GzipFile(fileobj=io.BytesIO(file_response.content)) as gz_file:
                    csv_content = gz_file.read().decode('utf-8')

                # Convert to DataFrame
                df_part = pd.read_csv(io.StringIO(csv_content))

                # Add metadata columns
                df_part['snapshot_time'] = file_info['time']
                df_part['file_part'] = file_info['part']

                all_dataframes.append(df_part)
                logger.info('Successfully processed file part %d with %d rows', file_info['part'], len(df_part))

            # Combine all parts into a single DataFrame
            if all_dataframes:
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                response['payload'] = combined_df
                logger.info('Successfully processed security snapshots data with %d total rows from %d file(s)',
                            len(combined_df), len(all_dataframes))
            else:
                logger.warning('No data files were processed')
                response['statusCode'] = -1

        except ApiException as e:
            logger.error("Exception when calling SecurityApi->get_security_snapshots: %s", e)
            response['statusCode'] = -1
            return response
        except requests.exceptions.RequestException as e:
            logger.error("Exception when downloading snapshot file: %s", e)
            response['statusCode'] = -1
            return response
        except Exception as e:
            logger.error("Exception when processing snapshot data: %s", e)
            response['statusCode'] = -1
            return response

        return response

