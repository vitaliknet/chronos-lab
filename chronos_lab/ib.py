"""Interactive Brokers market data integration for real-time and historical data.

This module provides the IBMarketData singleton class for connecting to Interactive Brokers
TWS/Gateway and retrieving market data including real-time ticks, historical bars, and
streaming OHLCV data. It wraps the ib_async library with chronos-lab conventions.

Key Features:
    - Singleton connection management to IB Gateway/TWS
    - Real-time tick data subscription and retrieval
    - Historical and real-time bar data subscription
    - Asynchronous batch operations with semaphore-controlled concurrency
    - Contract creation, qualification, and details lookup
    - Automatic timezone handling (UTC) and data formatting

Connection Configuration:
    IB connection parameters are configured via environment variables in ~/.chronos_lab/.env:
        - IB_GATEWAY_HOST: Gateway/TWS hostname (default: 127.0.0.1)
        - IB_GATEWAY_PORT: Gateway/TWS port (default: 4002 for Gateway, 7497 for TWS)
        - IB_GATEWAY_READONLY: Read-only mode (default: True)
        - IB_GATEWAY_CLIENT_ID: Client ID for connection (default: 1)
        - IB_GATEWAY_ACCOUNT: IB account identifier
        - IB_REF_DATA_CONCURRENCY: Max concurrent reference data requests (default: 10)
        - IB_HISTORICAL_DATA_CONCURRENCY: Max concurrent historical data requests (default: 10)

Typical Usage:
    Singleton pattern with automatic connection:
        >>> from chronos_lab.ib import get_ib
        >>>
        >>> # Get singleton instance and connect
        >>> ib = get_ib()
        >>>
        >>> # Subscribe to streaming bars
        >>> contract_ids = ib.subscribe_bars(
        ...     symbols=['AAPL', 'MSFT'],
        ...     period='1d',
        ...     interval='5m'
        ... )
        >>>
        >>> # Retrieve current bars
        >>> df = ib.get_bars(symbols=['AAPL', 'MSFT'])
        >>>
        >>> # Clean disconnect
        >>> ib.disconnect()

    Asynchronous historical data retrieval:
        >>> import asyncio
        >>> from chronos_lab.ib import get_ib
        >>>
        >>> ib = get_ib()
        >>> contracts = ib.symbols_to_contracts(['AAPL', 'MSFT', 'GOOGL'])
        >>>
        >>> # Fetch historical data asynchronously
        >>> hist_data = asyncio.run(
        ...     ib.get_hist_data_async(
        ...         contracts=contracts,
        ...         duration='30 D',
        ...         barsize='1 hour',
        ...         datatype='TRADES'
        ...     )
        ... )
"""

from chronos_lab import logger
from chronos_lab.settings import get_settings
from typing import Optional, List, Literal, Dict, Any, TypeAlias
from datetime import datetime, date
import asyncio
from asyncio import Semaphore
from ib_async import IB, util, Contract, RealTimeBar
import pandas as pd

settings = get_settings()

SecType: TypeAlias = Literal['STK', 'CASH', 'IND', 'FUT', 'CRYPTO', 'CMDTY']


class IBMarketData:
    """Singleton class for Interactive Brokers market data operations.

    Provides centralized connection management and data retrieval from Interactive Brokers
    TWS/Gateway. Supports real-time tick subscriptions, historical data requests, streaming
    bars, and contract lookups. Uses singleton pattern to ensure a single connection instance
    across the application.

    The class maintains internal state for active subscriptions (tickers, bars) and cached
    contract details. Supports both synchronous and asynchronous operations with semaphore-
    controlled concurrency for API rate limiting.

    Attributes:
        conn: Active IB connection instance from ib_async library. None if not connected.
        _connected: Boolean indicating whether connection is established.
        tickers: Dictionary mapping contract IDs to real-time tick data objects.
        bars: Nested dictionary containing OHLCV bar data and contract mappings.
            Structure: {'ohlcv': {conId: bars}, 'contract': {conId: contract}}
        contract_details: Dictionary mapping contract IDs to cached contract detail objects.
        gen_tick_list: Default generic tick list string for market data subscriptions
            (includes shortcuts, option volume, IV, etc.).
        _ref_data_sem: Asyncio semaphore controlling concurrent reference data requests.
        _historical_data_sem: Asyncio semaphore controlling concurrent historical data requests.
        _tickers_cols: List of column names for tick data DataFrame output.
        _bars_cols: List of column names for bar data DataFrame output.

    Note:
        - This class uses the singleton pattern. Use get_instance() or get_ib() to obtain instance.
        - Connection parameters are read from chronos_lab settings (configured in ~/.chronos_lab/.env).
        - All datetime values are converted to UTC timezone-aware timestamps.
        - Subscriptions remain active until explicitly cancelled or disconnected.
        - Contract IDs (conId) are used as primary keys for data storage and retrieval.
    """
    conn: Optional[IB] = None
    _instance: Optional["IBMarketData"] = None
    _connected: bool = False

    tickers: Dict[str, Any] = {}
    _tickers_cols: List = ['time', 'symbol', 'last', 'lastSize', 'bid', 'bidSize',
                         'ask', 'askSize', 'open', 'high', 'low', 'close', 'conId', 'marketPrice']
    bars: Dict[str, Any] = {
        'ohlcv': {},
        'contract': {}
    }
    _bars_cols: List = ['contract', 'date', 'open', 'high', 'low', 'close', 'volume']

    contract_details: Dict[str, Any] = {}
    gen_tick_list: str = '104, 106, 165, 221, 411'

    _ref_data_sem: Optional[Semaphore] = None
    _historical_data_sem: Optional[Semaphore] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._ref_data_sem = Semaphore(settings.ib_ref_data_concurrency)
            cls._instance._historical_data_sem = Semaphore(settings.ib_historical_data_concurrency)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "IBMarketData":
        """Get or create the singleton IBMarketData instance.

        Returns:
            The singleton IBMarketData instance. Creates a new instance if one doesn't exist.
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def connect(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            readonly: Optional[bool] = None,
            client_id: Optional[int] = None,
            account: Optional[str] = None,
    ) -> bool:
        """Connect to Interactive Brokers TWS or Gateway.

        Establishes connection to IB using provided parameters or defaults from settings.
        If already connected, returns True without creating a new connection.

        Args:
            host: TWS/Gateway hostname or IP address. If None, uses IB_GATEWAY_HOST from
                settings (default: 127.0.0.1).
            port: TWS/Gateway port number. If None, uses IB_GATEWAY_PORT from settings
                (default: 4002 for Gateway, 7497 for TWS).
            readonly: Read-only mode flag. If None, uses IB_GATEWAY_READONLY from settings
                (default: True).
            client_id: Client ID for connection. If None, uses IB_GATEWAY_CLIENT_ID from
                settings (default: 1). Must be unique per connection.
            account: IB account identifier. If None, uses IB_GATEWAY_ACCOUNT from settings.

        Returns:
            True if connection successful or already connected, False on connection failure.

        Note:
            - Connection parameters default to values in ~/.chronos_lab/.env
            - Uses ib_async IB.connect() for underlying connection
            - Sets _connected flag on successful connection
        """
        if self._connected:
            logger.info("Already connected to IB")
            return True

        host = host or settings.ib_gateway_host
        port = port or settings.ib_gateway_port
        readonly = readonly or settings.ib_gateway_readonly
        client_id = client_id or settings.ib_gateway_client_id
        account = account or settings.ib_gateway_account

        try:
            logger.info(f"Connecting to IB Gateway at {host}:{port}")

            self.conn = IB().connect(host=host,
                                     port=port,
                                     readonly=readonly,
                                     clientId=client_id,
                                     account=account
                                     )
            self._connected = True
            return True

        except Exception as e:
            logger.error(f"Failed to connect to IB: {str(e)}", exc_info=True)
            self._connected = False
            return False

    def disconnect(self):
        """Disconnect from Interactive Brokers and clean up active subscriptions.

        Automatically unsubscribes from all active tick and bar subscriptions before
        disconnecting. Safe to call even if not connected.

        Returns:
            Result of IB.disconnect() if connected, None if not connected.

        Note:
            - Cleans up all tick subscriptions via unsub_tickers()
            - Cleans up all bar subscriptions via unsub_bars()
            - Resets _connected flag
        """
        if not self._connected:
            logger.error('There is no active connection to IB gateway')
            return None
        else:
            if len(self.tickers) > 0:
                logger.info('Unsubscribing from tickers')
                self.unsub_tickers()
            if len(self.bars['ohlcv']) > 0:
                logger.info('Unsubscribing from bars')
                self.unsub_bars()
            logger.info('Disconnecting from IB gateway')
            return self.conn.disconnect()

    def get_hist_data(self,
                      contracts,
                      duration,
                      barsize,
                      datatype,
                      end_datetime: Optional[str | datetime | date] = '',
                      userth=True):
        """Retrieve historical bar data for multiple contracts synchronously.

        Fetches historical OHLCV data for a list of contracts using IB's reqHistoricalData.
        Automatically qualifies contracts if needed. Returns a MultiIndex DataFrame indexed
        by (contract, datatype, date).

        Args:
            contracts: List of ib_async Contract objects to retrieve data for.
            duration: IB duration string (e.g., '1 D', '2 W', '30 D', '1 Y').
            barsize: IB bar size string (e.g., '1 min', '5 mins', '1 hour', '1 day').
            datatype: IB data type string ('TRADES', 'MIDPOINT', 'BID', 'ASK', 'BID_ASK',
                'ADJUSTED_LAST', 'HISTORICAL_VOLATILITY', 'OPTION_IMPLIED_VOLATILITY').
            end_datetime: End date/time for historical data. Empty string (default) uses
                current time. Accepts string, datetime, or date objects.
            userth: Use Regular Trading Hours only. True (default) for RTH, False for
                extended hours.

        Returns:
            MultiIndex DataFrame with index (contract, datatype, date) and columns
            ['open', 'high', 'low', 'close', 'volume', 'barsize']. Returns empty
            DataFrame if no data available for any contract.

        Note:
            - Date column is converted to UTC timezone-aware timestamps
            - Contracts with conId=0 are automatically qualified
            - Each contract is fetched sequentially (for async, use get_hist_data_async)
            - Warnings logged for contracts with no data
        """

        hist_data = []
        for contract in contracts:
            logger.info('Requesting historical data for %s', contract)

            if contract.conId == 0:
                self.conn.qualifyContracts(contract)

            hist_data_contract = util.df(self.conn.reqHistoricalData(
                contract,
                endDateTime=end_datetime,
                durationStr=duration,
                barSizeSetting=barsize,
                whatToShow=datatype,
                useRTH=userth,
                formatDate=2,
                keepUpToDate=False))

            if not isinstance(hist_data_contract, pd.DataFrame):
                logger.warning(f'No data returned for {contract}')
                continue

            hist_data_contract['date'] = pd.to_datetime(hist_data_contract['date'], utc=True)
            hist_data_contract['datatype'] = datatype
            hist_data_contract['contract'] = contract
            hist_data_contract['barsize'] = barsize

            hist_data.append(hist_data_contract.set_index(['contract', 'datatype', 'date']))

        if not hist_data:
            logger.warning('No valid historical data returned for any contract')
            return pd.DataFrame()
        return pd.concat(hist_data, sort=False)

    async def get_hist_data_single(self,
                                   contract,
                                   duration,
                                   barsize,
                                   datatype,
                                   end_datetime: Optional[str | datetime | date] = '',
                                   userth=True):
        """Asynchronously retrieve historical data for a single contract with rate limiting.

        Internal async method for fetching historical data with semaphore-controlled
        concurrency. Used by get_hist_data_async for parallel batch operations.

        Args:
            contract: Single ib_async Contract object to retrieve data for.
            duration: IB duration string (e.g., '1 D', '2 W', '30 D', '1 Y').
            barsize: IB bar size string (e.g., '1 min', '5 mins', '1 hour', '1 day').
            datatype: IB data type string ('TRADES', 'MIDPOINT', 'BID', 'ASK', etc.).
            end_datetime: End date/time for historical data. Empty string (default) uses
                current time. Accepts string, datetime, or date objects.
            userth: Use Regular Trading Hours only. True (default) for RTH, False for
                extended hours.

        Returns:
            DataFrame with columns ['date', 'open', 'high', 'low', 'close', 'volume',
            'datatype', 'contract', 'barsize']. Returns empty DataFrame on error.

        Note:
            - Uses _historical_data_sem semaphore for rate limiting
            - Automatically qualifies contract if conId=0
            - Date column converted to UTC timezone-aware timestamps
            - Logs errors and returns empty DataFrame on failure
        """
        try:
            async with self._historical_data_sem:
                if contract.conId == 0:
                    await self.conn.qualifyContractsAsync(contract)

                bars = await self.conn.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_datetime,
                    durationStr=duration,
                    barSizeSetting=barsize,
                    whatToShow=datatype,
                    useRTH=userth,
                    formatDate=2,
                    keepUpToDate=False)

                hist_data_contract = util.df(bars)

                if not isinstance(hist_data_contract, pd.DataFrame):
                    logger.warning(f'No data returned for {contract}')
                    return pd.DataFrame()

                hist_data_contract['date'] = pd.to_datetime(hist_data_contract['date'], utc=True)
                hist_data_contract['datatype'] = datatype
                hist_data_contract['contract'] = contract
                hist_data_contract['barsize'] = barsize

                return hist_data_contract

        except Exception as e:
            logger.error(f'Failed to get historical data for {contract}: {e}')
            return pd.DataFrame()

    async def get_hist_data_async(self, contracts, duration, barsize, datatype,
                                  end_datetime: Optional[str | datetime | date] = '',
                                  userth=True):
        """Asynchronously retrieve historical data for multiple contracts in parallel.

        Fetches historical OHLCV data for multiple contracts concurrently using asyncio
        TaskGroup. Rate-limited by _historical_data_sem semaphore. Returns a MultiIndex
        DataFrame indexed by (contract, datatype, date).

        Args:
            contracts: List of ib_async Contract objects to retrieve data for.
            duration: IB duration string (e.g., '1 D', '2 W', '30 D', '1 Y').
            barsize: IB bar size string (e.g., '1 min', '5 mins', '1 hour', '1 day').
            datatype: IB data type string ('TRADES', 'MIDPOINT', 'BID', 'ASK', etc.).
            end_datetime: End date/time for historical data. Empty string (default) uses
                current time. Accepts string, datetime, or date objects.
            userth: Use Regular Trading Hours only. True (default) for RTH, False for
                extended hours.

        Returns:
            MultiIndex DataFrame with index (contract, datatype, date) and columns
            ['open', 'high', 'low', 'close', 'volume', 'barsize']. Returns empty
            DataFrame if no valid data retrieved for any contract.

        Note:
            - Uses asyncio.TaskGroup for concurrent execution
            - Concurrency controlled by IB_HISTORICAL_DATA_CONCURRENCY setting
            - Logs progress: total contracts requested and successfully retrieved
            - Failed contracts are skipped (logged as warnings)
        """
        logger.info(f'Requesting historical data for {len(contracts)} contracts')

        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(
                    self.get_hist_data_single(contract, duration, barsize, datatype, end_datetime, userth)
                )
                for contract in contracts
            ]

        results = [task.result() for task in tasks]
        valid_dfs = [df for df in results if not df.empty]

        if not valid_dfs:
            logger.warning('No valid historical data returned for any contract')
            return pd.DataFrame()

        logger.info(f'Successfully retrieved data for {len(valid_dfs)}/{len(contracts)} contracts')
        indexed_dfs = [df.set_index(['contract', 'datatype', 'date']) for df in valid_dfs]
        return pd.concat(indexed_dfs, sort=False)

    def sub_tickers(self,
                  contracts,
                  gen_tick_list=''):
        """Subscribe to real-time tick data for specified contracts.

        Initiates real-time market data subscriptions for a list of contracts. Automatically
        qualifies contracts if needed. Stores ticker objects in self.tickers keyed by contract ID.

        Args:
            contracts: List of ib_async Contract objects to subscribe to.
            gen_tick_list: Comma-separated string of generic tick types (e.g., '104,106,165').
                Empty string (default) subscribes to basic ticks only. Use self.gen_tick_list
                for a comprehensive set including shortcuts, option volume, and implied volatility.

        Note:
            - Contracts with conId=0 are automatically qualified
            - Skips contracts already subscribed (logs warning)
            - Ticker objects stored in self.tickers[conId]
            - Use get_tickers() to retrieve current tick data as DataFrame
            - Use unsub_tickers() to cancel subscriptions
        """
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)
            if c.conId not in self.tickers.keys():
                self.tickers[c.conId] = self.conn.reqMktData(c, genericTickList=gen_tick_list)
            else:
                logger.warning('Contract is already subscribed to receive ticks: %s', c)

    def unsub_tickers(self,
                    contract_ids=None):
        """Unsubscribe from real-time tick data subscriptions.

        Cancels market data subscriptions for specified contracts or all active subscriptions.

        Args:
            contract_ids: List of contract IDs (integers) to unsubscribe. If None (default),
                unsubscribes from all active tick subscriptions and clears self.tickers.

        Note:
            - Uses IB.cancelMktData() to cancel subscriptions
            - Removes unsubscribed contracts from self.tickers dictionary
            - Safe to call even if no active subscriptions
        """
        if contract_ids:
            for cid in contract_ids:
                self.conn.cancelMktData(self.tickers[cid].contract)
                del self.tickers[cid]
        else:
            for cid in self.tickers.keys():
                self.conn.cancelMktData(self.tickers[cid].contract)
            self.tickers = {}

    def get_tickers(self, allcols=False):
        """Retrieve current tick data as a DataFrame for all subscribed contracts.

        Converts active ticker objects to a pandas DataFrame with symbol index. Automatically
        handles timezone conversion (UTC) and calculates market price.

        Args:
            allcols: If True, returns all available columns (drops only columns with all NaN).
                If False (default), returns only standard tick columns defined in _tickers_cols:
                ['time', 'symbol', 'last', 'lastSize', 'bid', 'bidSize', 'ask', 'askSize',
                'open', 'high', 'low', 'close', 'conId', 'marketPrice'].

        Returns:
            DataFrame indexed by symbol with tick data columns. Returns empty DataFrame with
            standard columns if no active subscriptions.

        Note:
            - Time column is converted to UTC timezone-aware timestamps
            - marketPrice is calculated via ticker.marketPrice() method
            - Symbol and conId columns added from contract objects
        """
        if len(self.tickers) > 0:
            tickers_df = util.df(self.tickers.values())

            if len(tickers_df.time.dropna()) > 0:
                tickers_df.time = pd.to_datetime(tickers_df.time).dt.tz_convert('UTC')

            tickers_df['symbol'] = [x.symbol for x in tickers_df.contract]
            tickers_df['conId'] = [x.conId for x in tickers_df.contract]
            tickers_df['marketPrice'] = [self.tickers[x.conId].marketPrice() for x in tickers_df.contract]

            if allcols:
                return tickers_df.dropna(axis=1, how='all').set_index('symbol')
            else:
                return tickers_df[self._tickers_cols].set_index('symbol')
        else:
            return pd.DataFrame(columns=self._tickers_cols).set_index('symbol')

    def sub_bars(self,
                 contracts,
                 realtime=False,
                 **kwargs):
        """Subscribe to historical or real-time bar data for specified contracts.

        Initiates bar data subscriptions for a list of contracts. Supports both historical
        bars with keepUpToDate=True and real-time 5-second bars. Stores bar data in
        self.bars['ohlcv'] and contract references in self.bars['contract'].

        Args:
            contracts: List of ib_async Contract objects to subscribe to.
            realtime: If False (default), subscribes to historical bars (requires kwargs).
                If True, subscribes to real-time 5-second bars via reqRealTimeBars.
            **kwargs: Keyword arguments passed to IB.reqHistoricalData() when realtime=False.
                Required parameters: endDateTime, durationStr, barSizeSetting, whatToShow,
                useRTH, keepUpToDate, formatDate.

        Note:
            - Contracts with conId=0 are automatically qualified
            - Skips contracts already subscribed (logs warning)
            - Bar data stored in self.bars['ohlcv'][conId]
            - Contract objects stored in self.bars['contract'][conId]
            - Use get_bars() to retrieve bar data as DataFrame
            - Use unsub_bars() to cancel subscriptions
        """
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)
            if c.conId not in self.bars['ohlcv'].keys():
                if not realtime:
                    self.bars['ohlcv'][c.conId] = self.conn.reqHistoricalData(c, **kwargs)
                else:
                    self.bars['ohlcv'][c.conId] = self.conn.reqRealTimeBars(c, **kwargs)
                self.bars['contract'][c.conId] = c
            else:
                logger.warning('Contract is already subscribed to receive bars: %s', c)

    async def sub_bar_single(self,
                             contract,
                             realtime=False,
                             **kwargs):
        """Asynchronously subscribe to bar data for a single contract with rate limiting.

        Internal async method for subscribing to bars with semaphore-controlled concurrency.
        Used by sub_bars_async for parallel batch subscriptions.

        Args:
            contract: Single ib_async Contract object to subscribe to.
            realtime: If False (default), subscribes to historical bars (requires kwargs).
                If True, subscribes to real-time 5-second bars.
            **kwargs: Keyword arguments passed to IB.reqHistoricalDataAsync() when
                realtime=False. Required parameters: endDateTime, durationStr,
                barSizeSetting, whatToShow, useRTH, keepUpToDate, formatDate.

        Returns:
            True if subscription successful, False if already subscribed or error occurred.

        Note:
            - Uses _historical_data_sem semaphore for rate limiting
            - Automatically qualifies contract if conId=0
            - Stores bar data in self.bars['ohlcv'][conId]
            - Stores contract in self.bars['contract'][conId]
        """
        try:
            async with self._historical_data_sem:
                if contract.conId == 0:
                    await self.conn.qualifyContractsAsync(contract)

                if contract.conId in self.bars['ohlcv'].keys():
                    logger.warning('Contract is already subscribed to receive bars: %s', contract)
                    return False

                if not realtime:
                    self.bars['ohlcv'][contract.conId] = await self.conn.reqHistoricalDataAsync(
                        contract, **kwargs)
                else:
                    self.bars['ohlcv'][contract.conId] = self.conn.reqRealTimeBars(
                        contract, **kwargs)
                self.bars['contract'][contract.conId] = contract
                return True

        except Exception as e:
            logger.error(f'Failed to subscribe to bars for {contract}: {e}')
            return False

    async def sub_bars_async(self, contracts, realtime=False, **kwargs):
        """Asynchronously subscribe to bar data for multiple contracts in parallel.

        Subscribes to bar data for multiple contracts concurrently using asyncio TaskGroup.
        Rate-limited by _historical_data_sem semaphore.

        Args:
            contracts: List of ib_async Contract objects to subscribe to.
            realtime: If False (default), subscribes to historical bars (requires kwargs).
                If True, subscribes to real-time 5-second bars.
            **kwargs: Keyword arguments passed to IB.reqHistoricalDataAsync() when
                realtime=False. Required parameters: endDateTime, durationStr,
                barSizeSetting, whatToShow, useRTH, keepUpToDate, formatDate.

        Returns:
            Integer count of successfully subscribed contracts.

        Note:
            - Uses asyncio.TaskGroup for concurrent execution
            - Concurrency controlled by IB_HISTORICAL_DATA_CONCURRENCY setting
            - Logs progress: total contracts requested and successfully subscribed
            - Failed or already-subscribed contracts are skipped
        """
        logger.info(f'Subscribing to bars for {len(contracts)} contracts')

        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self.sub_bar_single(contract, realtime=realtime, **kwargs))
                for contract in contracts
            ]
        results = [task.result() for task in tasks]
        success_count = sum(results)

        logger.info(f'Successfully subscribed to {success_count}/{len(contracts)} contracts')
        return success_count

    def unsub_bars(self,
                   contract_ids=None):
        """Unsubscribe from bar data subscriptions.

        Cancels bar data subscriptions (historical or real-time) for specified contracts
        or all active subscriptions.

        Args:
            contract_ids: List of contract IDs (integers) to unsubscribe. If None (default),
                unsubscribes from all active bar subscriptions and clears self.bars.

        Note:
            - Automatically detects subscription type (RealTimeBar vs Historical)
            - Uses IB.cancelRealTimeBars() or IB.cancelHistoricalData() accordingly
            - Removes unsubscribed contracts from self.bars['ohlcv'] and self.bars['contract']
            - Safe to call even if no active subscriptions
        """
        if contract_ids:
            for cid in contract_ids:
                if not isinstance(self.bars['ohlcv'][cid][0], RealTimeBar):
                    self.conn.cancelHistoricalData(self.bars['ohlcv'][cid])
                else:
                    self.conn.cancelRealTimeBars(self.bars['ohlcv'][cid])
                del self.bars['ohlcv'][cid]
                del self.bars['contract'][cid]
        else:
            for cid in self.bars['ohlcv'].keys():
                if not isinstance(self.bars['ohlcv'][cid][0], RealTimeBar):
                    self.conn.cancelHistoricalData(self.bars['ohlcv'][cid])
                else:
                    self.conn.cancelRealTimeBars(self.bars['ohlcv'][cid])
            self.bars['ohlcv'] = {}
            self.bars['contract'] = {}

    def get_bars(
            self,
            contracts: Optional[List] = None,
            symbols: Optional[List[str]] = None,
            start_date: Optional[str | datetime | date] = None,
            end_date: Optional[str | datetime | date] = None,
            first: Optional[int] = None,
            last: Optional[int] = None,
            ohlcv: bool = True,
            allcols: bool = False
    ):
        """Retrieve bar data as DataFrame for subscribed contracts with flexible filtering.

        Retrieves and filters bar data from active subscriptions. Supports filtering by
        contract/symbol, date range, or head/tail selection. Returns data in OHLCV format
        (indexed by date and symbol) or contract format (indexed by contract and date).

        Args:
            contracts: List of contracts to retrieve. Can be contract IDs (int) or Contract
                objects. If None, retrieves all subscribed contracts. Mutually exclusive with
                symbols parameter.
            symbols: List of symbol strings to retrieve (e.g., ['AAPL', 'MSFT']). Matched
                against subscribed contracts. Mutually exclusive with contracts parameter.
            start_date: Start date for filtering (inclusive). Accepts string, datetime, or
                date objects. Converted to UTC. Mutually exclusive with first/last.
            end_date: End date for filtering (inclusive). Accepts string, datetime, or date
                objects. Converted to UTC. Mutually exclusive with first/last.
            first: Return first N bars per contract after sorting by date. Mutually exclusive
                with last and start_date/end_date.
            last: Return last N bars per contract after sorting by date. Mutually exclusive
                with first and start_date/end_date.
            ohlcv: If True (default), returns OHLCV format indexed by (date, symbol). If False,
                returns contract format indexed by (contract, date).
            allcols: If True, includes all available columns. If False (default), includes
                only standard columns (['open', 'high', 'low', 'close', 'volume', 'conId']
                for OHLCV format, or _bars_cols for contract format).

        Returns:
            DataFrame with bar data. Index and columns depend on ohlcv and allcols parameters.
            Returns empty DataFrame if no subscriptions or no matching data.

        Raises:
            Error logged if conflicting parameters specified (first/last with dates, both
            contracts and symbols, or both first and last).

        Note:
            - Date column is always UTC timezone-aware
            - Empty bar lists are skipped
            - 'time' column renamed to 'date' for real-time bars
            - 'open_' column renamed to 'open' if present
        """
        if (first is not None or last is not None) and (start_date is not None or end_date is not None):
            logger.error("Cannot use first/last with start_date/end_date")
            return pd.DataFrame()

        if first is not None and last is not None:
            logger.error("Cannot use both first and last")
            return pd.DataFrame()

        if contracts is not None and symbols is not None:
            logger.error("Cannot specify both contracts and symbols")
            return pd.DataFrame()

        if len(self.bars['ohlcv']) == 0:
            return pd.DataFrame()

        start_dt = pd.to_datetime(start_date, utc=True) if start_date is not None else None
        end_dt = pd.to_datetime(end_date, utc=True) if end_date is not None else None

        if symbols:
            symbol_to_conid = {
                self.bars['contract'][cid].symbol: cid
                for cid in self.bars['contract'].keys()
            }
            contract_ids = set([
                symbol_to_conid[s] for s in symbols
                if s in symbol_to_conid
            ])
            if not contract_ids:
                logger.warning(f"No contracts found for symbols: {symbols}")
                return pd.DataFrame()
        elif contracts:
            contract_ids = set()
            for c in contracts:
                if isinstance(c, int):
                    contract_ids.add(c)
                elif isinstance(c, Contract):
                    contract_ids.add(c.conId)
                else:
                    logger.warning(f"Invalid contract type: {type(c)}")
        else:
            contract_ids = None

        bars = []

        for conId, bar_list in self.bars['ohlcv'].items():
            if contract_ids and conId not in contract_ids:
                continue

            if len(bar_list) == 0:
                continue

            filtered_bars = bar_list

            if start_dt is not None or end_dt is not None:
                filtered_bars = []
                for bar in bar_list:
                    bar_time = bar.time if hasattr(bar, 'time') else bar.date
                    bar_time_tz = bar_time if bar_time.tzinfo else bar_time.replace(
                        tzinfo=pd.Timestamp.now(tz='UTC').tzinfo)

                    if start_dt and bar_time_tz < start_dt:
                        continue
                    if end_dt and bar_time_tz > end_dt:
                        continue
                    filtered_bars.append(bar)

                if len(filtered_bars) == 0:
                    continue

            bar_df = util.df(filtered_bars)
            if len(bar_df) == 0:
                continue

            contract = self.bars['contract'][conId]
            bar_df['contract'] = contract
            bar_df['symbol'] = contract.symbol
            bar_df['conId'] = conId

            if 'time' in bar_df.columns:
                bar_df.rename(columns={'time': 'date', 'open_': 'open'}, inplace=True)

            bars.append(bar_df)

        if not bars:
            return pd.DataFrame()

        result = pd.concat(bars, ignore_index=True)

        if pd.to_datetime(result['date']).dt.tz is None:
            result['date'] = pd.to_datetime(result['date']).dt.tz_localize('UTC')

        if first is not None or last is not None:
            result = result.sort_values(['conId', 'date'])
            grouped = []
            for contract, group in result.groupby('contract', sort=False):
                if first is not None:
                    grouped.append(group.head(first))
                else:
                    grouped.append(group.tail(last))
            result = pd.concat(grouped, ignore_index=True)

        if ohlcv:
            index_cols = ['date', 'symbol']
            if allcols:
                result = result.drop(columns=['contract']).set_index(index_cols)
            else:
                value_cols = ['open', 'high', 'low', 'close', 'volume', 'conId']
                result = result[index_cols + value_cols].set_index(index_cols)
        else:
            result = result.drop(columns=['symbol', 'conId'])
            if allcols:
                result = result.set_index(['contract', 'date'])
            else:
                result = result[self._bars_cols].set_index(['contract', 'date'])

        return result

    def _create_contracts(
            self,
            symbols: List[str],
            sec_type: SecType,
            exchange: str,
            currency: str
    ) -> List[Contract]:
        """Create unqualified IB Contract objects from symbol strings.

        Internal method for constructing Contract objects before qualification. Handles
        special cases for different security types (e.g., CASH pairs).

        Args:
            symbols: List of symbol strings (e.g., ['AAPL', 'MSFT'] or ['EURUSD']).
            sec_type: Security type ('STK', 'CASH', 'IND', 'FUT', 'CRYPTO', 'CMDTY').
            exchange: Exchange code (e.g., 'SMART', 'NYSE', 'NASDAQ'). Not used for CASH.
            currency: Currency code (e.g., 'USD', 'EUR'). For CASH, this is the quote currency.

        Returns:
            List of unqualified Contract objects (conId=0). Empty list if all contracts fail.

        Note:
            - CASH contracts are created without exchange parameter
            - Non-CASH contracts include exchange in constructor
            - Failed contracts are logged and skipped (not included in return list)
            - Contracts must be qualified before use with IB API
        """
        contracts = []

        for symbol in symbols:
            try:
                if sec_type == 'CASH':
                    contract = Contract(secType='CASH', symbol=symbol, currency=currency)
                else:
                    contract = Contract(secType=sec_type, symbol=symbol, exchange=exchange, currency=currency)

                contracts.append(contract)

            except Exception as e:
                logger.error(f"Failed to create contract for {symbol}: {e}")
                continue

        return contracts

    def symbols_to_contracts(
            self,
            symbols: List[str],
            sec_type: SecType = 'STK',
            exchange: str = 'SMART',
            currency: str = 'USD'
    ) -> List[Contract]:
        """Convert symbol strings to qualified IB Contract objects synchronously.

        Creates Contract objects from symbols and qualifies them with IB to obtain contract
        IDs and full details. Uses IB.qualifyContracts() for synchronous qualification.

        Args:
            symbols: List of symbol strings to convert (e.g., ['AAPL', 'MSFT', 'GOOGL']).
            sec_type: Security type. Defaults to 'STK' (stocks). Options: 'STK', 'CASH',
                'IND', 'FUT', 'CRYPTO', 'CMDTY'.
            exchange: Exchange code. Defaults to 'SMART' (IB smart routing). Common values:
                'NYSE', 'NASDAQ', 'CBOE', 'IDEALPRO' (for forex).
            currency: Currency code. Defaults to 'USD'. Use 'EUR', 'GBP', etc. for other
                currencies. For CASH pairs, this is the quote currency.

        Returns:
            List of qualified Contract objects with conId populated. Empty list if creation
            or qualification fails.

        Note:
            - Requires active IB connection
            - Failed symbols are logged and excluded from results
            - For async version with rate limiting, use symbols_to_contracts_async()
        """

        contracts = self._create_contracts(symbols, sec_type, exchange, currency)

        if not contracts:
            logger.warning("No contracts created from symbols")
            return []

        try:
            logger.info(f"Qualifying {len(contracts)} contracts")
            qualified = self.conn.qualifyContracts(*contracts)
            logger.info(f"Successfully qualified {len(qualified)} contracts")
            return qualified
        except Exception as e:
            logger.error(f"Failed to qualify contracts: {e}")
            return []

    async def symbols_to_contracts_async(
            self,
            symbols: List[str],
            sec_type: SecType = 'STK',
            exchange: str = 'SMART',
            currency: str = 'USD'
    ) -> List[Contract]:
        """Asynchronously convert symbol strings to qualified IB Contract objects.

        Creates Contract objects from symbols and qualifies them with IB to obtain contract
        IDs and full details. Uses IB.qualifyContractsAsync() for asynchronous qualification.

        Args:
            symbols: List of symbol strings to convert (e.g., ['AAPL', 'MSFT', 'GOOGL']).
            sec_type: Security type. Defaults to 'STK' (stocks). Options: 'STK', 'CASH',
                'IND', 'FUT', 'CRYPTO', 'CMDTY'.
            exchange: Exchange code. Defaults to 'SMART' (IB smart routing). Common values:
                'NYSE', 'NASDAQ', 'CBOE', 'IDEALPRO' (for forex).
            currency: Currency code. Defaults to 'USD'. Use 'EUR', 'GBP', etc. for other
                currencies. For CASH pairs, this is the quote currency.

        Returns:
            List of qualified Contract objects with conId populated. Empty list if creation
            or qualification fails.

        Note:
            - Requires active IB connection
            - Failed symbols are logged and excluded from results
            - Logs qualification progress
            - For synchronous version, use symbols_to_contracts()
        """

        contracts = self._create_contracts(symbols, sec_type, exchange, currency)

        if not contracts:
            logger.warning("No contracts created from symbols")
            return []

        try:
            logger.info(f"Qualifying {len(contracts)} contracts asynchronously")
            qualified = await self.conn.qualifyContractsAsync(*contracts)
            logger.info(f"Successfully qualified {len(qualified)} contracts")
            return qualified
        except Exception as e:
            logger.error(f"Failed to qualify contracts: {e}")
            return []

    def lookup_cds(self, contracts):
        """Look up and cache contract details synchronously for multiple contracts.

        Retrieves detailed contract information from IB and stores in self.contract_details.
        Uses cached values for contracts already looked up.

        Args:
            contracts: List of ib_async Contract objects to look up details for.

        Note:
            - Automatically qualifies contracts if conId=0
            - Skips contracts already in cache (logs warning)
            - Stores results in self.contract_details[conId]
            - Use get_cds() to retrieve details as DataFrame
            - For async version with rate limiting, use lookup_cds_async()
        """
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)

            if c.conId in self.contract_details.keys():
                logger.warning('Contract details were previously looked-up, using cached values: %s', c)
                continue

            self.contract_details[c.conId] = self.conn.reqContractDetails(
                Contract(conId=c.conId))

    async def lookup_cd_single(self, contract):
        """Asynchronously look up contract details for a single contract with rate limiting.

        Internal async method for retrieving contract details with semaphore-controlled
        concurrency. Used by lookup_cds_async for parallel batch operations.

        Args:
            contract: Single ib_async Contract object to look up details for.

        Returns:
            True if lookup successful, False if already cached or error occurred.

        Note:
            - Uses _ref_data_sem semaphore for rate limiting
            - Automatically qualifies contract if conId=0
            - Stores results in self.contract_details[conId]
            - Logs errors and returns False on failure
        """
        try:
            async with self._ref_data_sem:
                if contract.conId == 0:
                    contract = (await self.conn.qualifyContractsAsync(contract))[0]

                if contract.conId in self.contract_details.keys():
                    logger.warning(f'Contract details already cached: {contract}')
                    return False

                self.contract_details[contract.conId] = await self.conn.reqContractDetailsAsync(
                    Contract(conId=contract.conId))
                return True

        except Exception as e:
            logger.error(f'Failed to lookup contract details for {contract}: {e}')
            return False

    async def lookup_cds_async(self, contracts):
        """Asynchronously look up contract details for multiple contracts in parallel.

        Retrieves contract details for multiple contracts concurrently using asyncio TaskGroup.
        Rate-limited by _ref_data_sem semaphore. Stores results in self.contract_details.

        Args:
            contracts: List of ib_async Contract objects to look up details for.

        Returns:
            Integer count of successfully looked up contracts (excluding already cached).

        Note:
            - Uses asyncio.TaskGroup for concurrent execution
            - Concurrency controlled by IB_REF_DATA_CONCURRENCY setting
            - Logs progress: total contracts requested and successfully retrieved
            - Failed or already-cached contracts are skipped
            - Use get_cds() to retrieve details as DataFrame
        """
        logger.info(f'Looking up contract details for {len(contracts)} contracts')

        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self.lookup_cd_single(contract))
                for contract in contracts
            ]

        results = [task.result() for task in tasks]
        success_count = sum(results)

        logger.info(f'Successfully looked up {success_count}/{len(contracts)} contracts')
        return success_count

    def get_cds(self):
        """Retrieve cached contract details as a DataFrame.

        Converts cached contract details from self.contract_details to a pandas DataFrame
        indexed by symbol.

        Returns:
            DataFrame indexed by symbol with contract detail columns including contract
            objects, symbol, and conId. Returns empty DataFrame with 'symbol' index if
            no contract details have been looked up.

        Note:
            - Only includes contracts that have been looked up via lookup_cds() or
              lookup_cds_async()
            - Symbol and conId columns are extracted from contract objects
            - Contract objects are included in 'contract' column
        """
        if len(self.contract_details) > 0:
            cds_df = util.df([x[0] for x in self.contract_details.values()])

            cds_df['symbol'] = [x.symbol for x in cds_df.contract]
            cds_df['conId'] = [x.conId for x in cds_df.contract]

            return cds_df.set_index('symbol')
        else:
            return pd.DataFrame(columns=['symbol']).set_index('symbol')

    def subscribe_bars(
            self,
            symbols: Optional[List[str]] = None,
            contracts: Optional[List] = None,
            period: str = '1d',
            interval: str = '5m',
            what_to_show: str = 'TRADES',
            use_rth: bool = True,
            realtime: bool = False
    ) -> List[int]:
        """Subscribe to streaming bar data with automatic IB API parameter calculation.

        High-level method for subscribing to bar data using period/interval notation.
        Automatically converts chronos-lab period and interval strings to IB API parameters
        (duration, barsize) and handles contract qualification.

        Args:
            symbols: List of symbol strings to subscribe to. Mutually exclusive with contracts.
                If provided, contracts are created and qualified automatically.
            contracts: List of ib_async Contract objects to subscribe to. Mutually exclusive
                with symbols.
            period: Chronos-lab period string (e.g., '1d', '7d', '1mo', '1y'). Used to
                calculate IB duration parameter via _period() utility.
            interval: Chronos-lab interval string (e.g., '1m', '5m', '1h', '1d'). Mapped to
                IB barsize via map_interval_to_barsize().
            what_to_show: IB data type string. Defaults to 'TRADES'. Options: 'TRADES',
                'MIDPOINT', 'BID', 'ASK', 'BID_ASK', 'ADJUSTED_LAST', etc.
            use_rth: Use Regular Trading Hours only. Defaults to True. Set False for
                extended hours.
            realtime: If False (default), uses historical bars with keepUpToDate=True. If True,
                uses real-time 5-second bars via reqRealTimeBars (ignores interval parameter).

        Returns:
            List of contract IDs (integers) successfully subscribed. Empty list on failure.

        Note:
            - Automatically calculates IB API parameters from period and interval
            - Logs warning if IB API constraints require overfetching data
            - Creates and qualifies contracts if symbols provided
            - Use get_bars() to retrieve subscribed bar data
            - Use unsub_bars() to cancel subscriptions
        """
        if symbols is None and contracts is None:
            logger.error("Either symbols or contracts must be provided")
            return []

        if symbols is not None and contracts is not None:
            logger.error("Cannot specify both symbols and contracts")
            return []

        try:
            barsize, ib_params = self._prepare_subscription_params(period, interval)
        except ValueError as e:
            logger.error(f"Failed to calculate IB parameters: {str(e)}")
            return []

        if symbols is not None:
            try:
                contracts = self.symbols_to_contracts(symbols=symbols)
                if not contracts:
                    logger.error("Failed to create/qualify contracts from symbols")
                    return []
            except Exception as e:
                logger.error(f"Failed to create contracts: {str(e)}")
                return []

        try:
            if not realtime:
                self.sub_bars(
                    contracts=contracts,
                    endDateTime='',
                    durationStr=ib_params['duration_str'],
                    barSizeSetting=barsize,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    keepUpToDate=True,
                    formatDate=2,
                    realtime=False
                )
            else:
                self.sub_bars(
                    contracts=contracts,
                    barSize=5,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    realtime=True
                )

            contract_ids = [c.conId for c in contracts]
            logger.info(f"Successfully subscribed to {len(contract_ids)} contracts for streaming")
            return contract_ids

        except Exception as e:
            logger.error(f"Failed to subscribe to bars: {str(e)}")
            return []

    async def subscribe_bars_async(
            self,
            symbols: Optional[List[str]] = None,
            contracts: Optional[List] = None,
            period: str = '1d',
            interval: str = '5m',
            what_to_show: str = 'TRADES',
            use_rth: bool = True,
            realtime: bool = False
    ) -> List[int]:
        """Asynchronously subscribe to streaming bar data with automatic parameter calculation.

        High-level async method for subscribing to bar data using period/interval notation.
        Automatically converts chronos-lab period and interval strings to IB API parameters
        and handles contract qualification asynchronously with rate limiting.

        Args:
            symbols: List of symbol strings to subscribe to. Mutually exclusive with contracts.
                If provided, contracts are created and qualified asynchronously.
            contracts: List of ib_async Contract objects to subscribe to. Mutually exclusive
                with symbols.
            period: Chronos-lab period string (e.g., '1d', '7d', '1mo', '1y'). Used to
                calculate IB duration parameter via _period() utility.
            interval: Chronos-lab interval string (e.g., '1m', '5m', '1h', '1d'). Mapped to
                IB barsize via map_interval_to_barsize().
            what_to_show: IB data type string. Defaults to 'TRADES'. Options: 'TRADES',
                'MIDPOINT', 'BID', 'ASK', 'BID_ASK', 'ADJUSTED_LAST', etc.
            use_rth: Use Regular Trading Hours only. Defaults to True. Set False for
                extended hours.
            realtime: If False (default), uses historical bars with keepUpToDate=True. If True,
                uses real-time 5-second bars via reqRealTimeBars (ignores interval parameter).

        Returns:
            List of contract IDs (integers) successfully subscribed. Empty list on failure.
            Only includes contracts with successful subscriptions.

        Note:
            - Uses asyncio.TaskGroup for concurrent subscriptions
            - Concurrency controlled by IB_HISTORICAL_DATA_CONCURRENCY setting
            - Automatically calculates IB API parameters from period and interval
            - Logs warning if IB API constraints require overfetching data
            - Creates and qualifies contracts asynchronously if symbols provided
            - Logs progress: contracts requested and successfully subscribed
        """
        if symbols is None and contracts is None:
            logger.error("Either symbols or contracts must be provided")
            return []

        if symbols is not None and contracts is not None:
            logger.error("Cannot specify both symbols and contracts")
            return []

        try:
            barsize, ib_params = self._prepare_subscription_params(period, interval)
        except ValueError as e:
            logger.error(f"Failed to calculate IB parameters: {str(e)}")
            return []

        if symbols is not None:
            try:
                contracts = await self.symbols_to_contracts_async(symbols=symbols)
                if not contracts:
                    logger.error("Failed to create/qualify contracts from symbols")
                    return []
            except Exception as e:
                logger.error(f"Failed to create contracts: {str(e)}")
                return []

        try:
            if not realtime:
                success_count = await self.sub_bars_async(
                    contracts=contracts,
                    endDateTime='',
                    durationStr=ib_params['duration_str'],
                    barSizeSetting=barsize,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    keepUpToDate=True,
                    formatDate=2,
                    realtime=False
                )
            else:
                success_count = await self.sub_bars_async(
                    contracts=contracts,
                    barSize=5,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    realtime=True
                )

            contract_ids = [c.conId for c in contracts if c.conId in self.bars['ohlcv']]
            logger.info(f"Successfully subscribed to {success_count}/{len(contracts)} contracts for streaming")
            return contract_ids

        except Exception as e:
            logger.error(f"Failed to subscribe to bars: {str(e)}")
            return []

    def init(self, ib: Optional[IB] = None):
        """Initialize or verify IB connection for the singleton instance.

        Ensures the singleton has an active connection. If an IB instance is provided,
        uses it. Otherwise, attempts to connect if not already connected.

        Args:
            ib: Optional ib_async IB instance to use. If provided, sets self.conn to this
                instance. If None and not connected, attempts to connect using default
                settings.

        Returns:
            Self (IBMarketData instance) if connection successful or already established,
            None if connection fails.

        Note:
            - Used internally by get_ib() helper function
            - If ib parameter provided, assumes it's already connected
            - If not connected and no ib provided, calls self.connect()
        """
        if not self._connected:
            if isinstance(ib, IB):
                self.conn = ib
                return self
            else:
                logger.warning("Attempting to get IB but not connected")
                if not self.connect():
                    logger.error("Failed to connect to IB")
                    return None
        return self

    def _prepare_subscription_params(self, period: str, interval: str):
        """Convert chronos-lab period and interval to IB API parameters.

        Internal method that translates chronos-lab's period/interval notation to IB API's
        duration/barsize format using utility functions. Logs warnings if IB API constraints
        require overfetching data.

        Args:
            period: Chronos-lab period string (e.g., '1d', '7d', '1mo', '1y').
            interval: Chronos-lab interval string (e.g., '1m', '5m', '1h', '1d').

        Returns:
            Tuple of (barsize, ib_params):
                - barsize: IB bar size string (e.g., '1 min', '5 mins', '1 hour', '1 day')
                - ib_params: Dictionary with keys from calculate_ib_params():
                    - 'duration_str': IB duration string (e.g., '1 D', '2 W', '1 Y')
                    - 'end_datetime': End datetime for request
                    - 'effective_start': Actual start after IB API rounding
                    - 'will_overfetch': Boolean indicating if overfetch required
                    - 'overfetch_days': Number of extra days being fetched

        Raises:
            ValueError: If interval is unsupported or period exceeds IB API limits for
                the given barsize.

        Note:
            - Uses map_interval_to_barsize() for interval conversion
            - Uses _period() utility to parse period string
            - Uses calculate_ib_params() to calculate IB duration parameters
            - Logs warning if will_overfetch is True
        """
        from chronos_lab._utils import _period

        barsize = map_interval_to_barsize(interval)
        start_dt, end_dt = _period(period)
        ib_params = calculate_ib_params(start_dt, end_dt, barsize)

        if ib_params['will_overfetch']:
            logger.warning(
                f"IB API constraints require fetching {ib_params['overfetch_days']} extra days. "
                f"Requested: {start_dt.date()} to {end_dt.date()}, "
                f"will fetch from: {ib_params['effective_start'].date()}"
            )

        return barsize, ib_params


def get_ib(ib: Optional[IB] = None) -> IBMarketData:
    """Get or initialize the IBMarketData singleton instance with optional IB connection.

    Convenience function for obtaining the IBMarketData singleton and ensuring it has
    an active connection. Preferred way to access IBMarketData in application code.

    Args:
        ib: Optional ib_async IB instance to use. If provided, the singleton uses this
            connection. If None, attempts to connect using default settings if not already
            connected.

    Returns:
        IBMarketData singleton instance with active connection, or None if connection fails.

    Note:
        - Internally calls IBMarketData.get_instance().init(ib)
        - Safe to call multiple times; returns the same singleton instance
        - Connection parameters from ~/.chronos_lab/.env used if ib not provided
    """
    ibmd = IBMarketData.get_instance()
    return ibmd.init(ib=ib)


def hist_to_ohlcv(hist_data):
    """Convert historical data DataFrame to OHLCV format.

    Transforms historical data from IB format (indexed by contract, datatype, date) to
    chronos-lab OHLCV format (indexed by date, symbol).

    Args:
        hist_data: DataFrame from get_hist_data() or get_hist_data_async() with MultiIndex
            (contract, datatype, date) and columns ['open', 'high', 'low', 'close', 'volume'].

    Returns:
        DataFrame with MultiIndex (date, symbol) and columns ['open', 'high', 'low', 'close',
        'volume', 'conId']. Returns empty DataFrame with correct structure if input is empty.

    Note:
        - Extracts symbol from contract objects in index
        - Extracts conId from contract objects
        - Reorders index to (date, symbol) for chronos-lab conventions
        - Compatible with ohlcv_to_arcticdb() for storage
    """
    index_cols = ['date', 'symbol']
    value_cols = ['open', 'high', 'low', 'close', 'volume', 'conId']

    if len(hist_data) == 0:
        return pd.DataFrame(columns=index_cols + value_cols).set_index(index_cols)

    ohlcv = hist_data.reset_index()
    ohlcv['symbol'] = [x.symbol for x in ohlcv.contract]
    ohlcv['conId'] = [x.conId for x in ohlcv.contract]
    ohlcv = ohlcv[index_cols + value_cols].set_index(index_cols)

    return ohlcv


def map_interval_to_barsize(interval: str) -> str:
    """Convert chronos-lab interval string to IB API bar size string.

    Maps human-readable interval notation to Interactive Brokers API bar size format.

    Args:
        interval: Chronos-lab interval string. Supported values:
            - Seconds: '1s', '5s', '10s', '15s', '30s'
            - Minutes: '1m', '2m', '3m', '5m', '10m', '15m', '20m', '30m'
            - Hours: '1h', '2h', '3h', '4h', '8h'
            - Days: '1d'
            - Weeks: '1w', '1wk'
            - Months: '1mo'

    Returns:
        IB API bar size string (e.g., '1 min', '5 mins', '1 hour', '1 day', '1 week', '1 month').

    Raises:
        ValueError: If interval is not in the supported list.

    Note:
        - IB API uses singular 'min' for 1 minute, plural 'mins' for multiple
        - IB API uses singular 'hour' for 1 hour, plural 'hours' for multiple
        - IB API uses 'secs' (plural) even for 1 second
        - Weeks and months use singular form ('1 week', '1 month')
    """
    interval_mapping = {
        '1s': '1 secs',
        '5s': '5 secs',
        '10s': '10 secs',
        '15s': '15 secs',
        '30s': '30 secs',
        '1m': '1 min',
        '2m': '2 mins',
        '3m': '3 mins',
        '5m': '5 mins',
        '10m': '10 mins',
        '15m': '15 mins',
        '20m': '20 mins',
        '30m': '30 mins',
        '1h': '1 hour',
        '2h': '2 hours',
        '3h': '3 hours',
        '4h': '4 hours',
        '8h': '8 hours',
        '1d': '1 day',
        '1w': '1 week',
        '1wk': '1 week',
        '1mo': '1 month',
    }

    if interval not in interval_mapping:
        raise ValueError(
            f"Unsupported interval '{interval}'. Supported intervals: "
            f"{', '.join(sorted(interval_mapping.keys()))}"
        )

    return interval_mapping[interval]


def calculate_ib_params(
        start_dt: pd.Timestamp,
        end_dt: pd.Timestamp,
        barsize: str
) -> dict:
    """Calculate Interactive Brokers API parameters respecting duration-barsize constraints.

    IB API has specific limitations on the duration that can be requested for different bar sizes.
    This function calculates the optimal duration string and handles cases where over-fetching
    is necessary to satisfy API constraints.

    Args:
        start_dt: Start datetime (timezone-aware pandas Timestamp)
        end_dt: End datetime (timezone-aware pandas Timestamp)
        barsize: IB bar size string (e.g., '1 min', '1 hour', '1 day')

    Returns:
        Dictionary containing:
            - duration_str: IB API duration string (e.g., "2 Y", "365 D", "3600 S")
            - end_datetime: End datetime to pass to reqHistoricalData
            - effective_start: Actual start datetime after rounding to valid duration
            - will_overfetch: True if fetching more data than requested
            - overfetch_days: Number of extra days being fetched (0 if no overfetch)

    Raises:
        ValueError: If the requested period is invalid or exceeds API limits

    """
    from dateutil.relativedelta import relativedelta
    import math

    time_diff = end_dt - start_dt
    total_seconds = time_diff.total_seconds()

    if total_seconds <= 0:
        raise ValueError("end_dt must be after start_dt")

    # Define barsize categories and their maximum allowed durations (in seconds)
    # Based on IB API historical data limitations
    barsize_limits = {
        # Bars 30 seconds or less: max 30 minutes
        '1 secs': 1800,
        '5 secs': 1800,
        '10 secs': 1800,
        '15 secs': 1800,
        '30 secs': 1800,
        # Bars 1 minute to 30 minutes: max ~30 days
        '1 min': 2592000,  # 30 days
        '2 mins': 2592000,
        '3 mins': 2592000,
        '5 mins': 2592000,
        '10 mins': 2592000,
        '15 mins': 2592000,
        '20 mins': 2592000,
        '30 mins': 2592000,
        # Bars 1 hour to 8 hours: max ~30 days
        '1 hour': 2592000,  # 30 days
        '2 hours': 2592000,
        '3 hours': 2592000,
        '4 hours': 2592000,
        '8 hours': 2592000,
        # Daily, weekly, monthly bars: max ~1 year
        '1 day': 31536000,  # 365 days
        '1 week': 31536000,
        '1 month': 31536000,
    }

    max_duration = barsize_limits.get(barsize)
    if max_duration is None:
        raise ValueError(f"Unknown barsize '{barsize}' for IB API constraints")

    # Check if requested period exceeds the maximum for this barsize
    if total_seconds > max_duration:
        # For daily/weekly/monthly bars, we can use year units
        if barsize in ['1 day', '1 week', '1 month']:
            # Calculate how many years needed
            delta = relativedelta(end_dt, start_dt)
            years_needed = math.ceil(delta.years + (delta.months > 0 or delta.days > 0))

            duration_str = f"{years_needed} Y"
            effective_start = end_dt - pd.DateOffset(years=years_needed)

            overfetch_seconds = (effective_start - start_dt).total_seconds()
            overfetch_days = abs(int(overfetch_seconds / 86400))

            if overfetch_days > 0:
                will_overfetch = True
            else:
                will_overfetch = False

            return {
                'duration_str': duration_str,
                'end_datetime': end_dt,
                'effective_start': effective_start,
                'will_overfetch': will_overfetch,
                'overfetch_days': overfetch_days
            }
        else:
            # For intraday bars, exceeding the limit is an error
            max_days = max_duration / 86400
            requested_days = total_seconds / 86400
            raise ValueError(
                f"Requested period of {requested_days:.1f} days with barsize '{barsize}' "
                f"exceeds IB API maximum of {max_days:.1f} days. "
                f"Consider using a larger bar size or shorter time period."
            )

    # Determine optimal duration string based on time span
    # Prefer most granular unit that accurately represents the period

    days = total_seconds / 86400

    # For daily/weekly/monthly bars and periods close to a year, use year unit
    if barsize in ['1 day', '1 week', '1 month'] and days >= 360:
        duration_str = "1 Y"
        effective_start = end_dt - pd.DateOffset(years=1)
        will_overfetch = (effective_start < start_dt)
        overfetch_days = int((start_dt - effective_start).total_seconds() / 86400) if will_overfetch else 0

    # For periods >= 1 day, use days
    elif total_seconds >= 86400:
        days_int = int(math.ceil(days))
        duration_str = f"{days_int} D"
        effective_start = end_dt - pd.DateOffset(days=days_int)
        will_overfetch = False
        overfetch_days = 0

    # For periods < 1 day, use seconds
    else:
        seconds_int = int(math.ceil(total_seconds))
        duration_str = f"{seconds_int} S"
        effective_start = end_dt - pd.DateOffset(seconds=seconds_int)
        will_overfetch = False
        overfetch_days = 0

    return {
        'duration_str': duration_str,
        'end_datetime': end_dt,
        'effective_start': effective_start,
        'will_overfetch': will_overfetch,
        'overfetch_days': overfetch_days
    }
