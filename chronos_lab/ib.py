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
    conn: Optional[IB] = None
    _instance: Optional["IBMarketData"] = None
    _connected: bool = False

    ticks: Dict[str, Any] = {}
    _ticks_cols: List = ['time', 'symbol', 'last', 'lastSize', 'bid', 'bidSize',
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
        if not self._connected:
            logger.error('There is no active connection to IB gateway')
            return None
        else:
            if len(self.ticks) > 0:
                logger.info('Unsubscribing from ticks')
                self.unsub_ticks()
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

    def sub_ticks(self,
                  contracts,
                  gen_tick_list=''):
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)
            if c.conId not in self.ticks.keys():
                self.ticks[c.conId] = self.conn.reqMktData(c, genericTickList=gen_tick_list)
            else:
                logger.warning('Contract is already subscribed to receive ticks: %s', c)

    def unsub_ticks(self,
                    contract_ids=None):
        if contract_ids:
            for cid in contract_ids:
                self.conn.cancelMktData(self.ticks[cid].contract)
                del self.ticks[cid]
        else:
            for cid in self.ticks.keys():
                self.conn.cancelMktData(self.ticks[cid].contract)
            self.ticks = {}

    def get_ticks(self, allcols=False):
        if len(self.ticks) > 0:
            ticks_df = util.df(self.ticks.values())

            if len(ticks_df.time.dropna()) > 0:
                ticks_df.time = pd.to_datetime(ticks_df.time).dt.tz_convert('UTC')

            ticks_df['symbol'] = [x.symbol for x in ticks_df.contract]
            ticks_df['conId'] = [x.conId for x in ticks_df.contract]
            ticks_df['marketPrice'] = [self.ticks[x.conId].marketPrice() for x in ticks_df.contract]

            if allcols:
                return ticks_df.dropna(axis=1, how='all').set_index('symbol')
            else:
                return ticks_df[self._ticks_cols].set_index('symbol')
        else:
            return pd.DataFrame(columns=self._ticks_cols).set_index('symbol')

    def sub_bars(self,
                 contracts,
                 realtime=False,
                 **kwargs):
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
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)

            if c.conId in self.contract_details.keys():
                logger.warning('Contract details were previously looked-up, using cached values: %s', c)
                continue

            self.contract_details[c.conId] = self.conn.reqContractDetails(
                Contract(conId=c.conId))

    async def lookup_cd_single(self, contract):
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
    ibmd = IBMarketData.get_instance()
    return ibmd.init(ib=ib)


def hist_to_ohlcv(hist_data):
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
