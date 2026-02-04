from chronos_lab import logger
from chronos_lab.settings import get_settings
from typing import Optional, List, Dict, Any
import asyncio
from asyncio import Semaphore
from ib_async import IB, util, Contract, RealTimeBar
import pandas as pd

settings = get_settings()


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
                      userth=True):

        hist_data = []
        for contract in contracts:
            logger.info('Requesting historical data for %s', contract)

            hist_data_contract = util.df(self.conn.reqHistoricalData(
                contract,
                endDateTime='',
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
                                   userth=True):
        try:
            async with self._historical_data_sem:
                bars = await self.conn.reqHistoricalDataAsync(
                    contract,
                    endDateTime='',
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

    async def get_hist_data_async(self, contracts, duration, barsize, datatype, userth=True):
        logger.info(f'Requesting historical data for {len(contracts)} contracts')

        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(
                    self.get_hist_data_single(contract, duration, barsize, datatype, userth)
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

    def get_bars(self, allcols=True):
        if len(self.bars['ohlcv']) > 0:
            bars = []

            for (conId, bar) in self.bars['ohlcv'].items():
                bar_df = util.df(bar)
                if len(bar_df) == 0:
                    continue
                bar_df['contract'] = self.bars['contract'][conId]
                if 'time' in bar_df.columns:
                    bar_df.rename(columns={'time': 'date',
                                           'open_': 'open'
                                           }, inplace=True)
                bars.append(bar_df)

            if not bars:
                return pd.DataFrame()
            bars = pd.concat(bars)

            if pd.to_datetime(bars['date']).dt.tz is None:
                bars['date'] = pd.to_datetime(bars['date']).dt.tz_localize('UTC')

            if allcols:
                return bars.set_index(['contract', 'date'])
            else:
                return bars[self._bars_cols].set_index(['contract', 'date'])
        else:
            return pd.DataFrame()

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


def get_ib(ib: Optional[IB] = None) -> IBMarketData:
    ibmd = IBMarketData.get_instance()
    return ibmd.init(ib=ib)


def hist_to_ohlcv(hist_data):
    index_cols = ['date', 'symbol', 'conId']
    value_cols = ['open', 'high', 'low', 'close', 'volume']

    ohlcv = hist_data.reset_index()
    ohlcv['symbol'] = [x.symbol for x in ohlcv.contract]
    ohlcv['conId'] = [x.conId for x in ohlcv.contract]
    ohlcv = ohlcv[index_cols + value_cols].set_index(index_cols)
    ohlcv.columns = pd.MultiIndex.from_tuples(('ohlcv', i) for i in value_cols)

    return ohlcv
