from chronos_lab import logger
from chronos_lab.settings import get_settings
from typing import Optional, List, Dict, Any
from asyncio import Semaphore
from ib_async import IB, util, Contract
import pandas as pd

settings = get_settings()


class IBMarketData:
    conn: Optional[IB] = None
    _instance: Optional["IBMarketData"] = None
    _connected: bool = False

    tickers: Dict[str, Any] = {}
    _tickers_cols: List = ['time', 'ticker', 'last', 'lastSize', 'bid', 'bidSize',
                           'ask', 'askSize', 'open', 'high', 'low', 'close', 'conId', 'marketPrice']
    bars: Dict[str, Any] = {
        'ohlcv': {},
        'contract': {}
    }
    _bars_cols: List = ['contract', 'date', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount']

    contract_details: Dict[str, Any] = {}
    gen_tick_list: str = '104, 106, 165, 221, 411'

    _sem_1: Semaphore = Semaphore(20)
    _sem_2 = Semaphore = Semaphore(20)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
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
            logger.info('Disconnecting from IB gateway')
            return self.conn.disconnect()

    def get_hist_data(self,
                      contracts,
                      duration,
                      barsize,
                      datatype,
                      userth=True):

        hist_data = [pd.DataFrame()]
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
                continue

            hist_data_contract['date'] = pd.to_datetime(hist_data_contract['date'], utc=True)
            hist_data_contract['datatype'] = datatype
            hist_data_contract['contract'] = contract
            hist_data_contract['barsize'] = barsize

            hist_data.append(hist_data_contract.set_index(['contract', 'datatype', 'date']))

        return pd.concat(hist_data)

    async def get_hist_data_async(self,
                                  contract,
                                  duration,
                                  barsize,
                                  datatype,
                                  userth=True):
        async with self._sem_2:
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
                return pd.DataFrame()
            else:
                hist_data_contract['date'] = pd.to_datetime(hist_data_contract['date'], utc=True)
                hist_data_contract['datatype'] = datatype
                hist_data_contract['contract'] = contract
                hist_data_contract['barsize'] = barsize

                return hist_data_contract

    def sub_tickers(self,
                    contracts,
                    gen_tick_list=''):
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)
            if c.conId not in self.tickers.keys():
                self.tickers[c.conId] = self.conn.reqMktData(c, genericTickList=gen_tick_list)
            else:
                logger.warning('Contract is already subscribed to receive ticks: %s', c)

    def unsub_tickers(self,
                      contract_ids=None):
        if contract_ids:
            for cid in contract_ids:
                self.conn.cancelMktData(self.tickers[cid].contract)
                del self.tickers[cid]
        else:
            for cid in self.tickers.keys():
                self.conn.cancelMktData(self.tickers[cid].contract)
            self.tickers = {}

    def get_tickers(self, allcols=False):
        if len(self.tickers) > 0:
            ticks_df = util.df(self.tickers.values())

            if len(ticks_df.time.dropna()) > 0:
                ticks_df.time = pd.to_datetime(ticks_df.time).dt.tz_convert('UTC')

            ticks_df['ticker'] = [x.symbol for x in ticks_df.contract]
            ticks_df['conId'] = [x.conId for x in ticks_df.contract]
            ticks_df['marketPrice'] = [self.tickers[x.conId].marketPrice() for x in ticks_df.contract]

            if allcols:
                return ticks_df.dropna(axis=1, how='all').set_index('ticker')
            else:
                return ticks_df[self._tickers_cols].set_index('ticker')
        else:
            return pd.DataFrame(columns=self._tickers_cols).set_index('ticker')

    def sub_bars(self,
                 contracts,
                 **kwargs):
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)
            if c.conId not in self.bars['ohlcv'].keys():
                self.bars['ohlcv'][c.conId] = self.conn.reqHistoricalData(c, **kwargs)
                self.bars['contract'][c.conId] = c
            else:
                logger.warning('Contract is already subscribed to receive bars: %s', c)

    async def sub_bar(self,
                      contract,
                      **kwargs):

        async with self._sem_1:
            if contract.conId == 0:
                await self.conn.qualifyContractsAsync(contract)
            if contract.conId not in self.bars['ohlcv'].keys():
                self.bars['ohlcv'][contract.conId] = await self.conn.reqHistoricalDataAsync(
                    contract, **kwargs)
                self.bars['contract'][contract.conId] = contract
            else:
                logger.warning('Contract is already subscribed to receive bars: %s', contract)

    def unsub_bars(self,
                   contract_ids=None):
        if contract_ids:
            for cid in contract_ids:
                self.conn.cancelHistoricalData(self.bars['ohlcv'][cid])
                del self.bars['ohlcv'][cid]
                del self.bars['contract'][cid]
        else:
            for cid in self.bars['ohlcv'].keys():
                self.conn.cancelHistoricalData(self.bars['ohlcv'][cid])
            self.bars['ohlcv'] = {}
            self.bars['contract'] = {}

    def get_bars(self, allcols=False):
        if len(self.bars) > 0:
            bars = []

            for (conId, bar) in self.bars['ohlcv'].items():
                bar_df = util.df(bar)
                bar_df['contract'] = self.bars['contract'][conId]
                bars.append(bar_df)
            bars = pd.concat(bars)
            if pd.to_datetime(bars['date']).dt.tz is None:
                bars['date'] = pd.to_datetime(bars['date']).dt.tz_localize('UTC')

            if allcols:
                return bars.dropna(axis=1, how='all').set_index(['contract', 'date'])
            else:
                return bars[self._bars_cols].set_index(['contract', 'date'])
        else:
            return pd.DataFrame(columns=self._bars_cols).set_index(['contract', 'date'])

    def lookup_cds(self, contracts):
        for c in contracts:
            if c.conId == 0:
                self.conn.qualifyContracts(c)

            if c.conId in self.contract_details.keys():
                logger.warning('Contract details were previously looked-up, using cached values: %s', c)
                continue

            self.contract_details[c.conId] = self.conn.reqContractDetails(
                Contract(conId=c.conId))

    async def lookup_cds_async(self, c):
        async with self._sem_1:
            if c.conId == 0:
                c = (await self.conn.qualifyContractsAsync(c))[0]

            if c.conId not in self.contract_details.keys():
                self.contract_details[c.conId] = await self.conn.reqContractDetailsAsync(
                    Contract(conId=c.conId))

    def get_cds(self):
        if len(self.contract_details) > 0:
            cds_df = util.df([x[0] for x in self.contract_details.values()])

            cds_df['ticker'] = [x.symbol for x in cds_df.contract]
            cds_df['conId'] = [x.conId for x in cds_df.contract]

            return cds_df.set_index('ticker')
        else:
            return pd.DataFrame(columns=['ticker']).set_index('ticker')

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
    index_cols = ['date', 'ticker', 'conId']
    value_cols = ['open', 'high', 'low', 'close', 'volume']

    ohlcv = hist_data.reset_index()
    ohlcv['ticker'] = [x.symbol for x in ohlcv.contract]
    ohlcv['conId'] = [x.conId for x in ohlcv.contract]
    ohlcv = ohlcv[index_cols + value_cols].set_index(index_cols)
    ohlcv.columns = pd.MultiIndex.from_tuples(('ohlcv', i) for i in value_cols)

    return ohlcv
