from subprocess import call
from brokers.base import DataStream, DataStreamError
from brokers.interactivebrokers.base import InteractiveBrokersDispatch
from _utils.val import val_instance
from ib_insync import *
import ib_insync
from typing import Any, Callable, Iterable
from types import NoneType
import datetime as dt


class InteractiveBrokersDataStream(DataStream):
    @property
    def run(self) -> bool:
        return self._run

    @run.setter
    def run(self, _run: bool) -> None:
        val_instance(_run, bool)

        self.run = _run

    @run.deleter
    def run(self) -> None:
        self._run = False

    @property
    def contract(self) -> list[Contract] | NoneType:
        return self._contracts

    @contract.setter
    def contract(self, _contracts: Contract | Iterable[Contract] | NoneType) -> Contract | Iterable[Contract] | NoneType:
        if self._run:
            if _contracts is None:
                raise ValueError(
                    f"can't update `contract` to None while running.")

        if not _contracts is None:
            if isinstance(_contracts, Contract):
                self._contracts = [_contracts]
            else:
                try:
                    iter(_contracts)
                except TypeError:
                    raise TypeError(
                        f"if `_contracts` is not None or of type `Contract` _contracts must be Iterable.")

                if not all(isinstance(__o, Contract) for __o in _contracts):
                    raise TypeError(
                        f"if `_contracts` is Iterable it must only contain object of type `Contract`")

                self._contracts = _contracts

            self._contracts = self.__ib.qualifyContracts(*self._contracts)
        else:
            self._contracts = None

    @contract.deleter
    def contract(self) -> None:
        self._contracts = None

    @property
    def callback(self) -> Callable | NoneType:
        return self._callback

    @callback.setter
    def callback(self, _callback: Callable | NoneType) -> None:
        if self._run:
            if _callback is None:
                raise ValueError(
                    f"can't update `callback` to None while running.")

        if not _callback is None:
            if not callable(_callback):
                raise TypeError(
                    f"`_callback` must be callable. got {type(_callback)}.")

        self._callback = _callback

    @callback.deleter
    def callback(self) -> None:
        self._callback = None

    def __init__(self, contract: Contract | Iterable[Contract] | NoneType = None, callback: Callable | NoneType = None,
                 host: str = "127.0.0.1", port: int = 7497, timeout: float = 4, account: str = "") -> None:
        """
        Create a DataStream connected to the Interactive brokers web socket.

        DataStream requires connection to Interactive Brokers (IB) TWS (Trader Workstation) or IB Gateway (IBG)
        API through the `ib_insync` Python Library. Which makes this a WebSocket application since
        it communicates through localhost with IB TWS or IBG.
        Therefore it is a requirement to have one of these applications installed and set up.
        Interactive Brokers Gateway : https://www.interactivebrokers.com/en/trading/ibgateway-stable.php
        Interactive Brokers Trader Workstation: https://www.interactivebrokers.com/en/trading/tws-updateable-latest.php
        The Data Stream IB object will have a fixed clientId of 1001 (every IB connection must have a uniqueId).
        The Broker IB object will have a fixed clientId of 1002.
        The DataStream IB object will be readonly meaning that orders can't be placed out of this connection.
        port=7497 will be used for Paper trading while
        port=7496 will be used for Live trading.

        Args:
            contract (Contract | Iterable[Contract] | NoneType, optional): _description_. Defaults to None.
            callback (Callable | NoneType, optional): _description_. Defaults to None.
            host (str, optional): _description_. Defaults to "127.0.0.1".
            port (int, optional): _description_. Defaults to 7497.
            timeout (float, optional): _description_. Defaults to 4.
            account (str, optional): _description_. Defaults to "".
        """

        val_instance(host, str)
        val_instance(port, int)
        val_instance(timeout, (float, int))
        val_instance(account, str)

        self.__dispatch = InteractiveBrokersDispatch

        self.__ib = ib_insync.IB()

        # connect to IB object
        self.__uid = self.__dispatch.allocate_uid()
        self.__ib.connect(host=host, port=port, clientId=self.__uid,
                          timeout=timeout, readonly=True, account=account)

        # set marketDataType request to `live`
        self.__ib.reqMarketDataType(marketDataType=1)

        self._run: bool = False
        self._contracts: list[Contract] | NoneType = None
        self._callback: Callable | NoneType = None

        # initialize class objects
        self.contract: list[Contract] | NoneType = contract
        self.callback: Callable | NoneType = callback

    def update_contract(self, contract: Contract | Iterable[Contract] | NoneType) -> None:
        """
        Update the contract.

        The contract or contracts to be requested from interactive brokers.

        '''
        tickers = InteractiveBrokers.request(contracts)

        callback(*tickers)
        '''

        Args:
            callback (Contract | Iterable[Contract] | NoneType): Contract, iterable of Contracts or None.
        """

        self.contract = contract

    def update_callback(self, callback: Callable | NoneType) -> None:
        """
        Update the callback.

        A callable function which will be called with the returned tickers from the request.

        '''
        tickers = InteractiveBrokers.request(contracts)

        callback(*tickers)
        '''

        Args:
            callback (Callable | NoneType): Callback.
        """

        self.callback = callback

    def stream(self) -> None:
        """
        Run the DataStream which requests the specified contracts from the Interactive Brokers socket
        and passes them to the callback function.

        Full DataStream will call the callback function with every responded request.

        '''
        tickers = InteractiveBrokers.request(contracts)

        callback(*tickers)
        '''

        `InteractiveBrokersDataStream.contract` and `InteractiveBrokersDataStream.callback` have to be
        defined.
        """

        if self._run:
            raise DataStreamError(
                f"a signle DataStream object can't have multiple streams. initialize a new DataStream object.")

        if self.contract is None:
            raise ValueError(
                f"`contract` must be defined before starting the data stream.")

        if self.callback is None:
            raise ValueError(
                f"`callback` must be defined before starting the data stream.")

        self._run: bool = True

        while self._run:
            _tickers = self.__ib.reqTickers(*self.contract)

            self.callback(*_tickers)
    
    def start_drip_stream(self) -> None:
        """
        Run the DataStream which requests the specified contracts from the Interactive Brokers socket
        and passes them to the callback function.
        
        Full DataStream will call the callback function with the first response of the second.
        
        '''
        tickers = InteractiveBrokers.request(contracts)
        
        if Condition:
            callback(*tickers)
        '''
        
        `InteractiveBrokersDataStream.contract` and `InteractiveBrokersDataStream.callback` have to be
        defined.
        """

        if self._run:
            raise DataStreamError(f"a signle DataStream object can't have multiple streams. initialize a new DataStream object.")

        if self.contract is None:
            raise ValueError(
                f"`contract` must be defined before starting the data stream.")

        if self.callback is None:
            raise ValueError(
                f"`callback` must be defined before starting the data stream.")

        self._run: bool = True

        _last: dt.datetime = dt.datetime.now().replace(microsecond=0)
        while self._run:
            _now: dt.datetime = dt.datetime.now().replace(microsecond=0)
            
            if _now > _last:
                _tickers = self.__ib.reqTickers(*self.contract)
                
                self.callback(*_tickers)
            
            _last = _now
            
    def pause(self) -> None:
        """
        Pause the data stream.
        """

        self.run = False

    def __del__(self):
        """
        Free the UID being used by the DataStream and disconnect from the Interactive Brokers socket.
        """

        self.__dispatch.free_uid(self.__uid)
        self.__ib.disconnect()
