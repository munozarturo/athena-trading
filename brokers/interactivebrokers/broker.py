from collections import defaultdict
from _utils.errors import ReadOnlyPropertyError
from brokers.base import Broker
from brokers.interactivebrokers.base import InteractiveBrokersDispatch
from _utils.val import val_instance
from ib_insync import *
import ib_insync


# todo: Finish documentation
# todo: Fix UIDs

class InteractiveBrokersBroker(Broker):
    def getib(self) -> IB:
        return self.__ib
    
    def get_trades(self) -> list[Trade]:
        """
        Get all trades issues this session.

        Returns:
            list[Trade]: trades.
        """
        
        return self.__trades

    @property
    def trades(self) -> list[Trade]:
        return self.__trades

    @trades.setter
    def trades(self, _trades: None) -> None:
        raise ReadOnlyPropertyError(f"`trades` is a read-only property.")

    @trades.deleter
    def trades(self) -> None:
        raise ReadOnlyPropertyError(f"`trades` is a read-only property.")
    
    def get_account_summary(self) -> dict:
        """
        Get account summary.

        Returns:
            dict: account summary.
        """
        
        return self.account_summary
    
    @property
    def account_summary(self) -> dict:
        _values = self.__ib.accountSummary()
        
        summary: dict = defaultdict(dict)
        for value in _values:
            _value = f"{value.value}"
            
            if value.currency != "":
                _value += f" {value.currency}"
        
            if value.modelCode != "":
                _value += f" {value.modelCode}"
            
            summary[value.account][value.tag] = f"{_value}"
    
        return summary
    
    @account_summary.setter
    def account_summary(self, _account_summary: list[AccountValue]) -> None:
        raise ReadOnlyPropertyError(f"`account_summary` is a read-only property.")
    
    @account_summary.deleter
    def account_summary(self) -> None:
        raise ReadOnlyPropertyError(f"`account_summary` is a read-only property.")

    def __init__(self, host: str = "127.0.0.1", port: int = 7497, timeout: float = 4, account: str = "") -> None:
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

        self.__trades: list[Trade] = []

    def place_order(self, contract: Contract, order: Order) -> Trade:
        val_instance(contract, Contract)
        val_instance(order, Order)

        _trade = self.__ib.placeOrder(contract, order)
        self.__trades.append(_trade)

        return _trade

    def cancel_order(self, order: Order) -> Trade:
        val_instance(order, Order)

        _trade = self.__ib.cancelOrder(order)
        self.__trades.remove(_trade)

        return _trade

    def __del__(self):
        """
        Free the UID being used by the Broker and disconnect from the Interactive Brokers socket.
        """
        self.__dispatch.free_uid(self.__uid)
        self.__ib.disconnect()
