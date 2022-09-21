from multiprocessing.sharedctypes import Value
from _utils.time import get_city_timezone
from brokers.interactivebrokers import *

broker = InteractiveBrokersBroker(host="127.0.0.1", port=7497, timeout=4)

def strategy_wrapper(*tickers: Ticker) -> None:
    _order = MarketOrder("BUY", 500)
    broker.place_order(QQQ_USD_SMART, _order)
    
    _order = MarketOrder("SELL", 500)
    broker.place_order(QQQ_USD_SMART, _order)

stream = InteractiveBrokersDataStream([DJI_USD_CME, QQQ_USD_SMART], strategy_wrapper, host="127.0.0.1", port=7497, timeout=4)
stream.start_drip_stream()