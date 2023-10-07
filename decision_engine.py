import json
import asyncio
import enum
import logging
from argparse import ArgumentParser
from datetime import datetime
from collections import namedtuple, deque
from async_unix_socket import ContextManagedAsyncUnixSocketClient

#logger = Logger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
    handlers=[
        #logging.FileHandler("path.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

ORDER_SOCKET_INTERVAL_SEC = 2
SOCKET_CONN_INTERVAL_SEC = 2
SIGNAL_PROC_INTERVAL_SEC = 1
NUM_CONTRACTS = 1
MAX_ORDERS = 5
MSG_LENGTH_PREFIX_BYTES = 4

Order = namedtuple('Order', (
    'symbol',
    'order_type',
    'side',
    'price',
    'quantity',
    'offset',
    'tif',
    'asset_type',
))

class PriceState(enum.Enum):
    UP = enum.auto()
    PENDING_BREAKOUT_UP = enum.auto()
    DOWN = enum.auto()
    PENDING_BREAKOUT_DOWN = enum.auto()
    UNSET = enum.auto()

    def is_pending_breakout_up(self):
        return self == PriceState.PENDING_BREAKOUT_UP
    
    def is_pending_breakout_down(self):
        return self == PriceState.PENDING_BREAKOUT_DOWN
    
    def is_up(self):
        return self in (PriceState.UP, PriceState.PENDING_BREAKOUT_UP)
    
    def is_down(self):
        return self in (PriceState.DOWN, PriceState.PENDING_BREAKOUT_DOWN)
    
class Signal(enum.Enum):    
    LONG = enum.auto()
    SHORT = enum.auto()
    HOLD = enum.auto()

class DecisionEngine():
    def __init__(self, market_data_socket, exchange_socket, symbols):
        self.market_data_socket = market_data_socket
        self.exchange_socket = exchange_socket
        self.positions = dict()
        self.orders = deque(maxlen=MAX_ORDERS)
        self.quotes = dict()
        self.timesale = dict()
        self.mas = dict()
        self.lookback = dict()
        self.symbols = symbols
        self.price_state = PriceState.UNSET

    def generate_signal(self, symbol):
        price = self.quotes[symbol]['price']
        ma = self.mas[symbol]['ma']
        lookback = self.lookback[symbol]
        signal = Signal.HOLD
        if price > ma:
            if price > lookback['lookback_high']:
                if self.price_state.is_pending_breakout_up():
                    signal = Signal.LONG
                self.price_state = PriceState.UP
            else:
                self.price_state = PriceState.PENDING_BREAKOUT_UP
        elif price < ma:
            if price < lookback['lookback_low']:
                if self.price_state.is_pending_breakout_down():
                    signal =  Signal.SHORT 
                self.price_state = PriceState.DOWN
            else:
                self.price_state = PriceState.PENDING_BREAKOUT_DOWN
        else:
            self.price_state = PriceState.UNSET
        return signal

    # def create_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type):
    #     return Order(
    #         symbol=str(symbol),
    #         order_type=str(order_type),
    #         side=str(side),
    #         price=f"{price:f}",
    #         quantity=f"{quantity:f}",
    #         offset=str(offset),
    #         tif=str(tif),
    #         asset_type=str(asset_type)
    #     )
    
    # async def signal_handler(self):
    #     #TODO: This needs some work. Needs to handle every case/scenario. Currently, it does not.
    #     logger.info(f"Decision Engine signal handler started")
    #     try:
    #         while True:
    #             for symbol in self.symbols:
    #                 if symbol not in self.quotes or symbol not in self.timesale or symbol not in self.mas or symbol not in self.lookback:
    #                     continue
                    
    #                 signal = self.generate_signal(symbol)
    #                 if signal == Signal.HOLD:
    #                     continue

    #                 price = self.quotes[symbol]
    #                 if signal == Signal.LONG:
    #                     if symbol not in self.positions:
    #                         self.orders.append(self.create_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'OPTION'))
    #                     elif self.positions[symbol]['side'] == 'short':
    #                         self.orders.append(self.create_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'close', 'day', 'OPTION'))
    #                 else:
    #                     if symbol not in self.positions:
    #                         self.orders.append(self.create_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'open', 'day', 'OPTION'))
    #                     elif self.positions[symbol]['side'] == 'long':
    #                         self.orders.append(self.create_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'OPTION'))
    #             asyncio.sleep(SIGNAL_PROC_INTERVAL_SEC)
    #     finally:
    #         logger.info(f"Decision Engine signal handler shutting down")

    # async def send_orders(self, exchange_socket):
    #     try:
    #         while True:
    #             while self.orders.count() > 0:
    #                 order = self.orders.pop()
    #                 logger.info(f"Sending order: {order}")
    #                 await exchange_socket.send_str(json.dumps({
    #                     'type': 'request',
    #                     'channel': 'new_order',
    #                     'order': {
    #                         'symbol': order.symbol,
    #                         'order_type': order.order_type,
    #                         'side': order.side,
    #                         'price': order.price,
    #                         'quantity': order.quantity,
    #                         'offset': order.offset,
    #                         'tif': order.tif,
    #                         'asset_type': order.asset_type
    #                     }
    #                 }))
    #             asyncio.sleep(ORDER_SOCKET_INTERVAL_SEC)
    #     except ConnectionError as e:
    #         logger.error(f'Exchange socket disconnected; exchange handler orders task resetting: {e}')
    
    # async def handle_exchange_msgs(self, exchange_socket):
    #     try:
    #         while True:
    #             exchange_socket.send_str(json.dumps({
    #                         'type': 'subscribe',
    #                         'channel': 'positions',
    #                         'interval': '10'
    #                     }))
    #             logger.info(f"Subscribed to positions channel")
    #             async for msg in exchange_socket:
    #                 msg = json.loads(msg)
    #                 if msg['type'] == 'update':
    #                     if msg['channel'] == 'positions':
    #                         self.positions[msg['symbol']] = msg['data']
    #                 elif msg['type'] == 'success':
    #                     logger.info(msg)
    #                 elif msg['type'] == 'error':
    #                     logger.error(msg)
    #                     break
    #     except ConnectionError as e:
    #         logger.error(f'Exchange socket disconnected; exchange handler message task resetting: {e}')

    async def exchange_handler(self):
        while True:
            try:
                logger.info(f"Exchange Handler Starting")
                async with ContextManagedAsyncUnixSocketClient(self.exchange_socket) as exchange:
                    exchange_tasks = set()
                    exchange_tasks.add(asyncio.create_task(self.handle_exchange_msgs(exchange)).add_done_callback(exchange_tasks.discard))
                    exchange_tasks.add(asyncio.create_task(self.send_orders(exchange)).add_done_callback(exchange_tasks.discard))
                    _, pending = await asyncio.wait(exchange_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
            except (ConnectionRefusedError, ConnectionResetError):
                logger.error(f"Exchange Handler Unable to Connect. Resetting...")
                await asyncio.sleep(SOCKET_CONN_INTERVAL_SEC)
            finally:
                for task in exchange_tasks:
                    task.cancel()
                await asyncio.gather(*exchange_tasks, return_exceptions=True)
                logger.info(f"Exchange Handler Shutting Down")

    async def market_data_handler(self):
        while True:
            try:
                logger.info(f"Market Data Handler Starting")
                async with ContextManagedAsyncUnixSocketClient(self.market_data_socket) as md_socket:
                    await md_socket.send_json(json.dumps({
                        'type': 'subscribe',
                        'channels': ['quote', 'timesale', 'ma', 'lookback'],
                        'symbols': list(self.symbols)
                    }))
                    async for msg in md_socket.receive():
                        msg = json.loads(msg)
                        #logger.info(f"Received message: {json.dumps(msg['channel'], indent=2)}")
                        if msg['type'] == 'update':
                            if msg['channel'] == 'quote':
                                self.quotes[msg['symbol']] = msg['data']
                            elif msg['channel'] == 'timesale':
                                self.timesale[msg['symbol']] = msg['data']
                            elif msg['channel'] == 'ma':
                                self.mas[msg['symbol']] = msg['data']
                            elif msg['channel'] == 'lookback':
                                logger.info(f"Received lookback: {json.dumps(msg, indent=2)}")
                                self.lookback[msg['symbol']] = msg['data']
                            else:
                                logger.warning(f"Unhandled message type: {msg}")
                        elif msg['type'] == 'response':
                            continue
                        elif msg['type'] == 'error':
                            logger.error(msg)
                            break
                logger.info(f"Market Data Handler Shutting Down")
            except (ConnectionRefusedError, ConnectionResetError):
                logger.error(f"Market Data Handler Unable to Connect. Resetting...")
                await asyncio.sleep(SOCKET_CONN_INTERVAL_SEC)

    async def test_exchange_handler(self):
        while True:
            try:
                logger.info(f"Exchange Handler Starting")
                async with ContextManagedAsyncUnixSocketClient(self.exchange_socket) as exchange:
                    await exchange.send_json(json.dumps({
                        'type': 'subscribe',
                        'channels': ['positions', 'orders'],
                        'interval': '4'
                    }))
                    await exchange.send_json(json.dumps({
                        'type': 'request',
                        'channel': 'balances'
                    }))
                    async for msg in exchange.receive():
                        logger.info(f"Received response: {msg}")
                logger.info(f"Exchange Handler Shutting Down")
            except (ConnectionRefusedError, ConnectionResetError) as e:
                logger.error(f"Exchange Handler Unable to Connect. Resetting...")
                await asyncio.sleep(SOCKET_CONN_INTERVAL_SEC)

    async def decision_engine_main(self):
        while True:
            try:
                logger.info("Decision Engine Starting")
                self.running = True
                decision_engine_tasks = {
                                        #asyncio.create_task(self.signal_handler(), name=f'Signal Handler'),
                                        asyncio.create_task(self.market_data_handler(), name=f'Market Data Handler'),
                                        #asyncio.create_task(self.test_exchange_handler(), name=f'Exchange Handler')
                                    }
                await asyncio.gather(*decision_engine_tasks)
                # while self.running:
                #     done, _ = await asyncio.wait(decision_engine_tasks, return_when=asyncio.FIRST_COMPLETED)
                #     for task in done:
                #         decision_engine_tasks.add(asyncio.create_task(task.get_coro()))
            finally:
                logger.info("Decision Engine Shutting Down")
                for task in decision_engine_tasks:
                    logger.info(f"Cancelling task: {task}")
                    task.cancel()
                await asyncio.gather(*decision_engine_tasks, return_exceptions=True)

    def shutdown(self):
        self.running = False

def main():
    parser = ArgumentParser()
    decision_engine = parser.add_argument_group("Decision Engine", "Decision Engine parameters")
    decision_engine.add_argument('--market-data-socket', type=str, default='/tmp/md_server.sock', help="Path to data provider unix domain socket")
    decision_engine.add_argument('--exchange-socket', type=str, default='/tmp/exchange.sock', help="Path to order router unix domain socket")
    decision_engine.add_argument('--symbols', type=str.upper,  nargs='*', help="Symbols to trade")
    args = parser.parse_args()
    args = vars(args)
    decision_engine = DecisionEngine(**args)
    asyncio.run(decision_engine.decision_engine_main())

if __name__ == '__main__':
    main()