import json
import asyncio
import enum
import logging
from argparse import ArgumentParser
import datetime
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
POSITIONS_INTERVAL_SEC = 2
NUM_CONTRACTS = 1
MAX_ORDERS = 2
NEW_ORDER_LOCK_SECS = 20
MSG_LENGTH_PREFIX_BYTES = 4

Order = namedtuple('Order', (
    'symbol',
    'order_type',
    'side',
    'price',
    'quantity',
    'offset',
    'tif',
    'asset_class',
    'exp',
    'strike',
    'callput'
))

SymbolState = namedtuple('SymbolState', (
    'quote',
    'timesale',
    'ma',
    'lookback',
    'price_state'
))

class PriceState(enum.Enum):
    UP = enum.auto()
    PENDING_BREAKOUT_UP = enum.auto()
    DOWN = enum.auto()
    PENDING_BREAKOUT_DOWN = enum.auto()
    UNSET = enum.auto()
    
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
        self.symbols = {symbol:{
            'quote': dict(),
            'timesale': dict(),
            'ma':dict(),
            'lookback':dict(),
            'candle':dict(),
            'price_state':PriceState.UNSET,
            'new_order_short_lock': datetime.datetime.utcnow(),
            'new_order_long_lock': datetime.datetime.utcnow()
         } for symbol in symbols}

    def generate_signal(self, symbol):
        price = self.symbols[symbol]['quote'].get('price')
        ma = self.symbols[symbol]['ma'].get('ma')
        lookback_low = self.symbols[symbol]['lookback'].get('lookback_low')
        lookback_high = self.symbols[symbol]['lookback'].get('lookback_high')
        if price is None or ma is None or lookback_low is None or lookback_high is None:
            return None
        candle = self.symbols[symbol]['candle']
        price = float(price)
        ma = float(ma)
        lookback_low = float(lookback_low)
        lookback_high = float(lookback_high)
        price_state = self.symbols[symbol]['price_state']

        #Handle candle close
        if candle is not None:
            close_price = float(candle['close'])
            if close_price > ma:
                if close_price > lookback_high:
                    self.symbols[symbol]['price_state'] = PriceState.UP
                    if price_state == PriceState.PENDING_BREAKOUT_UP:
                        return Signal.LONG
                else:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_UP
            elif close_price < ma:
                if close_price < lookback_low:
                    self.symbols[symbol]['price_state'] = PriceState.DOWN
                    if price_state == PriceState.PENDING_BREAKOUT_DOWN:
                        return Signal.SHORT
                else:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_DOWN
            self.symbols[symbol]['candle'] = None
            return
        
        #Handle price
        if price_state.is_up():
            if price_state == PriceState.UP:
                if price < lookback_high:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_UP
            else:
                if price > lookback_high:
                    self.symbols[symbol]['price_state'] = PriceState.UP
                    return Signal.LONG
        elif price_state.is_down():
            if price_state == PriceState.DOWN:
                if price > lookback_low:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_DOWN
            else:
                if price < lookback_low:
                    self.symbols[symbol]['price_state'] = PriceState.DOWN
                    return Signal.SHORT
        return Signal.HOLD

    def create_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type, exp=None, strike=None, callput=None):
        return Order(
            symbol=str(symbol),
            order_type=str(order_type),
            side=str(side),
            price=f"{price:f}",
            quantity=f"{quantity:f}",
            offset=str(offset),
            tif=str(tif),
            asset_class=str(asset_type),
            exp=str(exp) if exp is not None else None,
            strike=str(strike) if strike is not None else None,
            callput=str(callput) if callput is not None else None
        )
    
    async def decision_handler(self):
        logger.info(f"Decision Engine signal handler started")
        expiration = (datetime.datetime.today() + datetime.timedelta(days=7)).strftime('%y%m%d')
        try:
            while True:
                for symbol in self.symbols.keys():
                    signal = self.generate_signal(symbol)
                    if signal is None:
                        continue
                    price = float(self.symbols[symbol]['quote'].get('price'))
                    if symbol in self.positions: #NOTE: Long only options strategy
                        ma = float(self.symbols[symbol]['ma'].get('ma'))
                        if price < ma and self.positions[symbol]['callput'] == 'call':
                            self.orders.append(self.create_order(symbol, 'market', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'OPTION', expiration))
                        elif price > ma and self.positions[symbol]['callput'] == 'put':
                            self.orders.append(self.create_order(symbol, 'market', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'OPTION', expiration))
                    if signal == Signal.HOLD:
                        continue
                    elif signal == Signal.LONG:
                        if datetime.datetime.utcnow() < self.new_long_lock_until:
                            logger.warning(f"New long order lock in effect. Skipping signal: {signal} symbol: {symbol} state: {self.symbols[symbol]}")
                            continue
                        self.orders.append(self.create_order(symbol, 'market', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'OPTION', expiration))
                        self.new_long_lock_until = datetime.datetime.utcnow() + datetime.timedelta(seconds=NEW_ORDER_LOCK_SECS)
                    else:
                        if datetime.datetime.utcnow() < self.new_short_lock_until:
                            logger.warning(f"New short order lock in effect. Skipping signal: {signal} symbol: {symbol} state: {self.symbols[symbol]}")
                            continue
                        self.orders.append(self.create_order(symbol, 'market', 'sell', price, NUM_CONTRACTS, 'open', 'day', 'OPTION', expiration))
                        self.new_short_lock_until = datetime.datetime.utcnow() + datetime.timedelta(seconds=NEW_ORDER_LOCK_SECS)
                await asyncio.sleep(SIGNAL_PROC_INTERVAL_SEC)
        finally:
            logger.info(f"Decision Engine signal handler shutting down")

    async def order_handler(self, exchange_socket):
        try:
            while True:
                # while self.orders.count() > 0:
                #     order = self.orders.popleft()
                #     logger.info(f"Sending order: {json.dumps(order._asdict(), indent=2)}")
                #     await exchange_socket.send_json(json.dumps({
                #         'type': 'request',
                #         'channel': 'new_order',
                #         'order': {
                #             'symbol': order.symbol,
                #             'order_type': order.order_type,
                #             'side': order.side,
                #             'price': order.price,
                #             'quantity': order.quantity,
                #             'offset': order.offset,
                #             'tif': order.tif,
                #             'asset_class': order.asset_class,
                #             'exp': order.exp,
                #             'strike': order.strike,
                #             'callput': order.callput
                #         }
                #     }))
                # logger.info(f"BREAKIN")
                await asyncio.sleep(ORDER_SOCKET_INTERVAL_SEC)
        except ConnectionError as e:
            logger.error(f'Exchange socket disconnected; exchange handler orders task resetting: {e}')
    
    async def handle_exchange_msgs(self, exchange_socket):
        try:
            while True:
                # await exchange_socket.send_json(json.dumps({
                #         'type': 'subscribe',
                #         'channels': ['positions', 'orders'],
                #         'interval': POSITIONS_INTERVAL_SEC
                # }))
                # await exchange_socket.send_json(json.dumps({
                #     'type': 'request',
                #     'channel': 'balances'
                # }))
                # async for msg in exchange_socket.receive():
                #     msg = json.loads(msg)
                #     if msg['type'] == 'update':
                #         if msg['channel'] == 'positions':
                #             self.positions = msg['data']
                #         elif msg['channel'] == 'orders':
                #             logger.info(f"Received order update: {msg['data']}")
                #     elif msg['type'] == 'success':
                #         logger.info(msg)
                #     elif msg['type'] == 'error':
                #         logger.error(msg)
                #         raise Exception(msg)
                # logger.info(f"BREAKIN")
                await asyncio.sleep(ORDER_SOCKET_INTERVAL_SEC)
        except ConnectionError as e:
            logger.error(f'Exchange socket disconnected; exchange handler message task resetting: {e}')

    async def exchange_handler(self):
        while True:
            try:
                logger.info(f"Exchange Handler Starting")
                async with ContextManagedAsyncUnixSocketClient(self.exchange_socket) as exchange_socket:
                    self.exchange_tasks = {
                        asyncio.create_task(self.handle_exchange_msgs(exchange_socket), name=f'Exchange Message Handler'),
                        asyncio.create_task(self.order_handler(exchange_socket), name=f'Order Handler')
                    }
                    await asyncio.wait(self.exchange_tasks, return_when=asyncio.FIRST_COMPLETED)
            except (ConnectionRefusedError, ConnectionResetError):
                logger.error(f"Exchange Handler Unable to Connect. Resetting...")
                await asyncio.sleep(SOCKET_CONN_INTERVAL_SEC)
            finally:
                for task in self.exchange_tasks:
                    logger.info(f"Cancelling task: {task}")
                    task.cancel()
                await asyncio.gather(*self.exchange_tasks, return_exceptions=True)
                logger.info(f"Exchange Handler Shutting Down")

    async def market_data_handler(self):
        while True:
            try:
                logger.info(f"Market Data Handler Starting")
                async with ContextManagedAsyncUnixSocketClient(self.market_data_socket) as md_socket:
                    await md_socket.send_json(json.dumps({
                        'type': 'subscribe',
                        'channels': ['quote', 'candle'], #, 'timesale', 'ma', 'lookback'],
                        'symbols': list(self.symbols.keys())
                    }))
                    async for msg in md_socket.receive():
                        msg = json.loads(msg)
                        logger.info(f"Received message: {msg['channel']}")
                        if msg['type'] == 'update' and msg['symbol'] in self.symbols:
                            if msg['channel'] == 'quote':
                                self.symbols[msg['symbol']]['quote'] = msg['data']
                            elif msg['channel'] == 'timesale':
                                self.symbols[msg['symbol']]['timesale'] = msg['data']
                            elif msg['channel'] == 'ma':
                                self.symbols[msg['symbol']]['ma'] = msg['data']
                            elif msg['channel'] == 'lookback':
                                self.symbols[msg['symbol']]['lookback'] = msg['data']
                            elif msg['channel'] == 'candle':
                                self.symbols[msg['symbol']]['candle'] = msg['data']
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
                        'interval': POSITIONS_INTERVAL_SEC
                    }))
                    await exchange.send_json(json.dumps({
                        'type': 'request',
                        'channel': 'balances'
                    }))
                    async for msg in exchange.receive():
                        msg = json.loads(msg)
                        logger.info(f"Received response: {json.dumps(msg, indent=2)}")
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
                    asyncio.create_task(self.decision_handler(), name=f'Order Handler'),
                    asyncio.create_task(self.market_data_handler(), name=f'Market Data Handler'),
                    asyncio.create_task(self.exchange_handler(), name=f'Exchange Handler')
                }
                await asyncio.wait(decision_engine_tasks, return_when=asyncio.FIRST_COMPLETED)
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