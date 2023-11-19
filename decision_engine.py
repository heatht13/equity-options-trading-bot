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
SIGNAL_PROC_INTERVAL_SEC = 0.1
POSITIONS_INTERVAL_SEC = 2
NUM_CONTRACTS = 1
MAX_ORDERS = 2
NEW_ORDER_LOCK_SECS = 20
MSG_LENGTH_PREFIX_BYTES = 4
DAYS_TO_EXPIRATION = 7
MAX_OPTION_PRICE = 1.5
MIN_OPTION_PRICE = 0.4

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
    
class Error(Exception):
    pass
    
class Signal(enum.Enum):    
    LONG = enum.auto()
    SHORT = enum.auto()
    HOLD = enum.auto()

class DecisionEngine():
    def __init__(self, market_data_socket, exchange_socket, symbols):
        self.market_data_socket = market_data_socket
        self.exchange_socket = exchange_socket
        self.expiration = (datetime.datetime.today() + datetime.timedelta(days=DAYS_TO_EXPIRATION)).strftime('%y%m%d')
        self.options_chain = dict()
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
        self.exchange_tasks = set()

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
            logger.info(f"Handling candle close: {candle}")
            close_price = float(candle['close'])
            if close_price > ma:
                if close_price > lookback_high:
                    self.symbols[symbol]['price_state'] = PriceState.UP
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
                    if price_state == PriceState.PENDING_BREAKOUT_UP:
                        return Signal.LONG
                else:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_UP
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
            elif close_price < ma:
                if close_price < lookback_low:
                    self.symbols[symbol]['price_state'] = PriceState.DOWN
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
                    if price_state == PriceState.PENDING_BREAKOUT_DOWN:
                        return Signal.SHORT
                else:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_DOWN
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
            self.symbols[symbol]['candle'] = None
            return None
        
        #Handle price
        if price_state.is_up():
            if price_state == PriceState.UP:
                if price < lookback_high:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_UP
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
            else:
                if price > lookback_high:
                    self.symbols[symbol]['price_state'] = PriceState.UP
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
                    return Signal.LONG
        elif price_state.is_down():
            if price_state == PriceState.DOWN:
                if price > lookback_low:
                    self.symbols[symbol]['price_state'] = PriceState.PENDING_BREAKOUT_DOWN
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
            else:
                if price < lookback_low:
                    self.symbols[symbol]['price_state'] = PriceState.DOWN
                    logger.info(f"Setting Symbol PriceState: {symbol} State: {self.symbols[symbol]['price_state']}")
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
        #TODO Define Strike price
        #TODO: Create tasks for each symbol
        try:
            while True:
                for symbol in self.symbols.keys():
                    signal = self.generate_signal(symbol)
                    if signal is None:
                        continue
                    #TODO: this only allows us to have 1 open position at a time for SPY. Need to make more robust
                    price = float(self.symbols[symbol]['quote'].get('price'))
                    if symbol in self.positions: #NOTE: Long only options strategy
                        ma = float(self.symbols[symbol]['ma'].get('ma'))
                        expiration = self.positions[symbol]['expiration']
                        strike = float(self.positions[symbol]['strike'])
                        callput = self.positions[symbol]['callput']
                        if price < ma and self.positions[symbol]['callput'] == 'call':
                            self.orders.append(self.create_order(symbol, 'market', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'option', expiration, strike, callput))
                        elif price > ma and self.positions[symbol]['callput'] == 'put':
                            self.orders.append(self.create_order(symbol, 'market', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'option', expiration, strike, callput))
                    if signal == Signal.HOLD:
                        continue
                    logger.info(f"FOUND SIGNAL: Symbol: {symbol} Signal: {signal} State: {self.symbols[symbol]}")
                    if symbol in self.positions:
                        logger.warning(f"Already have position. Skipping Signal {signal}. Position Side: {self.positions[symbol]['callput']} Positon: {self.positions[symbol]}")
                        continue
                    if self.options_chain[symbol] is None:
                        logger.warning(f"Options chain not found. Skipping Signal {signal}")
                        continue
                    if signal == Signal.LONG:
                        if datetime.datetime.utcnow() < self.symbols[symbol]['new_order_long_lock']:
                            logger.warning(f"New long order lock in effect. Skipping signal: {signal} symbol: {symbol} state: {self.symbols[symbol]} postions: {self.positions}")
                            continue
                        strike = float(self.options_chain[symbol]['call'][0]['strike']) #NOTE: gets strike with highest volum
                        self.orders.append(self.create_order(symbol, 'market', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'option', self.expiration, strike, 'call'))
                        self.symbols[symbol]['new_order_long_lock'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=NEW_ORDER_LOCK_SECS)
                    else:
                        if datetime.datetime.utcnow() < self.symbols[symbol]['new_order_short_lock']:
                            logger.warning(f"New short order lock in effect. Skipping signal: {signal} symbol: {symbol} state: {self.symbols[symbol]}")
                            continue
                        strike = float(self.options_chain[symbol]['put'][0]['strike']) #NOTE: gets strike with highest volume
                        self.orders.append(self.create_order(symbol, 'market', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'option', self.expiration, strike, 'put'))
                        self.symbols[symbol]['new_order_short_lock'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=NEW_ORDER_LOCK_SECS)
                await asyncio.sleep(SIGNAL_PROC_INTERVAL_SEC)
        except asyncio.CancelledError:
            logger.info(f"Decision Handler Cancelled")
            raise e
        except Exception as e:
            logger.exception(f"Decision Handler Exception: {str(e)}")
            raise e
        finally:
            logger.info(f"Decision Engine signal handler shutting down")

    async def exchange_order_handler(self, exchange_socket):
        try:
            while True:
                while len(self.orders)> 0:
                    order = self.orders.popleft()
                    logger.info(f"Sending order: {json.dumps(order._asdict(), indent=2)}")
                    await exchange_socket.send_json(json.dumps({
                        'type': 'request',
                        'channel': 'new_order',
                        'order': {
                            'symbol': order.symbol,
                            'order_type': order.order_type,
                            'side': order.side,
                            'price': order.price,
                            'quantity': order.quantity,
                            'offset': order.offset,
                            'tif': order.tif,
                            'asset_class': order.asset_class,
                            'exp': order.exp,
                            'strike': order.strike,
                            'callput': order.callput
                        }
                    }))
                await asyncio.sleep(ORDER_SOCKET_INTERVAL_SEC)
        except ConnectionError as e:
            logger.error(f'Exchange socket disconnected; Exchange Order Handler resetting: {e}')
        except asyncio.CancelledError:
            logger.info(f"Exchange Order Handler Cancelled")
            raise e
        except Exception as e:
            logger.exception(f"Exchange Order Handler Exception: {str(e)}")
            raise e
    
    async def exchange_msg_handler(self, exchange_socket):
        try:
            #TODO: modify to support sending msgs anytime decision engine wants. use queues and tasks
            while True:
                await exchange_socket.send_json(json.dumps({
                        'type': 'subscribe',
                        'channels': ['positions', 'orders'],
                        'interval': POSITIONS_INTERVAL_SEC
                }))
                await exchange_socket.send_json(json.dumps({
                    'type': 'request',
                    'channel': 'balances'
                }))
                for symbol in self.symbols.keys():
                    await exchange_socket.send_json(json.dumps({
                        'type': 'request',
                        'channel': 'options_chains',
                        'data': {
                            'underlying': symbol,
                            'expiration': self.expiration,
                            'max_price': MAX_OPTION_PRICE,
                            'min_price': MIN_OPTION_PRICE
                        }
                    }))
                async for msg in exchange_socket.receive():
                    msg = json.loads(msg)
                    #logger.info(f"Received Exchange message: {msg}")
                    if msg['type'] == 'update':
                        if msg['channel'] == 'positions':
                            self.positions = msg['data']
                        elif msg['channel'] == 'orders':
                            logger.info(f"Received order update: {msg['data']}")
                    elif msg['type'] == 'response':
                        if msg['type'] == 'success':
                            logger.info(msg)
                            if msg['channel'] == 'balances':
                                self.balances = msg['data']
                            elif msg['channel'] == 'options_chains':
                                if 'error' in msg['data'] or not msg['data']:
                                    raise Exception(f"Failed to retrieve options chain: {msg['data']}")
                                self.options_chain[msg['data']['underlying']] = msg['data']['options_chain']
                        elif msg['type'] == 'error':
                            logger.error(msg)
                await asyncio.sleep(ORDER_SOCKET_INTERVAL_SEC)
        except ConnectionError as e:
            logger.error(f'Exchange socket disconnected; Exchange Message Handler task resetting: {e}')
        except asyncio.CancelledError:
            logger.info(f"Exchange Message Handler Cancelled")
            raise e
        except Exception as e:
            logger.exception(f"Exchange Message Handler Exception: {str(e)}")
            raise e

    async def exchange_handler(self):
        methods = {
            'msg_handler': self.exchange_msg_handler,
            'order_handler': self.exchange_order_handler
        }
        self.exchange_tasks.clear()
        try:
            logger.info(f"Exchange Handler Starting")
            async with ContextManagedAsyncUnixSocketClient(self.exchange_socket) as exchange_socket:
                self.exchange_tasks = {
                    asyncio.create_task(self.exchange_msg_handler(exchange_socket), name='msg_handler'),
                    asyncio.create_task(self.exchange_order_handler(exchange_socket), name='order_handler')
                }
                while True:
                    #TODO: Remove when confident number of tasks doesnt increase over time
                    logger.info(f"{len(asyncio.all_tasks())} tasks running")
                    done, pending = await asyncio.wait(self.exchange_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task_name = task.get_name()
                        logger.info(f"Exchange {task_name} task completed. Respawning...")
                        self.exchange_tasks.remove(task)
                        self.exchange_tasks.add(asyncio.create_task(methods[task_name](exchange_socket), name=task_name))
        except (ConnectionRefusedError, ConnectionResetError):
            logger.error(f"Exchange Handler Unable to Connect. Resetting...")
            await asyncio.sleep(SOCKET_CONN_INTERVAL_SEC)
        except asyncio.CancelledError:
                logger.info(f"Exchange Handler Cancelled")
                raise e
        except Exception as e:
            logger.exception(f"Exchange Handler Exception: {str(e)}")
            raise e
        finally:
            for task in self.exchange_tasks:
                logger.info(f"Cancelling task: {task.get_name()}")
                task.cancel()
            await asyncio.gather(*self.exchange_tasks, return_exceptions=True)

    async def market_data_handler(self):
        while True:
            try:
                logger.info(f"Market Data Handler Starting")
                async with ContextManagedAsyncUnixSocketClient(self.market_data_socket) as md_socket:
                    await md_socket.send_json(json.dumps({
                        'type': 'subscribe',
                        'channels': ['quote', 'candle', 'ma', 'lookback'],
                        'symbols': list(self.symbols.keys())
                    }))
                    async for msg in md_socket.receive():
                        msg = json.loads(msg)
                        #logger.info(f"Received  MD message: {msg['channel']}")
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
            except asyncio.CancelledError:
                logger.info(f"Market Data Handler Cancelled")
                raise e
            except Exception as e:
                logger.exception(f"Market Data Handler Exception: {str(e)}")
                raise e

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
                methods = {
                    'decision_handler': self.decision_handler,
                    'market_data_handler': self.market_data_handler,
                    'exchange_handler': self.exchange_handler
                }
                logger.info("Decision Engine Starting")
                self.running = True
                decision_engine_tasks = {
                    asyncio.create_task(self.decision_handler(), name=f'decision_handler'),
                    asyncio.create_task(self.market_data_handler(), name=f'market_data_handler'),
                    asyncio.create_task(self.exchange_handler(), name=f'exchange_handler')
                }
                while True:
                    #TODO: Remove when confident number of tasks doesnt increase over time
                    logger.info(f"{len(asyncio.all_tasks())} tasks running")
                    done, pending = await asyncio.wait(decision_engine_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task_name = task.get_name()
                        logger.info(f"Exchange {task_name} task completed. Respawning...")
                        decision_engine_tasks.remove(task)
                        decision_engine_tasks.add(asyncio.create_task(methods[task_name](), name=task_name))
            finally:
                logger.info("Decision Engine Shutting Down")
                for task in decision_engine_tasks:
                    logger.info(f"Cancelling task: {task.get_name()}")
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