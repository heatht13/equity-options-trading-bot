import json
import asyncio
import enum
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime
from collections import namedtuple, deque
from async_unix_socket import ContextManagedAsyncUnixSocketClient

logger = Logger(__name__)

ORDER_SOCKET_INTERVAL_SEC = 2
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
    def __init__(self, data_provider_path, order_router_path):
        self.data_provider_path = data_provider_path
        self.order_router_path = order_router_path
        self.prices = dict()
        self.positions = dict()
        self.orders = deque(maxlen=MAX_ORDERS)
        self.indicators = dict()
        self.symbols = dict()
        self.price_state = PriceState.UNSET

    def generate_signal(self, symbol):
        price = self.prices[symbol]
        indicator = self.indicators[symbol]
        signal = Signal.HOLD
        if price > indicator['ma']:
            if price > indicator['lookback_high']:
                if self.price_state.is_pending_breakout_up():
                    signal = Signal.LONG
                self.price_state = PriceState.UP
            else:
                self.price_state = PriceState.PENDING_BREAKOUT_UP
        elif price < indicator['ma']:
            if price < indicator['lookback_low']:
                if self.price_state.is_pending_breakout_down():
                    signal =  Signal.SHORT 
                self.price_state = PriceState.DOWN
            else:
                self.price_state = PriceState.PENDING_BREAKOUT_DOWN
        else:
            self.price_state = PriceState.UNSET
        return signal

    def create_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type):
        return Order(
            symbol=str(symbol),
            order_type=str(order_type),
            side=str(side),
            price=f"{price:f}",
            quantity=f"{quantity:f}",
            offset=str(offset),
            tif=str(tif),
            asset_type=str(asset_type)
        )
    
    async def signal_handler(self):
        #TODO: This needs some work. Needs to handle every case/scenario. Currently, it does not.
        logger.info(f"Decision Engine signal handler started")
        try:
            while True:
                for symbol in self.symbols:
                    if symbol not in self.indicators or symbol not in self.prices:
                        continue
                    
                    signal = self.generate_signal(symbol)
                    if signal == Signal.HOLD:
                        continue

                    price = self.prices[symbol]
                    if signal == Signal.LONG:
                        if symbol not in self.positions:
                            self.orders.append(self.create_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'OPTION'))
                        elif self.positions[symbol]['side'] == 'short':
                            self.orders.append(self.create_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'close', 'day', 'OPTION'))
                    else:
                        if symbol not in self.positions:
                            self.orders.append(self.create_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'open', 'day', 'OPTION'))
                        elif self.positions[symbol]['side'] == 'long':
                            self.orders.append(self.create_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'OPTION'))
                asyncio.sleep(SIGNAL_PROC_INTERVAL_SEC)
        finally:
            logger.info(f"Decision Engine signal handler shutting down")

    async def send_orders(self, exchange_socket):
        try:
            while True:
                while self.orders.count() > 0:
                    order = self.orders.pop()
                    logger.info(f"Sending order: {order}")
                    await exchange_socket.send_str(json.dumps({
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
                            'asset_type': order.asset_type
                        }
                    }))
                asyncio.sleep(ORDER_SOCKET_INTERVAL_SEC)
        except ConnectionError as e:
            logger.error(f'Exchange socket disconnected; exchange handler orders task resetting: {e}')
    
    async def handle_exchange_msgs(self, exchange_socket):
        try:
            while True:
                exchange_socket.send_str(json.dumps({
                            'type': 'subscribe',
                            'channel': 'positions',
                            'interval': '10'
                        }))
                logger.info(f"Subscribed to positions channel")
                async for msg in exchange_socket:
                    msg = json.loads(msg)
                    if msg['type'] == 'update':
                        if msg['channel'] == 'positions':
                            self.positions[msg['symbol']] = msg['data']
                    elif msg['type'] == 'success':
                        logger.info(msg)
                    elif msg['type'] == 'error':
                        logger.error(msg)
                        break
        except ConnectionError as e:
            logger.error(f'Exchange socket disconnected; exchange handler message task resetting: {e}')

    async def exchange_handler(self):
        logger.info(f"Decision Engine exchange handler started")
        try:
            while True:
                async with ContextManagedAsyncUnixSocketClient(self.order_router_path) as exchange_socket:
                    exchange_tasks = set()
                    exchange_tasks.add(asyncio.create_task(self.handle_exchange_msgs(exchange_socket)).add_done_callback(exchange_tasks.discard))
                    exchange_tasks.add(asyncio.create_task(self.send_orders(exchange_socket)).add_done_callback(exchange_tasks.discard))
                    _, pending = await asyncio.wait(exchange_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                #TODO: Remove before going live. just doing this to mitigate against the inevitable while testing
                await asyncio.sleep(2)
        finally:
            for task in exchange_tasks:
                task.cancel()
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
            logger.info(f"Decision Engine exchange handler shutting down")

    async def market_data_handler(self):
        logger.info(f"Decision Engine market data handler started")
        try:
            while True:
                async with ContextManagedAsyncUnixSocketClient(self.data_provider_path) as md_socket:
                    await md_socket.send_str(json.dumps({
                        'type': 'subscribe',
                        'channel': 'prices',
                        'symbols': list(self.symbols)
                    }))
                    await md_socket.send_str(json.dumps({
                        'type': 'subscribe',
                        'channel': 'indicators',
                        'symbols': list(self.symbols)
                    }))
                    async for msg in md_socket:
                        msg = json.loads(msg)
                        print(msg)
                        if msg['type'] == 'update':
                            if msg['channel'] == 'prices':
                                self.prices[msg['symbol']] = msg['data']['price']
                            elif msg['channel'] == 'indicators':
                                self.indicators[msg['symbol']] = msg['data']
                        elif msg['type'] == 'success':
                            logger.info(msg)
                        elif msg['type'] == 'error':
                            logger.error(msg)
                            break
                #TODO: Remove before going live. just doing this to mitigate against the inevitable while testing
                await asyncio.sleep(2)
        finally:
            logger.info(f"Decision Engine market data handler shut down")

    async def decision_engine_main(self):
        logger.info("Decision Engine starting")
        try:
            self.running = True
            decision_engine_tasks = set()
            decision_engine_tasks.add(asyncio.create_task(self.signal_handler()).add_done_callback(decision_engine_tasks.discard))
            decision_engine_tasks.add(asyncio.create_task(self.exchange_handler()).add_done_callback(decision_engine_tasks.discard))
            decision_engine_tasks.add(asyncio.create_task(self.market_data_handler()).add_done_callback(decision_engine_tasks.discard))
            while self.running:
                done, _ = await asyncio.wait(decision_engine_tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    decision_engine_tasks.add(asyncio.create_task(task.get_coro()))
        finally:
            for task in decision_engine_tasks:
                task.cancel()
            await asyncio.gather(*decision_engine_tasks, return_exceptions=True)
            logger.info("Decision Engine shutting down")

    def shutdown(self):
        self.running = False

def main():
    parser = ArgumentParser()
    decision_engine = parser.add_argument_group("Decision Engine", "Decision Engine parameters")
    decision_engine.add_argument('--data-provider-path', type=str, default='/tmp/data_provider.sock', help="Path to data provider unix domain socket")
    decision_engine.add_argument('--order-router-path', type=str, default='/tmp/order_router.sock', help="Path to order router unix domain socket")
    args = parser.parse_args()
    args = vars(args)
    decision_engine = DecisionEngine(**args)
    asyncio.run(decision_engine.decision_engine_main())

if __name__ == '__main__':
    main()

DECISIONENGINE = DecisionEngine