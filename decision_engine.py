import json
import asyncio
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime
from collections import namedtuple
import enum

from async_unix_socket import ContextManagedAsyncUnixSocketClient

logger = Logger(__name__)

SIGNAL_PROC_WAIT_INTERVAL_SEC = 0.5
NUM_CONTRACTS = 1

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
        self.indicators = dict()
        self.symbols = dict()
        self.price_state = PriceState.UNSET

    def create_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type=None):
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

    async def signal_processor(self):
        try:
            async with ContextManagedAsyncUnixSocketClient(self.order_router_path) as order_router_socket:
                while True:
                    for symbol in self.symbols:
                        if symbol not in self.indicators or symbol not in self.prices:
                            continue
                       
                        signal = self.generate_signal(symbol)
                        if signal == Signal.HOLD:
                            continue

                        price = self.prices[symbol]
                        orders = set()
                        if signal == Signal.LONG:
                            if symbol in self.positions and self.positions[symbol]['side'] == 'short':
                                orders.add(self.create_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'close', 'day', 'OPTION'))
                            orders.add(self.create_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'OPTION'))
                        else:
                            if symbol in self.positions and self.positions[symbol]['side'] == 'long':
                                orders.add(self.create_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'OPTION'))
                            orders.add(self.create_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'open', 'day', 'OPTION'))
                            
                        for order in orders:
                            await order_router_socket.send_str(json.dumps({
                                'type': 'order',
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

                    await asyncio.sleep(SIGNAL_PROC_WAIT_INTERVAL_SEC)
        except Exception as e:
                logger.error(e)
        finally:
            #just doing this to mitgate against the inevitable
            await asyncio.sleep(5)

    async def signal_handler(self):
        while True:
            try:
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

                    async for msg in md_socket.receive():
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
            except Exception as e:
                logger.error(e)
            finally:
                #just doing this to mitgate against the inevitable
                await asyncio.sleep(5)

    async def start(self):
        self.signal_handler_task = asyncio.create_task(self.signal_handler())
        self.signal_processor_task = asyncio.create_task(self.signal_processor())
        await asyncio.gather(self.signal_handler_task, self.signal_processor_task)

def main():
    parser = ArgumentParser()
    decision_engine = parser.add_argument_group("Decision Engine", "Decision Engine parameters")
    decision_engine.add_argument('--data-provider-path', type=str, default='/tmp/data_provider.sock', help="Path to data provider unix domain socket")
    decision_engine.add_argument('--order-router-path', type=str, default='/tmp/order_router.sock', help="Path to order router unix domain socket")
    args = parser.parse_args()
    decision_engine = DecisionEngine(**args)
    asyncio.run(decision_engine.start())

if __name__ == '__main__':
    main()

DECISIONENGINE = DecisionEngine