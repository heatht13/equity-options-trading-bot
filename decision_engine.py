import aiohttp
import asyncio
from aiohttp import web
import weakref
from aiohttp import WSCloseCode
from collections import deque
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime
import json

logger = Logger(__name__)

SIGNAL_PROC_WAIT_INTERVAL_SEC = 1
NUM_CONTRACTS = 1

class DecisionEngine():
    def __init__(self, execution_path, port, order_router_uri):
        self.execution_path = execution_path
        self.port = port
        self.order_router_ws = aiohttp.ClientSession().ws_connect(order_router_uri)
        self.prices = dict()
        self.positions = dict()
        self.signals = dict()
        self.symbols = set()

    def get_positions(self):
        self.order_router_ws.send_json({
            'type': 'get_positions'
        })

    def send_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type=None):
        self.order_router_ws.send_str(json.dumps({
            'type': 'order',
            'symbol': symbol,
            'order_type': order_type,
            'side': side,
            'price': price,
            'quantity': quantity,
            'offset': offset,
            'tif': tif,
            'asset_type': asset_type,
            'timestamp': datetime.utcnow().timestamp()

        }))
    def handle_price_update(self, data):
        symbol = data['symbol']
        self.symbols.add(symbol)
        timestamp = data['timestamp']
        price = data['price']
        self.prices[symbol] = price

    def handle_signal_update(self, symbol, data):
        self.symbols.add(symbol)
        self.signals[symbol] = data
        
    async def handle_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        request.app['websockets'].add(ws)
        try:
            async for msg in ws:
                msg = msg.json()
                if msg['type'] == 'price':
                    self.handle_price_update(msg['data'])
                elif msg['type'] == 'signal_update':
                    self.handle_signal_update(msg['data'])
        finally:
            request.app['websockets'].discard(ws)
        return ws
    
    async def on_shutdown(app):
        for ws in set(app['websockets']):
            await ws.close(code=WSCloseCode.GOING_AWAY,
                        message='Server shutdown')

    async def request_handler(self):
        app = web.Application()
        app.add_routes([web.get('/ws', self.handle_ws)])
        app.on_shutdown.append(self.on_shutdown)
        web.run_app(app, port=self.port)

    async def signal_processor(self):
        while True:
            for symbol in self.symbols:
                if symbol not in self.signals or symbol not in self.prices:
                    continue
                signal = self.signals[symbol]
                price = self.prices[symbol]
                if symbol not in self.positions:
                    if price > signal['ma'] and price > signal['lookback_high']:
                        self.send_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'open', 'day', 'OPTION')
                    elif price < signal['ma'] and price < signal['lookback_low']:
                        self.send_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'open', 'day', 'OPTION')
                else:
                    #NOTE: Could open up counter short/long position here after closing this
                    # side, but will skip and let next iteration decide with code above
                    position = self.positions[symbol]
                    if position['side'] == 'long':
                        if price < signal['ma']:
                            self.send_order(symbol, 'limit', 'sell', price, NUM_CONTRACTS, 'close', 'day', 'OPTION')
                    elif position['side'] == 'short':
                        if price > signal['ma']:
                            self.send_order(symbol, 'limit', 'buy', price, NUM_CONTRACTS, 'close', 'day', 'OPTION')

            await asyncio.sleep(SIGNAL_PROC_WAIT_INTERVAL_SEC)

    async def start(self):
        request_handler_task = asyncio.create_task(self.request_handler())
        signal_processor_task = asyncio.create_task(self.signal_processor())
        await request_handler_task
        await signal_processor_task

def main():
    parser = ArgumentParser()
    decision_engine = parser.add_argument_group("Decision Engine", "Decision Engine parameters")
    decision_engine.add_argument('--execution_path', type=str, default='https://localhost', help="Path to decision engine")
    decision_engine.add_argument('--port', type=str, default='8080', help="Port to run the decision engine on")
    order_router = parser.add_argument_group("Order Router", "Order Router parameters")
    order_router.add_argument('--order_router_uri', type=str, default='https://localhost:8081',  help="URI of Order Router")
    args = parser.parse_args()
    decision_engine = DecisionEngine(**args)
    asyncio.run(decision_engine.start())

if __name__ == '__main__':
    main()

DECISIONENGINE = DecisionEngine