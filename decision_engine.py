import aiohttp
import asyncio
from aiohttp import web
from collections import deque
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime
logger = Logger(__name__)

class DecisionEngine():
    def __init__(self, execution_path, port):
        self.execution_path = execution_path
        self.port = port
        self.symbols = None

    def _process_price(self, data):
        symbol = data['symbol']
        timestamp = data['timestamp']
        price = float(data['price'])
        if symbol not in self.symbols:
            self.symbols[symbol] = dict()
        self.symbols[symbol]['price'] = price
        self.symbols[symbol]['timestamp'] = timestamp

    def _process_signal(self, data):
        raise NotImplementedError

    async def _handle_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async for msg in ws:
            msg = msg.json()
            if msg['type'] == 'price':
                self._process_price(msg['data'])
            elif msg['type'] == 'signal':
                self._process_signal(msg['data'])

    def start(self):
        app = web.Application()
        app.add_routes([web.get('/ws', self._handle_ws)])
        web.run_app(app, port=self.port)

def main():
    parser = ArgumentParser()
    decision_engine = parser.add_argument_group("Decision Engine", "Decision Engine parameters")
    decision_engine.add_argument('--execution_path', help="Path to Decision Engine")
    decision_engine.add_argument('--port', help="Port to run Decision Engine on")
    args = parser.parse_args()
    decision_engine = DecisionEngine(**args)

if __name__ == '__main__':
    main()