import aiohttp
from aiohttp import web
import asyncio
from collections import deque
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime
import json

logger = Logger(__name__)

MA = set(('sma', 'ema'))
OHLC = {'o': 0, 'h': 0, 'l': 0, 'c': 0, 't': 0}
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}

class MASignalGenerator():
    def __init__(self, execution_path, port, decision_engine_uri, timeframe, symbols, indicator, period, lookback):
        self.execution_path = execution_path
        self.port = port
        self.decision_engine_ws = aiohttp.ClientSession().ws_connect(decision_engine_uri)
        self.timeframe = TIMEFRAMES[timeframe]
        self.symbols = symbols
        self.indicator = indicator
        self.period = int(period)
        self.lookback_period = int(lookback)
        self.sma_queues = dict()
        self.ema_queues = dict()
        self.ohlc = OHLC

    def add_ma_queue(self, period, indicator):
        queues = self.sma_queues if indicator == 'sma' else self.ema_queues
        if period not in queues:
            queues[period] = deque(maxlen=period)
    
    def sma(self, candle, period):
        queue = self.sma_queues[period]
        queue.append(candle)
        if len(queue) < period:
            logger.info(f'Not enough data for {period} period sma')
            return None
        return sum((candle['c'] for candle in queue)) / queue.maxlen

    def ema(self, price, period, ema_prev=None):
        raise NotImplementedError

    def lookback(self, candle, lookback):
        self.lookback_queue.append(candle)
        if len(self.lookback_queue) < lookback:
            logger.info(f'Not enough data for {lookback} lookback period')
            return None
        return max((candle['h'] for candle in self.lookback_queue)), min((candle['l'] for candle in self.lookback_queue))
    
    async def send_signals(self, candle, symbol, timestamp):

        ma = self.sma(candle, self.period) if self.indicator == 'sma' else self.ema(candle, self.period)
        lookback_high, lookback_low = self.lookback(candle, self.lookback_period)
        self.decision_engine_ws.send_str(json.dumps({
                                    'type': 'signal_update',
                                    'symbol': symbol,
                                    'data': {
                                            'ohlc': candle,
                                            'ma': ma,
                                            #'ma_period': self.period,
                                            #'indicator': self.indicator,
                                            'lookback_high': lookback_high, 
                                            'lookback_low': lookback_low,
                                            #'look_back_period': self.lookback_period,
                                            'timestamp': timestamp
                                        }
                                    }))
                                     
    async def book(self, msg):
        raise NotImplementedError
    
    async def handle_ticks(self, msg):
        msg_time = datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        price = float(msg['price'])
        symbol = msg['symbol']
        self.decision_engine_ws.send_str(json.dumps({
                                    'type': 'price',
                                    'data': {
                                        'price': price,
                                        'timestamp': msg_time,
                                        'symbol': symbol
                                        }
                                     }))
        #TODO: Need to confirm msgs are in order and correspond to current candle
        if price > self.ohlc['h']:
            self.ohlc['h'] = price
        elif price < self.ohlc['l']:
            self.ohlc['l'] = price
        #open candle: will need more precision.Currently seconds
        if msg_time % self.timeframe == 1:
            self.ohlc['o'] = price
            self.ohlc['h'] = price
            self.ohlc['l'] = price
            self.ohlc['c'] = price
            self.ohlc['t'] = self.timeframe
            pass
        #close candle: will need more precision. Currently seconds
        elif msg_time % self.timeframe == 0:
            self.ohlc['c'] = price
            self.send_signals(self.ohlc, symbol, msg_time)
    
    async def handle_ws(self, symbols):
        raise NotImplementedError

    async def signal_handler_main(self):
        if self.indicator in MA:
            self.add_ma_queue(self.period, self.indicator)
        self.lookback_queue = deque(maxlen=self.lookback_period)

        app = web.Application()
        #TODO: not correct, just filling in for now
        app.add_routes([web.get('/ws', self.handle_ws)])
        web.run_app(app, port=self.port)
        if self.session:
            await self.session.close()
            self.session = None