import json
import asyncio
from collections import deque
from logging import Logger
from datetime import datetime
from async_unix_socket import AsyncUnixSocketServer

logger = Logger(__name__)

MA = {'sma', 'ema'}
OHLC = {'o': 0, 'h': 0, 'l': 0, 'c': 0, 't': 0}
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}

class MALookbackDataParser():
    def __init__(self, socket, timeframe, symbols, indicator, period, lookback):
        self.socket = socket
        self.timeframe = TIMEFRAMES[timeframe]
        self.symbols = symbols
        self.indicator = indicator
        self.period = int(period)
        self.lookback_period = int(lookback)
        self.ma_queue = deque(maxlen=self.period)
        self.lookback_queue = deque(maxlen=self.lookback_period)
        self.ohlc = OHLC
        self.client_subscriptions = {
            'prices': set(),
            'indicators': set()
        }
    
    def sma(self, candle, period):
        self.ma_queue.append(candle)
        if len(self.ma_queue) < period:
            logger.info(f'Not enough data for {period} period sma')
            return None
        return sum((candle['c'] for candle in self.ma_queue)) / self.ma_queue.maxlen

    def ema(self, price, period, ema_prev=None):
        raise NotImplementedError

    def lookback(self, candle, lookback):
        self.lookback_queue.append(candle)
        if len(self.lookback_queue) < lookback:
            logger.info(f'Not enough data for {lookback} lookback period')
            return None
        return max((candle['h'] for candle in self.lookback_queue)), min((candle['l'] for candle in self.lookback_queue))
    
    async def send_indicators(self, candle, symbol, timestamp):

        ma = self.sma(candle, self.period) if self.indicator == 'sma' else self.ema(candle, self.period)
        lookback_high, lookback_low = self.lookback(candle, self.lookback_period)
        await self.server.send_str(json.dumps({
                                    'type': 'update',
                                    'channel': 'indicator',
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
        if symbol in self.client_subscriptions['prices']:
            await self.server.send_str(json.dumps({
                                        'type': 'update',
                                        'channel': 'prices',
                                        'symbol': symbol,
                                        'data': {
                                            'price': price,
                                            'timestamp': msg_time,
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
            if symbol in self.client_subscriptions['indicators']:
                await self.send_indicators(self.ohlc, symbol, msg_time)
            self.ohlc = OHLC
    
    async def stream_handler(self, symbols):
        raise NotImplementedError
    
    async def data_handler(self):
        logger.info(f"Data Handler started")
        try:
            while self.running:
                if self.indicator not in MA:
                    raise ValueError(f'Invalid indicator {self.indicator}')
                await self.stream_handler(self.symbols)
                self.server = AsyncUnixSocketServer(self.socket)
                async for msg in self.server.open():
                    msg = json.loads(msg)
                    print("Received:", msg)
                    msg_type = msg.get('type', None)
                    if msg_type not in ('subscribe', 'unsubscribe'):
                        await self.server.send_str(json.dumps({'error': 'Invalid message type. Must be either \'subscribe\' or \'unsubscribe\''}))
                        continue
                    msg_channel = msg.get('channel', None)
                    if msg_type == 'subscribe':
                        if msg_channel == 'all':
                            self.client_subscriptions['prices'].update(msg['symbols'])
                            self.client_subscriptions['indicators'].update(msg['symbols'])
                            await self.server.send_str(json.dumps({'Success': f'Subscribed prices and indicators for {msg["symbols"]}'}))
                        channel = self.client_subscriptions.get(msg_channel)
                        if channel:
                            self.client_subscriptions[channel].update(msg['symbols'])
                            await self.server.send_str(json.dumps({'Success': f'Subscribed {msg_channel} for {msg["symbols"]}'}))
                        else:
                            await self.server.send_str(json.dumps({'error': 'Invalid message channel. Must be either \'prices\', \'indicators\', or \'all\''}))
                    else:
                        if msg_channel == 'all':
                            self.client_subscriptions['prices'].difference_update(msg['symbols'])
                            self.client_subscriptions['indicators'].difference_update(msg['symbols'])
                            await self.server.send_str(json.dumps({'Success': f'Unsubscribed prices and indicators for {msg["symbols"]}'}))
                        channel = self.client_subscriptions.get(msg_channel)
                        if channel:
                            self.client_subscriptions[channel].difference_update(msg['symbols'])
                            await self.server.send_str(json.dumps({'Success': f'Unsubscribed {msg_channel} for {msg["symbols"]}'}))
                        else:
                            await self.server.send_str(json.dumps({'error': 'Invalid message channel. Must be either \'prices\', \'indicators\', or \'all\''}))
        except ConnectionError as e:
            logger.error(f"Data Handler Connection Error; resetting. {e}")
        finally:
            if self.rest_session and not self.rest_session.closed:
                await self.rest_session.close()
                self.rest_session = None
            if self.ws_session and not self.ws_session.closed:
                await self.ws_session.close()
                self.ws_session = None
            if self.server:
                await self.server.close()
                self.server = None
            self.ma_queue.clear()
            self.lookback_queue.clear()
            logger.info(f"Data Handler shut down")

    def shutdown(self):
        self.running = False

    async def data_handler_main(self):
        logger.info("Data Provider starting")
        try:
            self.running = True
            data_handler_tasks = set()
            data_handler_tasks.add(asyncio.create_task(self.data_handler()).add_done_callback(data_handler_tasks.discard))
            while self.running:
                done, _ = await asyncio.wait(data_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    data_handler_tasks.add(asyncio.create_task(task.get_coro()))
        finally:
            for task in data_handler_tasks:
                task.cancel()
            await asyncio.gather(*data_handler_tasks, return_exceptions=True)
            logger.info("Data Provider shutting down")