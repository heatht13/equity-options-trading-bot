import aiohttp
import asyncio
from collections import deque
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime
logger = Logger(__name__)

MA = set('sma', 'ema')
OHLC = {'o': 0, 'h': 0, 'l': 0, 'c': 0, 't': 0}
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
#TODO: Need to implement candle building. Currently takes price msgs only

class MASignalGenerator():
    def __init__(self, execution_uri=None) -> None:
        self.execution_ws = aiohttp.ClientSession().ws_connect(execution_uri)
        self.sma_queues = dict()
        self.ema_queues = dict()
        self.ohlc = OHLC

    def _add_ma_queue(self, period, indicator):
        queues = self.sma_queues if indicator == 'sma' else self.ema_queues
        if period not in queues:
            queues[period] = deque(maxlen=period)
    
    def _sma(self, candle, period):
        queue = self.sma_queues[period]
        queue.append(candle)
        if len(queue) < period:
            logger.info(f'Not enough data for {period} period sma')
            return None
        return sum((candle['c'] for candle in queue)) / queue.maxlen

    def _ema(self, price, period, ema_prev=None):
        raise NotImplementedError

    def _lookback(self, candle, lookback):
        self.lookback_queue.append(candle)
        if len(self.lookback_queue) < lookback:
            logger.info(f'Not enough data for {lookback} lookback period')
            return None
        return max((candle['h'] for candle in self.lookback_queue)), min((candle['l'] for candle in self.lookback_queue))
    
    async def _update_signals(self, candle, symbol, timestamp):

        ma = self._sma(candle, self.period) if self.indicator == 'sma' else self._ema(candle, self.period)
        lookback_high, lookback_low = self._lookback(candle, self.lookback_period)
        self.execution_ws.send_json({
                                        'ohlc': candle,
                                        'ma': ma,
                                        'ma_period': self.period,
                                        'indicator': self.indicator,
                                        'lookback_high': lookback_high, 
                                        'lookback_low': lookback_low,
                                        'symbol': symbol,
                                        'timestamp': timestamp
                                    }) 
                                     
    async def _book(self, msg):
        raise NotImplementedError
    
    async def _handle_ticks(self, msg):
        msg_time = datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        price = float(msg['price'])
        symbol = msg['symbol']
        self.execution_ws.send_json({'price': price,
                                     'timestamp': msg_time,
                                     'symbol': symbol
                                     })
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
            self._update_signals(self.ohlc, symbol, msg_time)
        
    async def handle_ws(self, feed_uri, symbols):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(feed_uri) as ws:
                        sub_price = {
                            "type": "subscribe",
                            "channel": "QUOTE",
                            "symbol": symbols
                        }
                        await ws.send_json(sub_price)
                        sub_book = {
                            "type": "subscribe",
                            "channel": "TICK",
                            "symbol": symbols
                        }
                        await ws.send_json(sub_book)
                        async for msg in ws:
                            logger.info(msg)
                            msg = msg.json()
                            if msg['type'] == 'QUOTE':
                                await self._book(msg)
                            elif msg['type'] == 'TICK':
                                self._handle_ticks(msg)
                            else:
                                logger.warning(f'Unknown message: {msg["type"]}')
            except Exception as e:
                logger.error(e)
                await asyncio.sleep(1)

    async def signal_handler(self, feed_uri, timeframe, symbols, indicator, period, lookback):
        self.timeframe = TIMEFRAMES[timeframe]
        self.symbols = symbols
        self.indicator = indicator
        self.period = int(period)
        if indicator in MA:
            self._add_ma_queue(indicator, period)
        self.lookback_period = int(lookback)
        self.lookback_queue = deque(maxlen=self.lookback_period)
        await self.handle_ws(feed_uri, symbols)

def main(self):
    parser = ArgumentParser()
    ma_strategy = parser.add_argument_group("MA strategy", "Moving average strategy")
    ma_strategy.add_argument('--feed_uri', type=str, default='ws://localhost:8080', help="data source uri")
    ma_strategy.add_argument('--timeframe', type=str, default='5m', choices=['1m', '5m', '15m', '1h', '4h', '1d'], help="Timeframe for candles")
    ma_strategy.add_argument('--symbols', type=str, default='SPY', help="Symbols to trade")
    ma_strategy.add_argument('--indicator', type=str, default='sma', choices=MA, help="Moving average: ether sma or ema")
    ma_strategy.add_argument('--period', type=str, default='9', choices=[str(x) for x in range(1, 201)], help="Moving average period")
    ma_strategy.add_argument('--lookback', type=str, default='5', choices=[str(x) for x in range(1, 21)], help="Lookback period")
    execution_args = parser.add_argument_group("Execution", "Execution parameters")
    execution_args.add_argument('--execution_uri', type=str, default='ws://localhost:8080', help="Execution uri")
    credentials = parser.add_argument_group("Credentials", "Credentials for data source")
    credentials.add_argument('--api_key', type=str, default=None, help="API key")
    credentials.add_argument('--api_secret', type=str, default=None, help="API secret")
    args = parser.parse_args()
    #TODO:figure out how to get arg groups
    args = vars(args)
    signal_generator = MASignalGenerator(**args)
    ma_signal_task = asyncio.run(signal_generator.signal_handler(**args))

if __name__ == '__main__':
    main()

STRATEGY = MASignalGenerator