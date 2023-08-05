import aiohttp
import asyncio
from collections import deque
from logging import Logger
from argparse import ArgumentParser
logger = Logger(__name__)

MA = set('sma', 'ema')

class MASignalGenerator():
    def __init__(self, feed_path=None) -> None:
        self.md_session = aiohttp.ClientSession()
        self.sma_queues = dict()
        self.ema_queues = dict()

    def _add_ma_queue(self, period, indicator):
        queues = self.sma_queues if indicator == 'sma' else self.ema_queues
        if period not in queues:
            queues[period] = deque(maxlen=period)
    
    def _sma(self, price, period):
        queue = self.sma_queues[period]
        queue.append(price)
        if len(queue) < period:
            logger.info(f'Not enough data for {period} period sma')
            return None
        return sum(queue) / queue.maxlen

    def _ema(self, price, period, ema_prev=None):
        raise NotImplementedError

    def _lookback(self, price, lookback):
        self.lookback_queue.append(price)
        if len(self.lookback_queue) < lookback:
            logger.info(f'Not enough data for {lookback} lookback period')
            return None
        return max(self.lookback_queue), min(self.lookback_queue)
    
    async def _update_signals(self, msg):
        price = float(msg['price'])
        ma = self._sma(price, self.period) if self.indicator == 'sma' else self._ema(price, self.period)
        lookback_high, lookback_low = self._lookback(price, self.lookback_period)
        return {
                'price': price, 
                'ma': ma, 
                'lookback_high': lookback_high, 
                'lookback_low': lookback_low
            }
    
    async def _book(self, msg):
        raise NotImplementedError
        
    async def handle_ws(self, feed_path):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(feed_path) as ws:
                        async for msg in ws:
                            logger.info(msg)
                            msg = msg.json()
                            if msg['type'] == 'book':
                                #TODO: Build book and send bids/asks to execution engine. May separate
                                # into their own processes. ie MD, SignalGenerator, Book
                                await self._book(msg)
                            elif msg['type'] == 'price':
                                signals = await self._update_signals(msg)
                                ##TODO: Send signals to execution engine
            except Exception as e:
                logger.error(e)
                await asyncio.sleep(1)

    async def signal_handler(self, feed_path, indicator, period, lookback):
        feed_path = feed_path
        self.indicator = indicator
        self.period = int(period)
        if indicator in MA:
            self._add_ma_queue(indicator, period)
        self.lookback_period = int(lookback)
        self.lookback_queue = deque(maxlen=self.lookback_period)
        await self.handle_ws(feed_path)

    def main(self):
        parser = ArgumentParser()
        ma_strategy = parser.add_argument_group("MA strategy", "Moving average strategy")
        ma_strategy.add_argument('--feed_path', type=str, default='ws://localhost:8080', help="data source")
        ma_strategy.add_argument('--indicator', type=str, default='sma', choices=MA, help="Moving average: ether sma or ema")
        ma_strategy.add_argument('--period', type=str, default='9', choices=[str(x) for x in range(1, 201)], help="Moving average period")
        ma_strategy.add_argument('--lookback', type=str, default='5', choices=[str(x) for x in range(1, 21)], help="Lookback period")
        args = parser.parse_args()
        args = vars(args)
        ma_signal_generator = asyncio.run(self.signal_handler(**args))

STRATEGY = MASignalGenerator