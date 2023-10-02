import json
import asyncio
import logging
from datetime import datetime
from argparse import ArgumentParser
from importlib import import_module
from collections import deque

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
logger.setLevel(logging.INFO)

MA = {'sma', 'ema'}
OHLC = {'o': 0, 'h': 0, 'l': 0, 'c': 0, 't': 0}
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
EXCHANGES = {'tradier', 'fake'}

class MALookbackDataParser():
    class Error(Exception):
        pass

    def __init__(self, timeframe, symbols, indicator, period, lookback):
        self.client = {
                'queue': asyncio.Queue(),
                'quote': set(),
                'timesale': set(),
                'ma': set(),
                'lookback': set(),
                'trade': set()
            }
        self.rest_session = None
        self.ws_session = None
        self.timeframe = TIMEFRAMES[timeframe]
        self.symbols = symbols
        self.indicator = indicator
        self.period = int(period)
        self.lookback_period = int(lookback)
        self.ma_queue = deque(maxlen=self.period)
        self.lookback_queue = deque(maxlen=self.lookback_period)
        self.ohlc = OHLC.copy()

    def sma(self, candle, period, tick, price):
        if tick != 0: #NOTE: This is absolutely horrible but works for now
            if len(self.ma_queue) < period:
                return None
            return sum((candle['c'] for candle in self.ma_queue)) - self.ma_queue[0]['c'] + price / self.ma_queue.maxlen
        self.ma_queue.append(candle)
        if len(self.ma_queue) < period:
            return None
        return sum((candle['c'] for candle in self.ma_queue)) / self.ma_queue.maxlen

    def ema(self, candle, period, ema_prev=None):
        raise NotImplementedError

    def lookback(self, candle, lookback):
        self.lookback_queue.append(candle)
        if len(self.lookback_queue) < lookback:
            return None, None
        return max((candle['h'] for candle in self.lookback_queue)), min((candle['l'] for candle in self.lookback_queue))
    
    async def update_indicators(self, msg):
        symbol = msg['symbol']
        price = msg['data']['price']
        quote_time = msg['data']['quote_time']
        #open candle: will need more precision.Currently seconds
        if price > self.ohlc['h']:
            self.ohlc['h'] = price
        elif price < self.ohlc['l']:
            self.ohlc['l'] = price
        tick = quote_time % self.timeframe
        if tick == 1:
            self.ohlc['o'] = price
            self.ohlc['h'] = price
            self.ohlc['l'] = price
            self.ohlc['c'] = price
            self.ohlc['t'] = self.timeframe // 60
        #close candle: will need more precision. Currently seconds
        elif tick == 0:
            self.ohlc['c'] = price
            lookback_high, lookback_low = self.lookback(self.ohlc, self.lookback_period)
            #logger.info(f"Lookback Q: {self.lookback_queue}")
            msg = {
                'handler': 'data',
                'type': 'update',
                'channel': 'lookback',
                'symbol': symbol,
                'timestamp': datetime.utcnow().timestamp(),
                'data': {
                    # 'ohlc': self.ohlc,
                    #'ma_period': self.period,
                    #'indicator': self.indicator,
                    'lookback_high': lookback_high, 
                    'lookback_low': lookback_low,
                    #'look_back_period': self.lookback_period,
                    'close_time': quote_time
                }
            }
            self.ohlc = OHLC.copy()
            # logger.info(f"Lookback: {json.dumps(msg, indent=2)}")
            await self.send_msg(msg)
        ma = self.sma(self.ohlc, self.period, tick, price) if self.indicator == 'sma' else self.ema(self.ohlc, self.period, tick, price)
        msg = {
            'handler': 'data',
            'type': 'update',
            'channel': 'ma',
            'symbol': symbol,
            'timestamp': datetime.utcnow().timestamp(),
            'data': {
                'ma': ma,
                'ma_period': self.period,
                'close_time': quote_time
            }
        }
        await self.send_msg(msg)
        # logger.info(f"MA Q: {self.ma_queue}")
        # logger.info(f"MA: {json.dumps(msg, indent=2)}")
        # logger.info(f"OHLC: {json.dumps(self.ohlc, indent=2)}")
        
    async def book(self, **kwargs):
        raise NotImplementedError
    
    async def stream_handler(self, **kwargs):
        raise NotImplementedError
    
    def parse_msg(self, msg):
        raise NotImplementedError
    
    async def send_msg(self, msg):
        await self.client['queue'].put(msg)
    
    async def handle_msg(self, msg):
        msg = self.parse_msg(msg)
        if msg is None:
            return
        await self.send_msg(msg)
        if msg['channel'] == 'quote':
            logger.info(f"TIME: {datetime.fromtimestamp(msg['data']['quote_time'])}")
            await self.update_indicators(msg)

    async def data_handler_main(self):
        while True:
            try:
                logger.info("Data Handler Starting")
                await self.stream_handler()
            finally:
                logger.info("Data Handler Shutting Down")
                self.ma_queue.clear()
                self.lookback_queue.clear()
                self.ohlc = OHLC.copy()
                self.client['quote'].clear()
                self.client['timesale'].clear()
                self.client['ma'].clear()
                self.client['lookback'].clear()
                self.client['trade'].clear()

class MDSocketServer:
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, socket, exchange, **data_handler_kwargs):
        self.socket = socket
        self.exchange = exchange
        self.running = True
        self.data_handler_kwargs = data_handler_kwargs

    async def send_json(self, writer, msg):
        if writer.is_closing():
            raise ConnectionError("Connection to client closing")
        msg_bytes = msg.encode('utf-8')
        message_length = len(msg_bytes)
        writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        writer.write(msg_bytes)
        await writer.drain()

    async def msg_handler(self, writer):
        while True:
            msg = await self.data_handler.client['queue'].get()
            channel = msg['channel']
            symbol = msg['symbol']
            if symbol in self.data_handler.client.get(channel, set()):
                await self.send_json(writer, json.dumps(msg))
            self.data_handler.client['queue'].task_done()

    async def request_handler(self, reader, writer):
        while True:
            msg_length_prefix = await reader.read(self.MSG_LENGTH_PREFIX_BYTES)
            if not msg_length_prefix:
                break
            msg_length = int.from_bytes(msg_length_prefix, byteorder='big')
            msg = await reader.read(msg_length)
            if not msg:
                break
            msg = msg.decode('utf-8')
            msg = json.loads(msg)
            logger.info(f"Received: {msg}")
            msg_type = msg.get('type', None)
            if msg_type not in ('subscribe', 'unsubscribe'):
                await self.send_json(writer, json.dumps({'error': 'Invalid message type. Must be either \'subscribe\' or \'unsubscribe\''}))
                continue
            response = None
            msg_channels = msg.get('channels', [])
            if msg_type == 'subscribe':
                for channel in msg_channels:
                    if channel == 'quote':
                        self.data_handler.client['quote'].update(msg['symbols'])
                        response = {'success': f'Subscribed quotes for {msg["symbols"]}'}
                    elif channel == 'timesale':
                        self.data_handler.client['timesale'].update(msg['symbols'])
                        response = {'success': f'Subscribed timesale for {msg["symbols"]}'}
                    elif channel == 'ma':
                        self.data_handler.client['ma'].update(msg['symbols'])
                        response = {'success': f'Subscribed ma for {msg["symbols"]}'}
                    elif channel == 'lookback':
                        self.data_handler.client['lookback'].update(msg['symbols'])
                        response = {'success': f'Subscribed lookback for {msg["symbols"]}'}
                    elif channel == 'trade':
                        self.data_handler.client['trade'].update(msg['symbols'])
                        response = {'success': f'Subscribed trade for {msg["symbols"]}'}
                    else:
                        response = {'error': 'Invalid message channel. Must be either \'quote\', \'timesale\', \'ma\', \'lookback\', or \'trade\''}
            else:
                for channel in msg_channels:
                    if channel == 'quote':
                        self.data_handler.client['quote'].difference_update(msg['symbols'])
                        response = {'success': f'Unsubscibed quotes for {msg["symbols"]}'}
                    elif channel == 'timesale':
                        self.data_handler.client['timesale'].difference_update(msg['symbols'])
                        await self.send_json(writer, json.dumps({'type': 'success',
                                                                'Success': f'Unsubscibed timesale for {msg["symbols"]}'}))
                    elif channel == 'ma':
                        self.data_handler.client['ma'].difference_update(msg['symbols'])
                        response = {'success': f'Unsubscibed ma for {msg["symbols"]}'}
                    elif channel == 'lookback':
                        self.data_handler.client['lookback'].difference_update(msg['symbols'])
                        response = {'success': f'Unsubscibed lookback for {msg["symbols"]}'}
                    elif channel == 'trade':
                        self.data_handler.client['trade'].difference_update(msg['symbols'])
                        response = {'success': f'Unsubscibed trade for {msg["symbols"]}'}
                    else:
                        response = {'error': 'Invalid message channel. Must be either \'quote\', \'timesale\', \'ma\', \'lookback\', or \'trade\''}
            if response is not None:
                msg = {
                    'handler': 'data',
                    'type': 'response',
                    'channel': channel,
                    'timestamp': datetime.utcnow().timestamp(),
                    'data': response
                }
                await self.send_json(writer, json.dumps(msg))


    async def on_connect(self, reader, writer):
        try:
            client = str(writer.get_extra_info('sockname'))
            logger.info(f"Client connected on {client}")
            client_handler_tasks = {
                                    asyncio.create_task(self.request_handler(reader, writer), name=f'{client}_request_handler'), 
                                    asyncio.create_task(self.msg_handler(writer), name=f'{client}_msg_handler')
                                }
            await asyncio.wait(client_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            logger.info(f"Client disconnected on {client}")
            for task in client_handler_tasks:
                logger.debug(f"Cancelling task: {task.get_name()}")
                task.cancel()
            await asyncio.gather(*client_handler_tasks, return_exceptions=True)
            self.data_handler.client['quote'].clear()
            self.data_handler.client['timesale'].clear()
            self.data_handler.client['ma'].clear()
            self.data_handler.client['lookback'].clear()
            self.data_handler.client['trade'].clear()
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionResetError:
                pass
    
    async def server_task(self):
        while self.running:
            try:
                logger.info(f"Socket Server Starting")
                server = await asyncio.start_unix_server(self.on_connect, self.socket)
                async with server:
                    await server.serve_forever()
            finally:
                logger.info(f"Socket Server Shutting Down")
                if server:
                    server.close()
                    await server.wait_closed()

    async def main_task(self):
        while True:
            try:
                logger.info("Market Data Starting")
                self.running = True
                self.data_handler = getattr(import_module(f'data_handlers.{self.exchange}_data_handler'),
                                            f'{self.exchange.capitalize()}DataHandler')(**self.data_handler_kwargs)
                md_handler_tasks = {
                                    asyncio.create_task(self.data_handler.data_handler_main(), name=f'{self.exchange.capitalize()} Data Handler'), 
                                    asyncio.create_task(self.server_task(), name=f'MD Socket Server')
                                }
                await asyncio.wait(md_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                logger.info("Market Data Shutting Down")
                self.running = False
                for task in md_handler_tasks:
                    logger.info(f"Cancelling task: {task.get_name()}")
                    task.cancel()
                await asyncio.gather(*md_handler_tasks, return_exceptions=True)
            
def main():
    parser = ArgumentParser()
    signal_generator_args = parser.add_argument_group("Data Provider", "Data Provider parameters")
    signal_generator_args.add_argument('--socket', type=str, default='/tmp/md_server.sock', help="Path to unix domain socket responsible for serving data")
    credentials = parser.add_argument_group("Credentials", "Credentials for data source")
    credentials.add_argument('--exchange', type=str.lower, choices=EXCHANGES, help="Exchange to trade on")
    credentials.add_argument('--access-token', type=str, help="API access token")
    ma_strategy = parser.add_argument_group("MA strategy", "Moving average strategy")
    ma_strategy.add_argument('--timeframe', type=str, default='5m', choices=TIMEFRAMES.keys(), help="Timeframe for candles")
    ma_strategy.add_argument('--symbols', type=str.upper,  nargs='*', help="Symbols to trade")
    ma_strategy.add_argument('--indicator', type=str, default='sma', choices=MA, help="Moving average: ether sma or ema")
    ma_strategy.add_argument('--period', type=str, default='9', choices=[str(x) for x in range(1, 201)], help="Moving average period")
    ma_strategy.add_argument('--lookback', type=str, default='5', choices=[str(x) for x in range(1, 21)], help="Lookback period")
    args = parser.parse_args()
    kwargs = vars(args)
    server = MDSocketServer(**kwargs)
    asyncio.run(server.main_task())

if __name__ == '__main__':
    main()