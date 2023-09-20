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

    def __init__(self, clients, timeframe, symbols, indicator, period, lookback):
        self.clients = clients
        self.timeframe = TIMEFRAMES[timeframe]
        self.symbols = symbols
        self.indicator = indicator
        self.period = int(period)
        self.lookback_period = int(lookback)
        self.ma_queue = deque(maxlen=self.period)
        self.lookback_queue = deque(maxlen=self.lookback_period)
        self.ohlc = OHLC.copy()

    def sma(self, candle, period):
        self.ma_queue.append(candle)
        if len(self.ma_queue) < period:
            logger.info(f'Not enough data for {period} period sma')
            return None
        return sum((candle['c'] for candle in self.ma_queue)) / self.ma_queue.maxlen

    def ema(self, candle, period, ema_prev=None):
        raise NotImplementedError

    def lookback(self, candle, lookback):
        self.lookback_queue.append(candle)
        if len(self.lookback_queue) < lookback:
            logger.info(f'Not enough data for {lookback} lookback period')
            return None, None
        return max((candle['h'] for candle in self.lookback_queue)), min((candle['l'] for candle in self.lookback_queue))
    
    def update_indicators(self, msg):
        symbol = msg['symbol']
        price = msg['data']['price']
        quote_time = msg['data']['quote_time']
        if price > self.ohlc['h']:
            self.ohlc['h'] = price
        elif price < self.ohlc['l']:
            self.ohlc['l'] = price
        #open candle: will need more precision.Currently seconds
        if quote_time % self.timeframe == 1:
            self.ohlc['o'] = price
            self.ohlc['h'] = price
            self.ohlc['l'] = price
            self.ohlc['c'] = price
            self.ohlc['t'] = self.timeframe // 60
        #close candle: will need more precision. Currently seconds
        if quote_time % self.timeframe == 0:
            self.ohlc['c'] = price
            ma = self.sma(self.ohlc, self.period) if self.indicator == 'sma' else self.ema(self.ohlc, self.period)
            logger.info(f"MA Q: {self.ma_queue}")
            lookback_high, lookback_low = self.lookback(self.ohlc, self.lookback_period)
            logger.info(f"Lookback Q: {self.lookback_queue}")
            msg = {
                'type': 'update',
                'channel': 'indicator',
                'symbol': symbol,
                'timestamp': datetime.utcnow().timestamp(),
                'data': {
                    'ohlc': self.ohlc,
                    'ma': ma,
                    #'ma_period': self.period,
                    #'indicator': self.indicator,
                    'lookback_high': lookback_high, 
                    'lookback_low': lookback_low,
                    #'look_back_period': self.lookback_period,
                    'close_time': quote_time
                }
            }
            self.ohlc = OHLC.copy()
            logger.info(f"INDICATOR: {json.dumps(msg, indent=2)}")
            return msg
        logger.info(f"OHLC: {json.dumps(self.ohlc, indent=2)}")
        return
        
    async def book(self, msg):
        raise NotImplementedError
    
    async def parse_msg(self, msg):
        raise NotImplementedError
    
    async def stream_handler(self, symbols):
        raise NotImplementedError
    
    async def send_msg(self, msg):
        for client in self.clients:
            await self.clients[client]['queue'].put(msg)
    
    async def handle_msg(self, msg):
        msg = self.parse_msg(msg)
        if msg is None:
            return
        await self.send_msg(msg)
        if msg['channel'] == 'quote':
            #TODO: Need to pass MA msgs every seconds so DE can track where it is relative to price
            #Might make more sense to have DE caculate MA. Lookback can stay here, idk.
            #Going to pass MA inside price msgs for now. May decide to just create one msg that contains all data (indicators, timesale, quote, etc)
            #and pass that every second. lookback will be repetitive though, so maybe not. Only price and MA will change every second.
            logger.info(f"TIME: {datetime.fromtimestamp(msg['data']['quote_time'])}")
            update = self.update_indicators(msg)
            if update is not None:
                await self.send_msg(update)

    async def data_handler_main(self):
        while True:
            try:
                logger.info("Data Handler Starting")
                await self.stream_handler()
            finally:
                logger.info("Data Handler Shutting Down")
                self.ma_queue.clear()
                self.lookback_queue.clear()
                self.ohlc = OHLC

class MDSocketServer:
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, socket, exchange, **data_handler_kwargs):
        self.socket = socket
        self.exchange = exchange
        self.running = True
        self.clients = dict()
        data_handler_kwargs['clients'] = self.clients
        self.data_handler_kwargs = data_handler_kwargs

    async def send_json(self, writer, msg):
        if writer.is_closing():
            raise ConnectionError("Connection to client closing")
        msg_bytes = msg.encode('utf-8')
        message_length = len(msg_bytes)
        writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        writer.write(msg_bytes)
        await writer.drain()

    async def msg_handler(self, client, writer):
        while True:
            msg = await self.clients[client]['queue'].get()
            channel = msg['channel']
            symbol = msg['symbol']
            if symbol in self.clients[client]['quote']:
                await self.send_json(writer, json.dumps(msg))

    async def request_handler(self, client, reader, writer):
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
            msg_channel = msg.get('channel', None)
            if msg_channel not in ('quotes', 'timesale', 'indicators', 'all'):
                await self.send_json(writer, json.dumps({'error': 'Invalid message channel. Must be either \'quotes\', \'timesale\', \'indicators\', or \'all\''}))
                continue
            if msg_type == 'subscribe':
                if msg_channel in ('quotes', 'all'):
                    self.clients[client]['quote'].update(msg['symbols'])
                    await self.send_json(writer, json.dumps({'type': 'success',
                                                             'success': f'Subscribed quotes for {msg["symbols"]}'}))
                if msg_channel in ('timesale', 'all'):
                    self.clients[client]['timesale'].update(msg['symbols'])
                    await self.send_json(writer, json.dumps({'type': 'success',
                                                             'Success': f'Subscribed timesale for {msg["symbols"]}'}))
                if msg_channel in ('indicators', 'all'):
                    self.clients[client]['indicator'].update(msg['symbols'])
                    await self.send_json(writer, json.dumps({'type': 'success',
                                                             'success': f'Subscribed indicators for {msg["symbols"]}'}))
            else:
                if msg_channel in ('prices', 'all'):
                    self.clients[client]['quote'].difference_update(msg['symbols'])
                    await self.send_json(writer, json.dumps({'type': 'success',
                                                             'success': f'Unsubscribed prices for {msg["symbols"]}'}))
                if msg_channel in ('timesale', 'all'):
                    self.clients[client]['timesale'].difference_update(msg['symbols'])
                    await self.send_json(writer, json.dumps({'type': 'success',
                                                             'success': f'Subscribed timesale for {msg["symbols"]}'}))
                if msg_channel in ('indicators', 'all'):
                    self.clients[client]['indicator'].difference_update(msg['symbols'])
                    await self.send_json(writer, json.dumps({'type': 'success',
                                                             'success': f'Unsubscribed indicators for {msg["symbols"]}'}))
    
    async def on_connect(self, reader, writer):
        try:
            client = str(writer.get_extra_info('sockname'))
            logger.info(f"Client connected on {client}")
            self.clients[client] = {
                'queue': asyncio.Queue(),
                'quote': set(),
                'timesale': set(),
                'indicator': set()
            }
            client_handler_tasks = {
                                    asyncio.create_task(self.request_handler(client, reader, writer), name=f'{client}_request_handler'), 
                                    asyncio.create_task(self.msg_handler(client, writer), name=f'{client}_msg_handler')
                                }
            await asyncio.wait(client_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            logger.info(f"Client disconnected on {client}")
            self.clients.pop(client, None) #TODO: do we need to wait for queue to empty before popping?
            for task in client_handler_tasks:
                logger.debug(f"Cancelling task: {task.get_name()}")
                task.cancel()
            await asyncio.gather(*client_handler_tasks, return_exceptions=True)
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
                self.clients.clear()
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