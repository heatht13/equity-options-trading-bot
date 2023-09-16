import json
import asyncio
from logging import Logger
from datetime import datetime
from argparse import ArgumentParser
from importlib import import_module
from collections import deque

logger = Logger(__name__)

MA = {'sma', 'ema'}
OHLC = {'o': 0, 'h': 0, 'l': 0, 'c': 0, 't': 0}
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
EXCHANGES = {'tradier'}

class MALookbackDataParser():
    def __init__(self, clients, timeframe, symbols, indicator, period, lookback):
        self.clients = clients
        self.timeframe = TIMEFRAMES[timeframe]
        self.symbols = symbols
        self.indicator = indicator
        self.period = int(period)
        self.lookback_period = int(lookback)
        self.ma_queue = deque(maxlen=self.period)
        self.lookback_queue = deque(maxlen=self.lookback_period)
        self.ohlc = OHLC

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
    
    def update_indicators(self, msg):
        symbol = msg['symbol']
        msg_time = msg['timestamp']
        price = msg['data']['price']
        size = msg['data']['size']
        trade_time = msg['data']['trade_time']
        #TODO: Need to confirm msgs are in order and correspond to current candle
        if price > self.ohlc['h']:
            self.ohlc['h'] = price
        elif price < self.ohlc['l']:
            self.ohlc['l'] = price
        #open candle: will need more precision.Currently seconds
        if trade_time % self.timeframe == 1:
            self.ohlc['o'] = price
            self.ohlc['h'] = price
            self.ohlc['l'] = price
            self.ohlc['c'] = price
            self.ohlc['t'] = self.timeframe
        #close candle: will need more precision. Currently seconds
        if trade_time % self.timeframe == 0:
            self.ohlc['c'] = price
            ma = self.sma(self.ohlc, self.period) if self.indicator == 'sma' else self.ema(self.ohlc, self.period)
            lookback_high, lookback_low = self.lookback(self.ohlc, self.lookback_period)
            msg = json.dumps({
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
                    'trade_time': trade_time
                }
            })
            self.ohlc = OHLC
            return msg
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
        await self.send_msg(msg)
        if msg['channel'] == 'trade':
            update = self.update_indicators(msg)
            if update is not None:
                await self.send_msg(update)

    def shutdown(self):
        self.running = False

    async def data_handler_main(self):
        logger.info("Data Handler Starting")
        while self.running:
            try:
                self.running = True
                data_handler_task = asyncio.create_task(self.stream_handler())
                await data_handler_task
            finally:
                logger.info("Data Handler Shutting Down")
                data_handler_task.cancel()
                try:
                    await data_handler_task
                except asyncio.CancelledError:
                    pass
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
        self.data_handler_kwargs = data_handler_kwargs.update({'clients': self.clients})

    async def send_str(self, writer, msg):
        if writer.is_closing():
            raise ConnectionError("Connection to client closing")
        msg = str(msg)
        message_length = len(msg)
        writer.write(message_length.to_bytes(self.MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
        writer.write(msg.encode('utf-8'))
        await writer.drain()

    async def msg_handler(self, client, writer):
        while True:
            msg = await self.clients[client]['queue'].get()
            await self.send_str(client, writer, msg)

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
            logger.info("Received:", msg)
            msg_type = msg.get('type', None)
            if msg_type not in ('subscribe', 'unsubscribe'):
                await self.send_str(writer, json.dumps({'error': 'Invalid message type. Must be either \'subscribe\' or \'unsubscribe\''}))
                continue
            msg_channel = msg.get('channel', None)
            if msg_channel not in ('prices', 'indicators', 'all'):
                await self.send_str(writer, json.dumps({'error': 'Invalid message channel. Must be either \'prices\', \'indicators\', or \'all\''}))
                continue
            if msg_type == 'subscribe':
                if msg_channel in ('prices', 'all'):
                    self.clients[client]['prices'].update(msg['symbols'])
                    await self.send_str(writer, json.dumps({'Success': f'Subscribed prices for {msg["symbols"]}'}))
                if msg_channel in ('indicators', 'all'):
                    self.clients[client]['indicators'].update(msg['symbols'])
                    await self.send_str(writer, json.dumps({'Success': f'Subscribed indicators for {msg["symbols"]}'}))
            else:
                if msg_channel in ('prices', 'all'):
                    self.clients[client]['prices'].difference_update(msg['symbols'])
                    await self.send_str(writer, json.dumps({'Success': f'Unsubscribed prices for {msg["symbols"]}'}))
                if msg_channel in ('indicators', 'all'):
                    self.clients[client]['indicators'].difference_update(msg['symbols'])
                    await self.send_str(writer, json.dumps({'Success': f'Unsubscribed indicators for {msg["symbols"]}'}))
    
    async def on_connect(self, reader, writer):
        try:
            client = writer.get_extra_info('sockname')
            self.clients[client] = {
                'queue': asyncio.Queue(),
                'prices': set(),
                'indicators': set()
            }
            client_handler_tasks = {
                                    asyncio.create_task(self.request_handler(client, reader, writer)), 
                                    asyncio.create_task(self.msg_handler(client, writer))
                                }
            await asyncio.wait(client_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            #TODO: Handle cancelling an already completed/cancelled task
            for task in client_handler_tasks:
                task.cancel()
            await asyncio.gather(*client_handler_tasks, return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            self.clients.pop(client, None) #TODO: do we need to wait for queue to empty before popping?
            self.data_handler.clients.pop(client, None)
    
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
        logger.info("Market Data Server Starting")
        self.running = True
        self.data_handler = getattr(import_module(f'data_handlers.{self.exchange}_data_handler'),
                                    f'{self.exchange.capitalize()}DataHandler')(**self.data_handler_kwargs)
        md_handler_tasks = {
                            asyncio.create_task(self.data_handler.data_handler_main()), 
                            asyncio.create_task(self.server_task())
                        }
        await asyncio.wait(md_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
            
def main():
    parser = ArgumentParser()
    signal_generator_args = parser.add_argument_group("Data Provider", "Data Provider parameters")
    signal_generator_args.add_argument('--socket', type=str, default='/tmp/data_provider.sock', help="Path to unix domain socket responsible for serving data")
    credentials = parser.add_argument_group("Credentials", "Credentials for data source")
    credentials.add_argument('--exchange', type=str, choices=EXCHANGES, help="Exchange to trade on")
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