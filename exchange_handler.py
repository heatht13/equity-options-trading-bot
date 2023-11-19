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

SUB_HANDLER_INTERVAL_SECS = 2

class ExchangeHandler:
    def __init__(self, exchange, **kwargs):
        self.exchange = exchange
        self.client = {
            'queue': asyncio.Queue(),
            'position': 0,
            'order': False
        }
        self.rest_session = None
        self.ws_session = None
        pass

    async def get_accounts(self, **kwargs):
        raise NotImplementedError
    
    async def get_balances(self, **kwargs):
        raise NotImplementedError
    
    async def get_positions(self, **kwargs):
        raise NotImplementedError

    async def place_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type, exp=None, strike=None, callput=None):
        raise NotImplementedError
    
    async def modify_order(self, order_id, order_type, price, quantity, tif):
        raise NotImplementedError
    
    async def cancel_order(self, order_id):
        raise NotImplementedError
    
    async def get_orders(self, order_id):
        raise NotImplementedError
    
    async def get_options_chains(self, underlying, expiration):
        raise NotImplementedError
    
    def parse_msg(self, msg):
        raise NotImplementedError
    
    async def ws_handler(self):
        raise NotImplementedError
    
    async def send_msg(self, msg):
        await self.client['queue'].put(msg)
        
    async def handle_msg(self, msg):
        msg = self.parse_msg(msg)
        if msg is None:
            return
        await self.send_msg(msg)

    async def exchange_ws_handler(self):
        while True:
            try:
                while self.client['order']:
                    await self.ws_handler()
            finally:
                await asyncio.sleep(SUB_HANDLER_INTERVAL_SECS)

    async def exchange_position_handler(self):
        while True:
            try:
                while self.client['position'] > 0:
                    positions = await self.get_positions()
                    msg = {
                        'type': 'update',
                        'channel': 'positions',
                        'timestamp': datetime.utcnow().timestamp(),
                        'data': positions
                    }
                    await self.send_msg(msg)
                    await asyncio.sleep(self.client['position'])
            finally:
                await asyncio.sleep(SUB_HANDLER_INTERVAL_SECS)

    async def exchange_handler_main(self):
        while True:
            try:
                logger.info("Exchange Handler Starting")
                exchange_handler_tasks = {
                                        asyncio.create_task(self.exchange_ws_handler(), name=f'{self.exchange}_exchange_ws_handler'),
                                        asyncio.create_task(self.exchange_position_handler(), name=f'{self.exchange}_exchange_position_handler')
                                    }
                await asyncio.wait(exchange_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in exchange_handler_tasks:
                    task.cancel()
                await asyncio.gather(*exchange_handler_tasks, return_exceptions=True)
                self.client['position'] = 0
                self.client['order'] = False
                if self.rest_session and not self.rest_session.closed:
                    await self.rest_session.close()
                    self.rest_session = None
                if self.ws_session and not self.ws_session.closed:
                    await self.ws_session.close()
                    self.ws_session = None
                logger.info("Exchange Handler Shutting Down")

class ExchangeSocketServer:
    MSG_LENGTH_PREFIX_BYTES=4
    def __init__(self, socket, exchange, **exchange_handler_kwargs):
        self.socket = socket
        self.exchange = exchange
        exchange_handler_kwargs['exchange'] = exchange
        self.exchange_handler_kwargs = exchange_handler_kwargs

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
            msg = await self.exchange_handler.client['queue'].get()
            await self.send_json(writer, json.dumps(msg))
            self.exchange_handler.client['queue'].task_done()

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
            response = None
            if msg_type == 'request':
                channel = msg.get('channel', None)
                if channel == 'accounts':
                    response = await self.exchange_handler.get_accounts()
                elif channel == 'positions':
                    response = await self.exchange_handler.get_positions()
                elif channel == 'balances':
                    response = await self.exchange_handler.get_balances()
                elif channel == 'new_order':
                    response = await self.exchange_handler.place_order(**msg['order'])
                elif channel == 'get_order':
                    response = await self.exchange_handler.get_orders(order_id=msg.get('order_id', None))
                elif channel == 'cancel_order':
                    if 'order_id' not in msg:
                        response = {'error': 'Invalid message. Must specify order_id'}
                    else:
                        response = await self.exchange_handler.cancel_order(order_id=msg['order_id'])
                elif channel == 'options_chains':
                    response = await self.exchange_handler.get_options_chains(**msg['data'])
                else:
                    response = {'error': 'Invalid message channel. Must be either \'accounts\', \'new_order\', \'get_order\', \'cancel_order\', or \'options_chains\''}
            else:
                if msg_type == 'subscribe':
                    channels = msg.get('channels', [])
                    for channel in channels:
                        if channel == 'positions':
                            self.exchange_handler.client['position'] = int(msg['interval'])
                            response = {'success': 'Subscribed positions'}
                        elif channel == 'orders':
                            self.exchange_handler.client['order'] = True
                            response = {'success': 'Subscribed order events'}
                        elif channel == 'all':
                            self.exchange_handler.client['position'] = int(msg['interval'])
                            self.exchange_handler.client['order'] = True
                            response = {'success': 'Subscribed positions and order events'}
                        else:
                            response = {'error': 'Invalid message channel. Must be either \'positions\', \'orders\', or \'all\''}
                elif msg_type == 'unsubscribe':
                    channels = msg.get('channels', [])
                    for channel in channels:
                        if channel == 'positions':
                            self.exchange_handler.client['position'] = 0
                            response = {'success': 'Unsubscribed positions'}
                        elif channel == 'orders':
                            self.exchange_handler.client['order'] = False
                            response = {'success': 'Unsubscribed order events'}
                        elif channel == 'all':
                            self.exchange_handler.client['position'] = 0
                            self.exchange_handler.client['order'] = False
                            response = {'success': 'Unsubscribed positions and order events'}
                        else:
                            response = {'error': 'Invalid message channel. Must be either \'positions\', \'orders\', or \'all\''}
                else:
                    response = {'error': 'Invalid message type. Must be either \'subscribe\', \'unsubscribe\'. or \'request\''}

            if response is not None:
                msg = {
                    'type': 'response',
                    'channel': channel,
                    'timestamp': datetime.utcnow().timestamp(),
                    'data': response
                }
                await self.send_json(writer, json.dumps(msg))
        
    async def on_connect(self, reader, writer):
        try:
            client = str(writer.get_extra_info('sockname'))
            logger.info(f"Client {client} connected")
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
            self.exchange_handler.client['position'] = 0
            self.exchange_handler.client['order'] = False
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionResetError:
                pass

    async def server_task(self):
        while True:
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
                logger.info("Exchange Starting")
                self.running = True
                self.exchange_handler = getattr(import_module(f'exchange_handlers.{self.exchange}_exchange_handler'),
                                            f'{self.exchange.capitalize()}ExchangeHandler')(**self.exchange_handler_kwargs)
                exchange_handler_tasks = {
                                    asyncio.create_task(self.exchange_handler.exchange_handler_main(), name=f'{self.exchange} Exchange Handler'),
                                    asyncio.create_task(self.server_task(), name=f'Exchange Socket Server')
                                }
                await asyncio.wait(exchange_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                logger.info("Exchange Shutting Down")
                self.running = False
                for task in exchange_handler_tasks:
                    logger.info(f"Cancelling task: {task.get_name()}")
                    task.cancel()
                await asyncio.gather(*exchange_handler_tasks, return_exceptions=True)

def main():
    parser = ArgumentParser()
    exchange_handler_args = parser.add_argument_group("Exchange Handler", "Exchange Handler parameters")
    exchange_handler_args.add_argument('--socket', type=str, default='/tmp/exchange.sock', help="Path to unix domain socket responsible for serving exchange related requests")
    exchange_handler_args.add_argument('--exchange', type=str, default='tradier', help="Exchange to connect to")
    credentials = parser.add_argument_group("Credentials", "Exchange API Credentials")
    credentials.add_argument('--account-id', type=str, default=None, help="API account id")
    credentials.add_argument('--access-token', type=str, default=None, help="API access token")
    args = parser.parse_args()
    kwargs = vars(args)
    server = ExchangeSocketServer(**kwargs)
    asyncio.run(server.main_task())

if __name__ == '__main__':
    main()