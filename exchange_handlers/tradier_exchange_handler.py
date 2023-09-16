import aiohttp
from aiohttp import web
import asyncio
import json
import logging
from argparse import ArgumentParser
from datetime import datetime
from async_unix_socket import AsyncUnixSocketServer

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

POSITIONS_HANDLER_INTERVAL_SECS = 2

#TODO: separate TD order router from base order router class
class TDAExchangeHandler():
    def __init__(self, socket, account_id, user_id, consumer_key, refresh_token, **args):
        super().__init__(**args)
        self.socket = socket
        self.client_subscriptions = {
            'positions': '0'
        }
        self.account_id = account_id
        self.user_id = user_id
        self.consumer_key = consumer_key
        self.refresh_token = refresh_token
        self.access_token = None
        self.access_token_exp = None
        self.principals = None
        self.td_uri = 'https://api.tdameritrade.com/v1'

        #NOTE: Use Request type to determine how the order endpoint is used. Order ID
        #    must be specified at end of endpoint. Saved Orders can be used as well:
        #    https://developer.tdameritrade.com/account-access/apis
        #    - GET Request: retrieves the order's information/status. Can query in various ways. ref docs
        #    - POST Request: places a new order. An order_id is returned
        #    - DELETE Request: cancels the order
        #    - PUT Request: replaces the order
        self.endpoints = {
            'oath_token': '/oauth2/token',
            'user_principals': '/userprincipals',
            'orders': f'/accounts/{self.account_id}/orders',
            'get_accounts': '/accounts',
        }

    async def rest_query(self,method, endpoint, headers=None, json=None):
        if self.rest_session is None:
            self.rest_session = aiohttp.ClientSession()
        headers = (headers if headers is not None
                                    else {'Authorization': f'Bearer {self.access_token}',
                                        'Content-Type':'application/json'})
        async with self.rest_session.request(method, endpoint, headers=headers, json=json) as response:
            if response.status // 100 != 2:
                raise Exception(f'Error {response.status} on {method} {endpoint}: {response.reason}')
            return await response.json()
    
    async def generate_token_from_refresh(self, refresh_token):
        headers = {'Content-Type':'application/x-www-form-urlencoded'}
        data = {
            'grant_type':'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.consumer_key,
        }
        response = await self.rest_query('post', self.td_uri+self.endpoints['oath_token'], headers, json=data)
        access_token = response['access_token']
        access_token_expiration = datetime.utcnow() + int(response['expires_in'])
        return access_token, access_token_expiration

    async def get_user_principals(self):
        data = {'fields' : "streamerSubscriptionKeys,streamerConnectionInfo"}
        response = await self.rest_query('get', self.td_uri+self.endpoints['user_principals'], json=data)
        return response
    
    async def get_accounts(self, account_id=None):
        account = '' if account_id is None else f'/{account_id}'
        accounts = await self.rest_query('get', self.td_uri+self.endpoints['get_accounts']+account)

        #TODO: need to support multiple accounts
        positions = {}
        for account in accounts:
            if account['accountId'] == self.account_id:
                positions = account['positions']
        return positions
    
    async def get_orders(self, order_id=None):
        order = '' if order_id is None else f'/{order_id}'
        orders = await self.rest_query('post', self.td_uri+self.endpoints['orders']+order)
        return orders
    
    async def place_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type):
        #NOTE: ref https://developer.tdameritrade.com/content/place-order-samples for more information.
        #    Currently, we assume all orders are options orders. ie. quantity denom. in num of contracts

        #Order Type
        if order_type not in ('market', 'limit', 'stop', 'stop_limit, trailing_stop'):
            raise ValueError(f'Invalid order_type {order_type}')
        order_type = order_type.upper()

        #Order Direction
        if side == 'buy':
            offset = "BUY_TO_OPEN" if offset == 'open' else "BUY_TO_CLOSE"
        else: # side == 'sell'
            offset = "SELL_TO_OPEN" if offset == 'open' else "SELL_TO_CLOSE"

        #Time in Force
        if tif not in ('gtc', 'day', 'fok'):
            raise ValueError(f'Invalid time in force {tif}')
        tif = 'GOOD_TILL_CANCEL' if tif == 'gtc' else 'DAY' if tif == 'day' else 'FILL_OR_KILL'

        asset_type = 'OPTION'

        data = {
            "complexOrderStrategyType": "NONE",
            "orderType": order_type,
            "session": "NORMAL",
            "price": str(price),
            "duration": tif,
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                "instruction": offset,
                "quantity": str(quantity),
                "instrument": {
                    "symbol": symbol,
                    "assetType": asset_type
                    }
                }
            ]
        }
        response = await self.rest_query('post', self.td_uri+self.endpoints['orders'], json=data)
        return response

    async def cancel_order(self, order_id):
        response = await self.rest_query('delete', self.td_uri+self.endpoints['orders']+order_id)
        return response
    
    async def positions_handler(self):
        while True:
            try:
                while self.client_subscriptions['positions'] != '0':
                    if self.access_token is None or self.access_token_exp < datetime.utcnow():
                        self.access_token, self.access_token_exp = await self.generate_token_from_refresh(self.refresh_token)
                    if self.server:
                        await self.server.send_str(json.dumps({'Positions Update': f'{await self.get_accounts()}'}))
                    else:
                        logger.error(f"Server not initialized")
                    await asyncio.sleep(int(self.client_subscriptions['positions']))
            except ConnectionError as e:
                pass
            finally:
                await asyncio.sleep(POSITIONS_HANDLER_INTERVAL_SECS)
    
    async def exchange_handler(self):
        logger.info(f"Exchange Handler Task started")
        try:
            while self.running:
                if self.access_token is None or self.access_token_exp < datetime.utcnow():
                    self.access_token, self.access_token_exp = await self.generate_token_from_refresh(self.refresh_token)
                self.server = AsyncUnixSocketServer(self.socket)
                async for msg in self.server.open():
                    msg = json.loads(msg)
                    logger.info("Received:", msg)
                    msg_type = msg.get('type', None)
                    if msg_type not in ('subscribe', 'unsubscribe', 'request'):
                        await self.server.send_str(json.dumps({'error': 'Invalid message type. Must be either \'subscribe\', \'unsubscribe\'. or \'request\''}))
                        continue
                    msg_channel = msg.get('channel', None)
                    if msg_type == 'request':
                        if msg_channel == 'accounts':
                            response = await self.get_accounts()
                        elif msg_channel == 'new_order':
                            response = await self.place_order(**msg['order'])
                        elif msg_channel == 'get_order':
                            response = await self.get_orders(order_id=msg.get('order_id', None))
                        elif msg_channel == 'cancel_order':
                            if 'order_id' not in msg:
                                await self.server.send_str(json.dumps({'error': 'Invalid message. Must specify order_id'}))
                            response = await self.cancel_order(order_id=msg['order_id'])
                        else:
                            await self.server.send_str(json.dumps({'error': 'Invalid message channel. Must be either \'accounts\', \'new_order\', \'get_order\', or \'cancel_order\''}))
                            continue
                        await self.server.send_str(json.dumps({'Response': f'{response}'}))
                    elif msg_type == 'subscribe':
                        if msg_channel == 'all':
                            self.client_subscriptions['positions'] = msg['interval']
                            await self.server.send_str(json.dumps({'Success': f'Subscribed positions'}))
                        channel = self.client_subscriptions.get(msg_channel)
                        if channel:
                            self.client_subscriptions[msg_channel] = msg['interval']
                            await self.server.send_str(json.dumps({'Success': f'Subscribed {msg_channel}'}))
                        else:
                            await self.server.send_str(json.dumps({'error': 'Invalid message channel. Must be either \'positions\' or \'all\''}))
                    else:
                        if msg_channel == 'all':
                            self.client_subscriptions['positions'] = '0'
                            await self.server.send_str(json.dumps({'Success': f'Unsubscribed positions'}))
                        channel = self.client_subscriptions.get(msg_channel)
                        if channel:
                            self.client_subscriptions[msg_channel] = '0'
                            await self.server.send_str(json.dumps({'Success': f'Unsubscribed {msg_channel}'}))
                        else:
                            await self.server.send_str(json.dumps({'error': 'Invalid message channel. Must be either \'positions\' or \'all\''}))
        except ConnectionError as e:
            logger.error(f"Data Handler Connection Error; resetting. {e}")
        finally:
            if self.rest_session and not self.rest_session.closed:
                await self.rest_session.close()
                self.rest_session = None
            if self.server:
                await self.server.close()
                self.server = None
            logger.info(f"Exchange Handler Task shut down")

    def shutdown(self):
        self.running = False

    async def exchange_handler_main(self):
        logger.info("Exchange Handler starting")
        try:
            self.running = True
            exchange_handler_tasks = set()
            exchange_handler_tasks.add(asyncio.create_task(self.exchange_handler()).add_done_callback(exchange_handler_tasks.discard))
            exchange_handler_tasks.add(asyncio.create_task(self.positions_handler()).add_done_callback(exchange_handler_tasks.discard))
            while self.running:
                done, _ = await asyncio.wait(exchange_handler_tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    exchange_handler_tasks.add(asyncio.create_task(task.get_coro()))
        finally:
            for task in exchange_handler_tasks:
                task.cancel()
            await asyncio.gather(*exchange_handler_tasks, return_exceptions=True)
            logger.info("Exchange Handler shutting down")

def main():
    parser = ArgumentParser()
    order_router_args = parser.add_argument_group("Exchange Handler", "Exchange Handler parameters")
    order_router_args.add_argument('--socket', type=str, default='/tmp/exchange.sock', help="Path to unix domain socket responsible for serving exchange related requests")
    credentials = parser.add_argument_group("Credentials", "TD account owners API credentials")
    credentials.add_argument('--account_id', type=str, default=None, help="API account id")
    credentials.add_argument('--user_id', type=str, default=None, help="API user id")
    credentials.add_argument('--consumer_key', type=str, default=None, help="API application consumer key")
    credentials.add_argument('--refresh_token', type=str, default=None, help="API refresh token")
    args = parser.parse_args()
    args = vars(args)
    exchange_handler = TDAExchangeHandler(**args)
    asyncio.run(exchange_handler.exchange_handler_main())

if __name__ == '__main__':
    main()