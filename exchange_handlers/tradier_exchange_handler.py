import aiohttp
import asyncio
import json
import logging
from argparse import ArgumentParser
from datetime import datetime

from exchange_handler import ExchangeHandler

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
class TradierExchangeHandler(ExchangeHandler):
    def __init__(self, account_id, access_token, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.access_token = access_token
        self.session_id = None
        self.rest_url = 'https://api.tradier.com'
        self.ws_uri = 'wss://ws.tradier.com/v1/markets/events'

        #NOTE: Use Request type to determine how the order endpoint is used. Order ID
        #    must be specified at end of endpoint. Saved Orders can be used as well:
        #    https://developer.tdameritrade.com/account-access/apis
        #    - GET Request: retrieves the order's information/status. Can query in various ways. ref docs
        #    - POST Request: places a new order. An order_id is returned
        #    - DELETE Request: cancels the order
        #    - PUT Request: replaces the order
        self.endpoints = {
            # 'oath_token': '/oauth2/token',
            # 'user_principals': '/userprincipals',
            # 'orders': f'/accounts/{self.account_id}/orders',
            # 'get_accounts': '/accounts',
            'positions': f'/v1/accounts/{self.account_id}/positions',
            'balances': f'/v1/accounts/{self.account_id}/balances'
        }

    async def rest_query(self,method, endpoint, headers=None, json=None):
        if self.rest_session is None:
            self.rest_session = aiohttp.ClientSession()
        headers = (headers if headers is not None
                        else {'Authorization': f'Bearer {self.access_token}',
                        'Accept':'application/json'})
        async with self.rest_session.request(method, endpoint, headers=headers, json=json) as response:
            if response.status // 100 != 2:
                raise Exception(f'Error {response.status} on {method} {endpoint}: {response.reason}')
            return await response.json()
        
    async def get_session_id(self):
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        resp = await self.rest_query('POST', self.endpoints['create_session'], headers=headers)
        session_id = resp.get('stream', {}).get('sessionid', None)
        if session_id is None:
            logger.error(f'Failed to get session id: {resp}')
            return
        self.session_id = session_id

    # async def get_accounts(self, account_id=None):
    #     account = '' if account_id is None else f'/{account_id}'
    #     accounts = await self.rest_query('get', self.td_uri+self.endpoints['get_accounts']+account)

    #     #TODO: need to support multiple accounts
    #     # positions = {}
    #     # for account in accounts:
    #     #     if account['accountId'] == self.account_id:
    #     #         positions = account['positions']
    #     return accounts
    
    async def get_balances(self):
        balances = await self.rest_query('get', self.rest_url+self.endpoints['balances'])
        return balances
    
    async def get_positions(self):
        positions = await self.rest_query('get', self.rest_url+self.endpoints['positions'])
        return positions

    # async def get_orders(self, order_id=None):
    #     order = '' if order_id is None else f'/{order_id}'
    #     orders = await self.rest_query('post', self.td_uri+self.endpoints['orders']+order)
    #     return orders
    
    # async def place_order(self, symbol, order_type, side, price, quantity, offset, tif, asset_type):
    #     #NOTE: ref https://developer.tdameritrade.com/content/place-order-samples for more information.
    #     #    Currently, we assume all orders are options orders. ie. quantity denom. in num of contracts

    #     #Order Type
    #     if order_type not in ('market', 'limit', 'stop', 'stop_limit, trailing_stop'):
    #         raise ValueError(f'Invalid order_type {order_type}')
    #     order_type = order_type.upper()

    #     #Order Direction
    #     if side == 'buy':
    #         offset = "BUY_TO_OPEN" if offset == 'open' else "BUY_TO_CLOSE"
    #     else: # side == 'sell'
    #         offset = "SELL_TO_OPEN" if offset == 'open' else "SELL_TO_CLOSE"

    #     #Time in Force
    #     if tif not in ('gtc', 'day', 'fok'):
    #         raise ValueError(f'Invalid time in force {tif}')
    #     tif = 'GOOD_TILL_CANCEL' if tif == 'gtc' else 'DAY' if tif == 'day' else 'FILL_OR_KILL'

    #     asset_type = 'OPTION'

    #     data = {
    #         "complexOrderStrategyType": "NONE",
    #         "orderType": order_type,
    #         "session": "NORMAL",
    #         "price": str(price),
    #         "duration": tif,
    #         "orderStrategyType": "SINGLE",
    #         "orderLegCollection": [
    #             {
    #             "instruction": offset,
    #             "quantity": str(quantity),
    #             "instrument": {
    #                 "symbol": symbol,
    #                 "assetType": asset_type
    #                 }
    #             }
    #         ]
    #     }
    #     response = await self.rest_query('post', self.td_uri+self.endpoints['orders'], json=data)
    #     return response

    # async def cancel_order(self, order_id):
    #     response = await self.rest_query('delete', self.td_uri+self.endpoints['orders']+order_id)
        # return response

    def parse_msg(self, msg):
        channel = msg['event']
        if channel == 'order':
            if msg['cancel'] or msg['correction'] or msg['seq'] <= self.last_ts_seq:
                return None
            data = {
                'order_id': str(msg['id']),
                'status': msg['status'],
                'type': msg['type'],
                'price': float(msg['price']),
                'avg_fill_price': float(msg['avg_fill_price']),
                'exec_quantity': float(msg['exec_quantity']),
                'last_fill_quantity': float(msg['last_fill_quantity']),
                'remaining_quantity': float(msg['remaining_quantity']),
                'trade_time': float(datetime.strptime(msg['transaction_date'], '%Y-%m-%dT%H:%M:%SZ').timestamp()),
                'create_time': float(datetime.strptime(msg['create_date'], '%Y-%m-%dT%H:%M:%SZ').timestamp()),
                'account_id': msg['account']
            }
        else:
            return None
        return {
                'handler': 'data',
                'type': 'update',
                'channel': channel,
                'timestamp': datetime.utcnow().timestamp(),
                'data': data
            }
        
    async def ws_handler(self):
        while self.client['order']:
            try:
                async with aiohttp.ClientSession() as self.ws_session:
                    async with self.ws_session.ws_connect(self.ws_uri, ssl=True) as ws:
                        try:
                            logger.info('Exchange WS Handler Started')
                            if self.session_id is None:
                                await self.get_session_id()
                            sub_events = {
                                "events": ["order"],
                                "sessionid": self.session_id,
                                "excludeAccounts": []
                            }
                            await ws.send_str(json.dumps(sub_events))
                            async for msg in ws:
                                msg = msg.json()
                                logger.info(msg)
                                if 'event' in msg:
                                    if msg['event'] == 'order':
                                        await self.handle_msg(msg)
                                    else:
                                        logger.info(f'Unhandled message type: {msg}')
                                elif 'error' in msg:
                                    if 'session' in msg['error']:
                                        logger.info(f"Session expired; resetting. {msg}")
                                        self.session_id = None
                                        break
                                    else:
                                        logger.error(f"Received unhandled error msg: {msg}")
                                else:
                                    logger.warning(f'Unknown message: {msg}')
                                if not self.client['order']:
                                    break
                        finally:
                            logger.info(f"Exchange WS Handler Dhutting Down")
            except (aiohttp.ClientError, aiohttp.WSServerHandshakeError, ConnectionResetError) as e:
                logger.error(f"websocket connection closed; resetting. {e}")
            finally:
                if self.ws_session and not self.ws_session.closed:
                    await self.ws_session.close()
                    self.ws_session = None

    def shutdown(self):
        self.running = False