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

class TradierExchangeHandler(ExchangeHandler):
    def __init__(self, account_id, access_token, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.access_token = access_token
        self.session_id = None
        self.rest_url = 'https://api.tradier.com'
        self.ws_uri = 'wss://ws.tradier.com/v1/markets/events'

        self.endpoints = {
            'accounts': f'/v1/user/profile',
            'balances': f'/v1/accounts/{self.account_id}/balances',
            'positions': f'/v1/accounts/{self.account_id}/positions',
            'orders': f'/v1/accounts/{self.account_id}/orders',
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

    async def get_accounts(self, account_id=None):
        accounts = await self.rest_query('get', self.rest_url+self.endpoints['accounts'])
        return accounts
    
    async def get_balances(self):
        balances = await self.rest_query('get', self.rest_url+self.endpoints['balances'])
        return balances
    
    async def get_positions(self):
        positions = await self.rest_query('get', self.rest_url+self.endpoints['positions'])
        return positions

    async def get_orders(self, order_id=None):
        order = '' if order_id is None else f'/{order_id}'
        orders = await self.rest_query('get', self.rest_url+self.endpoints['orders']+order)
        return orders
    
    async def place_order(self, symbol, order_type, side, offset, price, quantity, tif, asset_class='option', exp=None, strike=None, callput=None):
        #    Currently, we assume all orders are options orders. ie. quantity denom. in num of contracts

        symbol = symbol.upper()

        #Order Type
        if order_type not in ('market', 'limit', 'stop', 'stop_limit'):
            raise ValueError(f'Invalid order_type {order_type}')

        #Order Direction
        if side == 'buy':
            direction = "buy" if offset == 'open' else "buy_to_cover"
        else: # side == 'sell'
            direction = "sell_short" if offset == 'open' else "sell"

        price = str(price)
        quantity = str(quantity)

        #Time in Force
        if tif not in ('gtc', 'day', 'pre', 'post'):
            raise ValueError(f'Invalid time in force {tif}')

        data = {
            'class': asset_class,
            'symbol': symbol,
            'side': direction,
            'quantity': quantity,
            'type': order_type,
            'duration': tif,
            'price': price
        }

        if asset_class == 'option':
            option_type = 'C' if callput == 'call' else 'P'
            strike = f"{int(strike*10**3):0>{8}}"
            data['option_symbol'] = symbol+exp+option_type+strike

            if side == 'buy':
                direction = "buy_to_open" if offset == 'open' else "buy_to_close"
            else: # side == 'sell'
                direction = "sell_to_open" if offset == 'open' else "sell_to_close"
            data['side'] = direction
            data['quantity'] = "1" #NOTE: tradier says number of shares, but we need to test #str(int(quantity)*100)
        
        response = await self.rest_query('post', self.rest_url+self.endpoints['orders'], json=data)
        return response

    async def modify_order(self, order_id, order_type, price, tif):
        raise NotImplementedError

    async def cancel_order(self, order_id):
        response = await self.rest_query('delete', self.rest_url+self.endpoints['orders']+order_id)
        return response

    def parse_msg(self, msg):
        channel = msg['event']
        if channel == 'order':
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