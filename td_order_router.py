import aiohttp
from aiohttp import web
import asyncio
import json
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime

#TODO: separate TD order router from base order router class
class TDAOrderRouter():
    def __init__(self, execution_path, port, decision_engine_uri, account_id, user_id, consumer_key, refresh_token, **args):
        super().__init__(**args)
        self.execution_path = execution_path
        self.port = port
        self.decision_engine_ws = aiohttp.ClientSession().ws_connect(decision_engine_uri)
        self.account_id = account_id
        self.user_id = user_id
        self.consumer_key = consumer_key
        self.refresh_token = refresh_token
        self.access_token = None
        self.access_token_exp = None
        self.principals = None
        self.session = None
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
        if self.session is None:
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

        #TODO: need to handle balances, positions better here
        for account in accounts:
            if account['accountId'] == self.account_id:
                positions = account['positions']
                balances = account['currentBalances']
                self.decision_engine_ws.send_str(json.dumps({'account':account}))
        return accounts
    
    async def get_orders(self, order_id=None):
        order = '' if order_id is None else f'/{order_id}'
        orders = await self.rest_query('post', self.td_uri+self.endpoints['orders']+order)
        self.decision_engine_ws.send_str(json.dumps({'orders':orders}))
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
    
    async def order_handler_main(self):
        app = web.Application()
        app.add_routes([web.post('/order', self.place_order)])
        app.add_routes([web.delete('/order', self.cancel_order)])
        app.add_routes([web.get('/order', self.get_orders)])
        app.add_routes([web.get('/accounts', self.get_accounts)])
        web.run_app(app, port=self.port)

    
def main():
    parser = ArgumentParser()
    order_router_args = parser.add_argument_group("OrderRouter", "Order Router parameters")
    order_router_args.add_argument('--execution_path', type=str, default='https://localhost', help="Path to order router")
    order_router_args.add_argument('--port', type=str, default='8082', help="Port to run the order router on")
    decision_engine_args = parser.add_argument_group("DecisionEngine", "Decision Engine parameters")
    decision_engine_args.add_argument('--decision_engine_uri', type=str, default='https://localhost:8080', help="URI of decision engine")
    credentials = parser.add_argument_group("Credentials", "TD account owners API credentials")
    credentials.add_argument('--account_id', type=str, default=None, help="API account id")
    credentials.add_argument('--user_id', type=str, default=None, help="API user id")
    credentials.add_argument('--consumer_key', type=str, default=None, help="API application consumer key")
    credentials.add_argument('--refresh_token', type=str, default=None, help="API refresh token")
    args = parser.parse_args()
    order_router = TDAOrderRouter(**args)
    asyncio.run(order_router.order_handler_main())

if __name__ == '__main__':
    main()

ORDERROUTER = TDAOrderRouter