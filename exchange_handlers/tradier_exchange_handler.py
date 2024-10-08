import aiohttp
import asyncio
import re
import json
import logging
from argparse import ArgumentParser
from datetime import datetime

from exchange_handler import ExchangeHandler

POSITIONS_HANDLER_INTERVAL_SECS = 2
OCC_REGEX = r'(\d{6})([PC])(\d{8})'

class TradierExchangeHandler(ExchangeHandler):
    def __init__(self, account_id, access_token, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.access_token = access_token
        self.session_id = None
        self.rest_url = 'https://api.tradier.com'
        self.ws_uri = 'wss://ws.tradier.com/v1/accounts/events'

        self.endpoints = {
            'accounts': f'/v1/user/profile',
            'balances': f'/v1/accounts/{self.account_id}/balances',
            'positions': f'/v1/accounts/{self.account_id}/positions',
            'orders': f'/v1/accounts/{self.account_id}/orders',
            'options_chains': '/v1/markets/options/chains',
            'create_session': '/v1/accounts/events/session'
        }

    async def rest_query(self, method, endpoint, headers=None, data=None, params=None):
        if self.rest_session is None:
            self.rest_session = aiohttp.ClientSession()
        headers = (headers if headers is not None else {'Authorization': f'Bearer {self.access_token}',
                                                        'Accept':'application/json'})
        uri = self.rest_url + endpoint
        async with self.rest_session.request(method, uri, headers=headers, data=data, params=params) as response:
            if response.status // 100 != 2:
                self.logger.exception(f'Error {response.status} on {method} {endpoint}: {response.reason}')
                return response.text
            return await response.json()
        
    async def get_session_id(self):
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        resp = await self.rest_query('POST', self.endpoints['create_session'], headers=headers)
        session_id = resp.get('stream', {}).get('sessionid', None)
        if session_id is None:
            self.logger.error(f'Failed to get session id: {resp}')
            return
        self.session_id = session_id

    async def get_accounts(self, account_id=None):
        accounts = await self.rest_query('GET', self.endpoints['accounts'])
        return accounts
    
    async def get_balances(self):
        balances = await self.rest_query('GET', self.endpoints['balances'])
        return balances
    
    async def get_positions(self):
        def parse_position(pos):
            position = {
                'symbol': pos['symbol'],
                'quantity': float(pos['quantity']),
                'cost_basis': float(pos['cost_basis']),
                'side': 'long' if float(pos['quantity']) > 0 else 'short',
                'date_acquired': float(datetime.strptime(pos['date_acquired'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()),
            }
            symbol = re.search(OCC_REGEX+'$', pos['symbol'])
            if symbol is None:
                position['asset_class'] = 'stock'
            else:
                position['asset_class'] = 'option'
                position['expiration'] = symbol.group(1)
                position['strike'] = float(symbol.group(3))/10**3
                position['callput'] = 'call' if symbol.group(2) == 'C' else 'put'
                position['underlying'] = str(position['symbol']).split(symbol.group(1))[0]
            return position
        positions_resp = await self.rest_query('GET', self.endpoints['positions'])
        positions = dict()
        if positions_resp['positions'] == 'null':
            return positions
        if isinstance(positions_resp['positions']['position'], list):
            for pos in positions_resp['positions']['position']:
                position = parse_position(pos)
                symbol = position['underlying'] if position['asset_class'] == 'option' else position['symbol']
                positions[symbol] = position
        else:
            position = parse_position(positions_resp['positions']['position'])
            symbol = position['underlying'] if position['asset_class'] == 'option' else position['symbol']
            positions[symbol] = position
        return positions

    async def get_orders(self, order_id=None):
        order = '' if order_id is None else f'/{order_id}'
        orders = await self.rest_query('GET', self.endpoints['orders']+order)
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
            #'price': price #NOTE: Decision Engine is passing underlying price. Dont want to use that when placing option orders
        }

        if asset_class == 'option':
            option_type = 'C' if callput == 'call' else 'P' if callput == 'put' else None
            strike = f"{int(float(strike)*10**3):0>{8}}"
            data['option_symbol'] = symbol+exp+option_type+strike

            if side == 'buy':
                direction = "buy_to_open" if offset == 'open' else "buy_to_close"
            else: # side == 'sell'
                direction = "sell_to_open" if offset == 'open' else "sell_to_close"
            data['side'] = direction
            data['quantity'] = "1" #NOTE: tradier says number of shares, but we need to test #str(int(quantity)*100)
        
        self.logger.info(f"New Order: {json.dumps(data, indent=4)}")
        response = await self.rest_query('POST', self.endpoints['orders'], data=data)
        self.logger.info(f"NEW ORDER RESP: {json.dumps(response, indent=4)}")
        return response

    async def modify_order(self, order_id, order_type, price, quantity, tif):
        raise NotImplementedError

    async def cancel_order(self, order_id):
        response = await self.rest_query('DELETE', self.endpoints['orders']+'/'+order_id)
        return response
    
    async def get_options_chains(self, underlying, expiration, max_price, min_price):
        if '-' not in expiration:
            exp = datetime.strptime(expiration, '%y%m%d')
            exp = exp.strftime('%Y-%m-%d')
        data = {
            'symbol': str(underlying),
            'expiration': exp,
            'greeks': 'false',
        }
        chains = dict()
        chains_resp = await self.rest_query('GET', self.endpoints['options_chains'], params=data)
        if chains_resp['options'] is not None:
            calls = []
            puts = []
            for chain in chains_resp['options']['option']:
                if chain['last'] is None:
                    continue
                last = float(chain['last'])
                if last > max_price or last < min_price:
                    continue
                strike = float(chain['strike'])
                symbol_data = {
                        'symbol': chain['underlying'],
                        'option_symbol': chain['symbol'],
                        'strike': float(chain['strike']),
                        'expiration': chain['expiration_date'],
                        'callput': chain['option_type'],
                        'last': last,
                        'open_interest': float(chain['open_interest']),
                    }
                if chain['option_type'] == 'call':
                    calls.append(symbol_data)
                else:
                    puts.append(symbol_data)
            # calls = {k: v for k, v in sorted(calls.items(), key=lambda x: x[1]['volume'], reverse=True)}
            # puts = {k: v for k, v in sorted(puts.items(), key=lambda x: x[1]['volume'], reverse=True)}
            calls = sorted(calls, key=lambda x: x['open_interest'], reverse=True)
            puts = sorted(puts, key=lambda x: x['open_interest'], reverse=True)
            chains['call'] = calls
            chains['put'] = puts
        response = {
            'underlying': underlying,
            'expiration': expiration,
            'options_chain': chains
        }
        return response

    def parse_msg(self, msg):
        channel = msg['event']
        if channel == 'order':
            #2023-12-27 11:31:11,911 [INFO] [tradier_exchange_handler.py:242]: {'id': 44586343, 'event': 'order', 'status': 'filled', 'type': 'market', 'price': 0.0, 'last_fill_price': 0.95, 'stop_price': 0.0, 'avg_fill_price': 0.95, 'exec_quantity': 1.0, 'last_fill_quantity': 1.0, 'remaining_quantity': 0.0, 'transaction_date': '2023-12-27T16:31:11.769Z', 'create_date': '2023-12-27T16:31:11.626Z', 'account': '6YA38565'}
            data = {
                # 'order_id': str(msg['id']),
                # 'status': msg['status'],
                # 'type': msg['type'],
                # 'price': float(msg['price']),
                # 'avg_fill_price': float(msg['avg_fill_price']),
                # 'exec_quantity': float(msg['exec_quantity']),
                # 'last_fill_quantity': float(msg['last_fill_quantity']),
                # 'remaining_quantity': float(msg['remaining_quantity']),
                # 'trade_time': float(datetime.strptime(msg['transaction_date'], '%Y-%m-%dT%H:%M:%SZ').timestamp()),
                # 'create_time': float(datetime.strptime(msg['create_date'], '%Y-%m-%dT%H:%M:%SZ').timestamp()),
                # 'account_id': msg['account']
            }
        else:
            return None
        return {
                'handler': 'exchange',
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
                            self.logger.info('Exchange WS Handler Started')
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
                                if 'event' in msg:
                                    if msg['event'] == 'order':
                                        self.logger.info(msg)
                                        #await self.handle_msg(msg)
                                    elif msg['event'] == 'heartbeat':
                                        pass
                                    else:
                                        self.logger.info(f'Unhandled message type: {msg}')
                                elif 'error' in msg:
                                    if 'session' in msg['error']:
                                        self.logger.info(f"Session expired; resetting. {msg}")
                                        self.session_id = None
                                        break
                                    else:
                                        self.logger.error(f"Received unhandled error msg: {msg}")
                                else:
                                    self.logger.warning(f'Unknown message: {msg}')
                                if not self.client['order']:
                                    break
                        finally:
                            self.logger.info(f"Exchange WS Handler Shutting Down")
            except (aiohttp.ClientError, aiohttp.WSServerHandshakeError, ConnectionResetError) as e:
                self.logger.error(f"websocket connection closed; resetting. {e}")
            finally:
                if self.ws_session and not self.ws_session.closed:
                    await self.ws_session.close()
                    self.ws_session = None

    def shutdown(self):
        self.running = False