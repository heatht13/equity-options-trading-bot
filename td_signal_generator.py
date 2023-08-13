import aiohttp
from aiohttp import web
import asyncio
from collections import deque
from logging import Logger
from argparse import ArgumentParser
from datetime import datetime, timezone
from urllib.parse import urlencode
import json

from signal_generator import MASignalGenerator, TIMEFRAMES, MA

logger = Logger(__name__)

class TDASignalGenerator(MASignalGenerator):
    def __init__(self, user_id, consumer_key, refresh_token, **args):
        super().__init__(**args)
        self.user_id = user_id
        self.consumer_key = consumer_key
        self.refresh_token = refresh_token
        self.access_token = None
        self.access_token_exp = None
        self.principals = None
        self.session = None
        self.td_uri = 'https://api.tdameritrade.com/v1'
        self.endpoints = {
            'oath_token': '/oauth2/token',
            'user_principals': '/userprincipals',
        }

    async def rest_query(self,method, endpoint, headers=None, json=None):
        if self.session is None:
            self.rest_session = aiohttp.ClientSession()

        async with self.rest_session.request(method, endpoint, headers=headers, json=json) as response:
            if response.status // 100 != 2:
                raise Exception(f'Error {response.status} on {method} {endpoint}: {response.reason}')
            return await response.json()
    
    async def generate_token_from_refresh(self, refresh_token):
        headers = {'Content-Type':'application/x-www-form-urlencoded'}
        payload = {
            'grant_type':'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.consumer_key,
        }
        response = await self.rest_query('post', self.td_uri+self.endpoints['oath_token'], headers, payload)
        access_token = response['access_token']
        access_token_expiration = datetime.utcnow() + int(response['expires_in'])
        return access_token, access_token_expiration

    async def get_user_principals(self):
        headers = {'Authorization': f'Bearer {self.access_token}'}
        payload = {'fields' : "streamerSubscriptionKeys,streamerConnectionInfo"}
        response = await self.rest_query('get', self.td_uri+self.endpoints['user_principals'], headers, payload)
        return response

    async def handle_ws(self, symbols):

        if self.access_token is None or self.access_token_exp < datetime.utcnow():
            self.access_token, self.access_token_exp = await self.generate_token_from_refresh(self.refresh_token)
        if self.principals is None:
            self.principals = await self.get_user_principals()

        credentials = {
            "userid": self.principals['accounts'][0]['accountId'],
            "token": self.principals['streamerInfo']['token'],
            "company": self.principals['accounts'][0]['company'],
            "segment": self.principals['accounts'][0]['segment'],
            "cddomain": self.principals['accounts'][0]['accountCdDomainId'],
            "usergroup": self.principals['streamerInfo']['userGroup'],
            "accesslevel": self.principals['streamerInfo']['accessLevel'],
            "authorized": "Y",
            "timestamp": str(datetime.strptime(self.principals['streamerInfo']['tokenTimestamp'], "%Y-%m-%dT%H:%M:%S%z").timestamp()*1000),
            "appid": self.principals['streamerInfo']['appId'],
            "acl": self.principals['streamerInfo']['acl']
        }

        uri = 'wss://' + self.principals['streamerInfo']['streamerSocketUrl'] + '/ws'
        while True:
            req_id = 1
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(uri) as ws:
                        login = {
                            "requests": [
                                {
                                    "service": "ADMIN",
                                    "requestid": str(req_id),
                                    "command": "LOGIN",
                                    "account": self.principals['accounts'][0]['accountId'],
                                    "source": self.principals['streamerInfo']['appId'],
                                    "parameters": {
                                        "credential": urlencode(credentials),
                                        "token": self.principals['streamerInfo']['token'],
                                        "version": "1.0"
                                    }
                                }
                            ]
                        }
                        await ws.send_str(json.dumps(login))
                        response = await ws.receive()
                        if response['status'] != 'success':
                            raise Exception(f'Error {response.status} on login: {response}')
                        sub_price = {
                            "type": "subscribe",
                            "channel": "QUOTE",
                            "symbol": symbols
                        }
                        await ws.send_str(json.dumps(sub_price))
                        sub_book = {
                            "type": "subscribe",
                            "channel": "TICK",
                            "symbol": symbols
                        }
                        await ws.send_str(json.dumps(sub_book))
                        async for msg in ws:
                            logger.info(msg)
                            msg = msg.json()
                            if msg['type'] == 'QUOTE':
                                await self.book(msg)
                            elif msg['type'] == 'TICK':
                                self.handle_ticks(msg)
                            else:
                                logger.warning(f'Unknown message: {msg["type"]}')
            except Exception as e:
                logger.error(e)
                #probably shouldnt catch exceptions at this veryu moment, so added sleep to allow ctrl+c SIGTERM
                await asyncio.sleep(60)
            req_id += 1

def main():
    parser = ArgumentParser()
    ma_strategy = parser.add_argument_group("MA strategy", "Moving average strategy")
    ma_strategy.add_argument('--timeframe', type=str, default='5m', choices=TIMEFRAMES.keys(), help="Timeframe for candles")
    ma_strategy.add_argument('--symbols', type=str, default='SPY', help="Symbols to trade")
    ma_strategy.add_argument('--indicator', type=str, default='sma', choices=MA, help="Moving average: ether sma or ema")
    ma_strategy.add_argument('--period', type=str, default='9', choices=[str(x) for x in range(1, 201)], help="Moving average period")
    ma_strategy.add_argument('--lookback', type=str, default='5', choices=[str(x) for x in range(1, 21)], help="Lookback period")
    execution_args = parser.add_argument_group("Execution", "Execution parameters")
    execution_args.add_argument('--execution_uri', type=str, default='https://localhost:8080/ws', help="Execution uri")
    credentials = parser.add_argument_group("Credentials", "Credentials for data source")
    credentials.add_argument('--user_id', type=str, default=None, help="API user id")
    credentials.add_argument('--consumer_key', type=str, default=None, help="API application consumer key")
    credentials.add_argument('--refresh_token', type=str, default=None, help="API refresh token")
    args = parser.parse_args()
    signal_generator = TDASignalGenerator(**args)
    asyncio.run(signal_generator.signal_handler_main())

if __name__ == '__main__':
    main()

SIGNALGENERATOR = TDASignalGenerator