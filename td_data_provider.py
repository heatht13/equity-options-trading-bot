import aiohttp
import asyncio
import logging
from argparse import ArgumentParser
from datetime import datetime
from urllib.parse import urlencode
import json

from ma_lookback_data_parser import MALookbackDataParser, TIMEFRAMES, MA

#logger = Logger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        #logging.FileHandler("path.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

QOS_RATES = {
    '500': 0, #Express
    '750': 1, #Real-Time, default value for http binary protocol
    '1000': 2, #Fast, default value for websocket and http asynchronous protocol
    '1500': 3, #Moderate
    '3000': 4, #Slow
    '5000': 5 #Delayed
}

EQUITIES_QUOTE_FIELDS = {
    'symbol': 0,
    'bid_price': 1,
    'ask_price': 2,
    'last_price': 3,
    #'bid_size': 4,
    #'ask_size': 5,
    #'ask_id': 6, #Exchange with the best ask
    #'bid_id': 7, #Exchange with the best bid
    #'total_volume': 8,
    #'last_size': 9,
    #'trade_time': 10, #Trade time of the last trade (Seconds since midnight EST)
    'quote_time': 11, #Trade time of the last quote (Seconds since midnight EST)
    #'high_price': 12, #day's high
    #'low_price': 13, #day's low
    #'bid_tick': 14,
    #'close_price': 15
    #.....plenty more
}

OPTIONS_QUOTE_FIELDS = {
    'symbol': 0,
    # 'description': 1,
    'bid_price': 2,
    'ask_price': 3,
    'last_price': 4,
    # 'high_price': 5,
    # 'low_price': 6,
    # 'close_price': 7,
    # 'total_volume': 8,
    # 'open_interest': 9,
    # 'volatility': 10,
    'quote_time': 11,
    # 'trade_time': 12,
    # 'money_intrinsic_value': 13,
    # 'quote_day': 14,
    # 'trade_day': 15,
    # 'expiration_year': 16,
    # 'multiplier': 17,
    # 'digits': 18,
    # 'open_price': 19,
    # 'bid_size': 20,
    # 'ask_size': 21,
    # 'last_size': 22,
    # 'net_change': 23,
    # 'strike_price': 24,
    # 'contract_type': 25,
    'underlying': 26,
    # 'expiration_month': 27,
    # 'deliverables': 28,
    # 'time_value': 29,
    # 'expiration_day': 30,
    # 'days_to_expiration': 31,
    # 'delta': 32,
    # 'gamma': 33,
    # 'theta': 34,
    # 'vega': 35,
    # 'rho': 36,
    # 'security_status': 37,
    # 'theoretical_option_value': 38,
    'underlying_price': 39,
    # 'uv_expiration_type': 40,
    # 'mark': 41,
}

class TDADataProvider(MALookbackDataParser):
    class Error(Exception):
        pass

    def __init__(self, user_id, consumer_key, refresh_token, rate, **args):
        super().__init__(**args)
        self.user_id = user_id
        self.consumer_key = consumer_key
        self.refresh_token = refresh_token
        self.rate = rate
        self.access_token = None
        self.access_token_exp = None
        self.principals = None
        self.rest_session = None
        self.ws_session = None
        self.url = 'https://api.tdameritrade.com/v1'
        self.endpoints = {
            'oath_token': '/oauth2/token',
            'user_principals': '/userprincipals',
        }

    async def rest_query(self,method, endpoint, headers=None, json=None):
        if self.rest_session is None:
            self.rest_session = aiohttp.ClientSession()

        uri = self.url + endpoint
        async with self.rest_session.request(method, uri, headers=headers, data=json) as response:
            if response.status // 100 != 2:
                raise Exception(f'Error {response.status} on {method} {endpoint}: {response.reason}')
            return await response.json()
    
    async def generate_token_from_refresh(self):
        headers = {'Content-Type':'application/x-www-form-urlencoded'}
        data = {
            'grant_type':'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.consumer_key,
        }
        response = await self.rest_query('POST', self.endpoints['oath_token'], headers, json=data)
        logger.info(f"response: {response}")
        access_token = response['access_token']
        access_token_expiration = datetime.utcnow().timestamp() + int(response['expires_in'])
        return access_token, access_token_expiration

    async def get_user_principals(self):
        headers = {'Authorization': f'Bearer {self.access_token}',
                   'Content-Type':'application/json'}
        data = {'fields' : "streamerSubscriptionKeys,streamerConnectionInfo"}
        response = await self.rest_query('GET', self.endpoints['user_principals'], headers, json=data)
        return response

    def msg(self, service, req_id, command, parameters):
        return {
            "requests": [
                {
                    "service": service,
                    "requestid": str(req_id),
                    "command": command,
                    "account": self.principals['accounts'][0]['accountId'],
                    "source": self.principals['streamerInfo']['appId'],
                    "parameters": parameters
                }
            ]
        }

    async def stream_handler(self, symbols):
        if self.access_token is None or self.access_token_exp < datetime.utcnow():
            self.access_token, self.access_token_exp = await self.generate_token_from_refresh()
        if self.principals is None:
            self.principals = await self.get_user_principals()

        logger.info(f"principals: {self.principals}")

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

        {'userId': 'heatht23', 'userCdDomainId': 'A000000076420708', 'primaryAccountId': '490932089', 'lastLoginTime': '2023-09-11T00:23:52+0000', 'tokenExpirationTime': '2023-09-11T00:54:53+0000', 'loginTime': '2023-09-11T00:24:53+0000', 'accessLevel': 'CUS', 'stalePassword': False, 'professionalStatus': 'NON_PROFESSIONAL', 'quotes': {'isNyseDelayed': False, 'isNasdaqDelayed': False, 'isOpraDelayed': False, 'isAmexDelayed': False, 'isCmeDelayed': True, 'isIceDelayed': True, 'isForexDelayed': True}, 'exchangeAgreements': {'OPRA_EXCHANGE_AGREEMENT': 'ACCEPTED', 'NYSE_EXCHANGE_AGREEMENT': 'ACCEPTED', 'NASDAQ_EXCHANGE_AGREEMENT': 'ACCEPTED'}, 'accounts': [{'accountId': '490932089', 'displayName': 'heatht23', 'accountCdDomainId': 'A000000076271239', 'company': 'AMER', 'segment': 'AMER', 'acl': 'AKBHBPCCCNCVDRDTDWESF7G1G3G5G7GKGLH1H3H5LTM1MRNSOLPNQ2QSRFSDT5TETFTOTTUAURXBXNXO', 'authorizations': {'apex': False, 'levelTwoQuotes': True, 'stockTrading': True, 'marginTrading': False, 'streamingNews': True, 'optionTradingLevel': 'LONG', 'streamerAccess': True, 'advancedMargin': False, 'scottradeAccount': False, 'autoPositionEffect': False}}, {'accountId': '253346254', 'displayName': 'LongTerm', 'accountCdDomainId': 'A000000094265059', 'company': 'AMER', 'segment': 'AMER', 'acl': 'BPDRDTDWESF7G1G3G5G7GKGLH1H3H5LTM1QSRFSDT5TETFTOTTUAUR', 'authorizations': {'apex': False, 'levelTwoQuotes': False, 'stockTrading': True, 'marginTrading': False, 'streamingNews': False, 'optionTradingLevel': 'NONE', 'streamerAccess': True, 'advancedMargin': False, 'scottradeAccount': False, 'autoPositionEffect': False}}]}

        uri = 'wss://' + self.principals['streamerInfo']['streamerSocketUrl'] + '/ws'
        while self.running:
            try:
                req_id = 1
                async with aiohttp.ClientSession() as self.ws_session:
                    async with self.ws_session.ws_connect(uri) as ws:
                        try:
                            ##Logon
                            login = self.msg('ADMIN', req_id, 'LOGIN', {"credential": urlencode(credentials),
                                                                        "token": self.principals['streamerInfo']['token'],
                                                                        "version": "1.0"})
                            await ws.send_str(json.dumps(login))
                            response = await ws.receive()
                            req_id += 1
                            logger.info(f"Logon: {response}")
                            if response[0].get('content', {}).get('code') != 0:
                                raise self.Error(f'Error on login: {response}')
                            logger.info(f"Logon Successful {response}")
                            ##Change Quality of Service (data rate)
                            if self.rate != '1000':
                                qos = self.msg('ADMIN', req_id, 'QOS', {"qoslevel": QOS_RATES[self.rate]})
                                response = await ws.send_str(json.dumps(qos))
                                req_id += 1
                                if response[0].get('content', {}).get('code') != 0:
                                    raise self.Error(f'Error on QOS req: {response}')
                            ##Subscribe Level 1 Equities Quotes and Trades. Level 2 available (BOOK)
                            equities = self.msg('QUOTE', req_id, 'SUBS', {"keys": symbols.upper(), "fields": ','.join(EQUITIES_QUOTE_FIELDS.keys())})
                            response = await ws.send_str(json.dumps(equities))
                            req_id += 1
                            if response[0].get('content', {}).get('code') != 0:
                                    raise self.Error(f'Error on quotes req: {response}')
                            ##Subscribe Level 1 Options Quotes and Trades. Level 2 available (BOOK)
                            #TODO: Needs to have options symbols passed in, not equities
                            options = self.msg('OPTION', req_id, 'SUBS', {"keys": symbols.upper(), "fields": ','.join(OPTIONS_QUOTE_FIELDS.keys())})
                            response = await ws.send_str(json.dumps(options))
                            req_id += 1
                            if response[0].get('content', {}).get('code') != 0:
                                    raise self.Error(f'Error on quotes req: {response}')
                            async for msg in ws:
                                msg = msg.json()
                                logger.info(msg)
                                # if msg['type'] == 'QUOTE' or msg['type'] == 'OPTION':
                                #     self.handle_ticks(msg)
                                # else:
                                #     logger.warning(f'Unknown message: {msg["type"]}')
                        finally:
                            logout = {
                                "requests": [
                                    {
                                        "service": "ADMIN", 
                                        "requestid": str(req_id), 
                                        "command": "LOGOUT", 
                                        "account": self.principals['accounts'][0]['accountId'], 
                                        "source": self.principals['streamerInfo']['appId'], 
                                        "parameters": { }
                                    }
                                ]
                            }
                            ws.send_str(json.dumps(logout))
                            response = await ws.receive()
                            logger.info(f"Logout: {response}")

            except (aiohttp.ClientError, aiohttp.WSServerHandshakeError, ConnectionResetError) as e:
                logger.error(f"websocket connection closed; resetting. {e}")

def main():
    parser = ArgumentParser()
    signal_generator_args = parser.add_argument_group("Data Provider", "Data Provider parameters")
    signal_generator_args.add_argument('--socket', type=str, default='/tmp/data_provider.sock', help="Path to unix domain socket responsible for serving data")
    ma_strategy = parser.add_argument_group("MA strategy", "Moving average strategy")
    ma_strategy.add_argument('--timeframe', type=str, default='5m', choices=TIMEFRAMES.keys(), help="Timeframe for candles")
    ma_strategy.add_argument('--symbols', type=str, default='SPY', help="Symbols to trade")
    ma_strategy.add_argument('--indicator', type=str, default='sma', choices=MA, help="Moving average: ether sma or ema")
    ma_strategy.add_argument('--period', type=str, default='9', choices=[str(x) for x in range(1, 201)], help="Moving average period")
    ma_strategy.add_argument('--lookback', type=str, default='5', choices=[str(x) for x in range(1, 21)], help="Lookback period")
    credentials = parser.add_argument_group("Credentials", "Credentials for data source")
    credentials.add_argument('--user_id', type=str, help="API user id")
    credentials.add_argument('--consumer_key', type=str, help="API application consumer key")
    credentials.add_argument('--refresh_token', type=str, help="API refresh token")
    credentials.add_argument('--rate', type=str, choices=QOS_RATES.keys(), help="Data rate (ms)")
    args = parser.parse_args()
    args = vars(args)
    data_provider = TDADataProvider(**args)
    asyncio.run(data_provider.data_handler_main())

if __name__ == '__main__':
    main()

SIGNALGENERATOR = TDADataProvider