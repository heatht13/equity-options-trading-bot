import aiohttp
import asyncio
import logging
from datetime import datetime
import json
import decimal

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

class TradierDataProvider(MALookbackDataParser):
    class Error(Exception):
        pass

    def __init__(self, access_token, **kwargs):
        super().__init__(**kwargs)
        self.access_token = access_token
        self.session_id = None
        self.rest_session = None
        self.ws_session = None
        self.running = True
        self.rest_url = 'https://api.tradier.com'
        self.ws_uri = 'wss://ws.tradier.com/v1/markets/events'
        self.endpoints = {
            'create_session': '/v1/markets/events/session',
        }

    async def rest_query(self,method, endpoint, headers=None, json=None):
        if self.rest_session is None:
            self.rest_session = aiohttp.ClientSession()

        uri = self.rest_url + endpoint
        async with self.rest_session.request(method, uri, headers=headers, data=json) as response:
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

    def parse_msg(self, msg):
        channel = msg['type']
        if channel == 'quote':
            bid = decimal(str(msg['bid']))
            ask = decimal(str(msg['ask']))
            price = (bid + ask) / 2
            data = {
                'bid': bid,
                'ask': ask,
                'price': price,
                'bid_size': msg['bidsz'],
                'ask_size': msg['asksz']
            }
        else:
            data = {
                'price': decimal(str(msg['price'])),
                'size': decimal(str(msg['size'])),
                'trade_time': int(msg['date'])
            }
        return json.dumps({
                'type': 'update',
                'channel': channel,
                'symbol': str(msg['symbol']).upper(),
                'timestamp': datetime.utcnow().timestamp(),
                'data': data
            })

    async def stream_handler(self, symbols):
        if self.access_token is None:
            raise self.Error('Access Token required. None provided')
        
        while self.running:
            try:
                async with aiohttp.ClientSession() as self.ws_session:
                    async with self.ws_session.ws_connect(self.ws_uri, ssl=True) as ws:
                        try:
                            logger.info(f"Stream Handler started")
                            if self.session_id is None:
                                await self.get_session_id()
                            sub_symbols = {
                                'symbols': symbols,
                                'filter': ['quote', 'trade'], #trade,quote,summary,timesale,tradex, dont pass if want all. summary gives OHLC data and prev close, but no timestamp
                                'sessionid': self.session_id,
                                'linebreak': True
                            }
                            await ws.send_str(json.dumps(sub_symbols))
                            async for msg in ws:
                                msg = msg.json()
                                logger.info(msg)
                                if 'type' in msg:
                                    if msg['type'] in ('quote', 'trade'):
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
                        finally:
                            logger.info(f"Stream Handler shutting down")
                            if self.rest_session and not self.rest_session.closed:
                                await self.rest_session.close()
                                self.rest_session = None
                            if self.ws_session and not self.ws_session.closed:
                                await self.ws_session.close()
                                self.ws_session = None
            except (aiohttp.ClientError, aiohttp.WSServerHandshakeError, ConnectionResetError) as e:
                logger.error(f"websocket connection closed; resetting. {e}")

SIGNALGENERATOR = TradierDataProvider