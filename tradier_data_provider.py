import aiohttp
import asyncio
import logging
from argparse import ArgumentParser
from datetime import datetime
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


class TradierDataProvider(MALookbackDataParser):
    class Error(Exception):
        pass

    def __init__(self, access_token, **kwargs):
        super().__init__(**kwargs)
        self.access_token = access_token
        self.session_id = None
        self.rest_session = None
        self.ws_session = None
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
                                'filter': ['quote'], #trade,quote,summary,timesale,tradex, dont pass if want all. summary gives OHLC data and prev close, but no timestamp
                                'sessionid': self.session_id,
                                'linebreak': True
                            }
                            await ws.send_str(json.dumps(sub_symbols))
                            async for msg in ws:
                                msg = msg.json()
                                logger.info(msg)
                                if 'type' in msg:
                                    if msg['type'] == 'quote':
                                        await self.handle_ticks(msg)
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

def main():
    parser = ArgumentParser()
    signal_generator_args = parser.add_argument_group("Data Provider", "Data Provider parameters")
    signal_generator_args.add_argument('--socket', type=str, default='/tmp/data_provider.sock', help="Path to unix domain socket responsible for serving data")
    ma_strategy = parser.add_argument_group("MA strategy", "Moving average strategy")
    ma_strategy.add_argument('--timeframe', type=str, default='5m', choices=TIMEFRAMES.keys(), help="Timeframe for candles")
    ma_strategy.add_argument('--symbols', type=str.upper,  nargs='*', help="Symbols to trade")
    ma_strategy.add_argument('--indicator', type=str, default='sma', choices=MA, help="Moving average: ether sma or ema")
    ma_strategy.add_argument('--period', type=str, default='9', choices=[str(x) for x in range(1, 201)], help="Moving average period")
    ma_strategy.add_argument('--lookback', type=str, default='5', choices=[str(x) for x in range(1, 21)], help="Lookback period")
    credentials = parser.add_argument_group("Credentials", "Credentials for data source")
    credentials.add_argument('--access-token', type=str, help="API access token")
    args = parser.parse_args()
    args = vars(args)
    data_provider = TradierDataProvider(**args)
    asyncio.run(data_provider.data_handler_main())

if __name__ == '__main__':
    main()

SIGNALGENERATOR = TradierDataProvider