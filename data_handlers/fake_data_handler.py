import asyncio
import decimal
import logging
from datetime import datetime

from md_handler import MALookbackDataParser

#logger = Logger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
    handlers=[
        #logging.FileHandler("path.log"),
        logging.StreamHandler()
    ]
)

#NOTE: Built as a simulation of the TradierDataHandler, but one which can run at any hour
class FakeDataHandler(MALookbackDataParser):
    def __init__(self, access_token, **kwargs):
        super().__init__(**kwargs)
        self.last_ts_seq = 0

    def parse_msg(self, msg):
        channel = msg['type']
        if channel == 'timesale':
            if msg['cancel'] or msg['correction'] or msg['seq'] <= self.last_ts_seq:
                return None
            data = {
                'bid': float(msg['bid']),
                'ask': float(msg['ask']),
                'price': float(msg['last']),
                'size': float(msg['size']),
                'trade_time': float(msg['date']) / 10 ** 3,
                'seq': msg['seq'],
            }
            self.last_ts_seq = msg['seq']
        elif channel == 'quote':
            bid = decimal.Decimal(str(msg['bid']))
            ask = decimal.Decimal(str(msg['ask']))
            price = float(str((bid + ask) / 2))
            data = {
                'bid': msg['bid'],
                'ask': msg['ask'],
                'price': price,
                'bid_size': msg['bidsz'],
                'ask_size': msg['asksz'],
                'quote_time': float(msg['date']) / 10 ** 3
            }
        elif channel == 'trade':
            data = {
                'price': float(msg['price']),
                'size': float(msg['size']),
                'trade_time': float(msg['date']) / 10 ** 3
            }
        else:
            return None
        return {
                'type': 'update',
                'channel': channel,
                'symbol': str(msg['symbol']).upper(),
                'timestamp': datetime.utcnow().timestamp(),
                'data': data
            }

    async def stream_handler(self):
        import random
        async def generate_timesale():
            bid = 281.84
            ask = 281.85
            last = 281.84
            size = 20
            seq = 1
            while True:
                timesale = {
                    "type": "timesale",
                    "symbol": "SPY",
                    "exchange": "Z",
                    "bid": str(bid),
                    "ask": str(ask),
                    "last": str(last),
                    "size": str(size),
                    "date": "1557757189000",
                    "seq": seq,
                    "flag": "",
                    "cancel": False,
                    "correction": False,
                    "session": "normal"
                }
                await asyncio.sleep(random.uniform(1, 3))
                timesale['date'] = str(int(datetime.utcnow().timestamp() * 10 ** 3))
                await self.handle_msg(timesale)
                bid += 1
                ask += 1
                last += 1
                size += 1
                seq += 1

        async def generate_quote():
            bid = 281.84
            bidsz = 60
            ask = 281.85
            asksz = 7
            while True:
                quote = {
                    "type": "quote",
                    "symbol": "SPY",
                    "bid": bid,
                    "bidsz": bidsz,
                    "bidexch": "M",
                    "biddate": "1557757189000",
                    "ask": ask,
                    "asksz": asksz,
                    "askexch": "Z",
                    "askdate": "1557757190000"
                }
                await asyncio.sleep(1)
                quote['date'] = str(int(datetime.utcnow().timestamp()) * 10 ** 3)
                #logger.info(f"Received message: {json.dumps(msg, indent=4)}")
                await self.handle_msg(quote)
                bid += 1
                bidsz += 1
                ask += 1
                asksz += 1

        async def generate_trade():
            price = 281.84
            size = 60
            cum_volume = 10000
            while True:
                trade = {
                    "type": "trade",
                    "symbol": "SPY",
                    "exch": "Z",
                    "price": str(price),
                    "size": str(size),
                    "cvol": str(cum_volume),
                    "date": "1557757189000",
                    "last": str(price)
                }
                await asyncio.sleep(random.uniform(1, 3))
                trade['date'] = str(int(datetime.utcnow().timestamp() * 10 ** 3))
                #logger.info(f"Received message: {json.dumps(msg, indent=4)}")
                await self.handle_msg(trade)
                price += 1
                size += 1
                cum_volume += 1

        await asyncio.gather(generate_quote(), generate_timesale(), generate_trade())