from exchange_handlers.tradier_exchange_handler import TradierExchangeHandler as TEH
import asyncio
exchange = TEH(exchange='tradier', account_id='', access_token='')
data = {
    "class": "option",
    "symbol": "SPY",
    "side": "buy_to_open",
    "quantity": "1",
    "type": "market",
    "duration": "day",
    "option_symbol": "SPY231201P00450000"
}
data = {
    'symbol': 'SPY',
    'order_type': 'limit',
    'side': 'buy',
    'price': '0.30',
    'quantity': '1.000000',
    'offset': 'open',
    'tif': 'gtc',
    'asset_class': 'option',
    'exp': '231201',
    'strike': '450.0',
    'callput': 'put',
}
async def test():
    out = await exchange.place_order(**data)
    print(out)
    # out = {'order': {'id': '42627396'}}
    # out = await exchange.cancel_order(out['order']['id'])
asyncio.run(test())