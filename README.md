# Bot Name Goes Here

## Strategy

* Stream real-time tick data (1s aggregation is fine) and build OHLC candles at 5 minute intervals
* When price crosses 9MA upwards, flatten position and go long (buy signal)
* When price crosses 9MA downwards, flatten position and go short (sell signal)
* Marketable limit orders should be priority here although we are more focused on getting into a position than saving 1 level.
* After we see either of the two signals above, wait to enter until we are breaking out of previous x number candles' H-L range (or MA slope). Assuming 5min candles, x should probably be somewhere between 5 and 10. This ensures the move is worthy and helps mitigate the downfalls below

* ### Pitfalls

  * Biggest downfall is number of trades taken can be very excessive without anything to show for it, so fees (if they exist) need to be managed accordingly. This is because when the price chops in a small range, it will continuously cross the 9MA resulting in sending multiple buy and sell signals. We would essentially be trading nothing and could likely incur losses
  * Due to potential frequency of trades, a cash account is not optimal. We need to be available to freely open and close positions all day. This requires margin or some alternative that allows us to place tens of orders a day. An account with $25k-$100k would be optimal.

## Structure and Design

* Note: Currently have SignalGenerator, MD, and Book in one process

* ### Decision Engine (Client)

  * subscribes to signals and bids/asks feeds from Signal Generator
  * Determines whether signal is generated from signal data
  * requests positions, balances, other relevant info from order router
  * Produce decisions based on signals, balances, and positions
  * sends order parameters to order router for execution on exchange

* ### Signal Generator (Server and Client)

  * Supports strategy discussed above
  * client subscribes to MD from exchange
  * produces signals from MD
  * server sends signals, bids/asks to decision engine client
 
* ### Order Router (Server and Client)

  * Building out on TD Ameritrade
  * Waiting on api from Webull
  * server handles position management, balances, and order entry requests from decision engine
  * client makes positions, balances, and order entry requests to exchange

* ### Market Data Feed

  * For first iteration, MD will exist within Signal Generator
  * Building out on TD Ameritrade
  * Waiting on api keys from Webull
  * Stream data to Book and Signal Generator

* ### Book

  * Not needed. Bids/Asks will be streamed directly to decision engine.
  * If we want to store book data to db, we can implement this
  * Recieves market data from Market Data Feed
  * Builds a book of bids and asks
  * Sends bid/ask to Decision Engine

## Instruments

* SPY Options about a week or two from expiry

## Immediate To Do

* Remove aiohttp from inter-process IO. Use async unix sockets instead. Keep aiohttp for order entry and if needed MD too
* Need to get api keys from webull
* Decide whether to build book inside signal generator or in its own process.
  * If in its own process, seperate md from signal generator and stream it to both signal generator and book
* Need to look into how many decimals can be used for order entry on options. Price levels are typically $0.01 each (so $1.00 in options), so if we can cutr this down to say $0.005 or less, we are able to place more marketable orders without paying the whole dollar

## Notes

* Incorporate other indicators, especially volume
* We want instruments that are less choppy/hold moves
* Optimize choosing expiry that minimizes time decay, but ensures a return

## Platform

* TD Ameritrade API (soon to be Schwab API)
* Webull OpenAPI
* Python 3.11.3

## Example async unix socket server and client creation:

```
MSG_LENGTH_PREFIX_BYTES = 4
##Client Side:
async def send_msg(reader, writer):
    ##handle any incoming msgs and generate outgoing msg
    message_length = len(msg)
    writer.write(message_length.to_bytes(MSG_LENGTH_PREFIX_BYTES, byteorder='big'))
    writer.write(message.encode('utf-8'))
    await writer.drain()
reader, writer = await asyncio.open_unix_connection(path='/tmp/unix_socket')
await send_msg(writer, "this is my message")
writer.close()
##Server Side
async def handle_client(reader, writer):
    data = b''
    while True:
        msg_length_prefix = await reader.read(MSG_LENGTH_PREFIX_BYTES)
        msg_length = int.from_bytes(message_length_prefix, byteorder='big')
        msg = await reader.read(msg_length)
        msg = msg.decode('utf-8')
        ##handle msg
        writer.write(response)
server = await asyncio.start_unix_server(handle_client, '/tmp/unix_socket')
async with server:
    await server.server_forever()
```
