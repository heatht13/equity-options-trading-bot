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

## Architecture

* ### Startup

  * Market Data Server: `.py311/bin/python md_handler.py --symbols SPY QQQ --exchange fake`
  * Exchange Router: `.py311/bin/python exchange_router.py --exchange fake`
  * Decision Engine: `.py311/bin/python decision_engine.py --symbols SPY QQQ`
  
* ### Decision Engine (Client)

  * subscribes to signals and bids/asks feeds from MDSocketServer
  * Determines whether signal is generated from signal data
  * requests positions, balances, other relevant info from Exchange Handler
  * Produce decisions based on signals, balances, and positions
  * sends order parameters to order router for execution on exchange

* ### Data Handler (Server and Client)

  * Supports strategy discussed above
  * client subscribes to MD from exchange
  * produces signals from MD
  * server sends signals, bids/asks to Decision Engine client

* ### Exchange Handler (Server and Client)

  * Tradier Exchange in development.
  * TDA and Webull handlers paused indefinitely
  * server handles position management, balances, and order entry requests from Decision Engine
  * client makes positions, balances, and order entry requests to exchange

* ### Book

  * Not needed. Bids/Asks will be streamed directly to decision engine.
  * If we want to store book data to db, we can implement this
  * Recieves market data from Market Data Feed
  * Builds a book of bids and asks
  * Sends bid/ask to Decision Engine

## Instruments

* SPY Options about a week or two from expiry

## To Do

* Have server respond with list of symbols that fail to subscribe
* Have client send name so server can better track who the client is
* Potentially add some timeout logic to the queues and sockets
* Need to fix signal handler logic to account for any state
* Need to get api keys from webull
* Decide whether to build book inside signal generator or in its own process.
  * If in its own process, seperate md from signal generator and stream it to both signal generator and book
* Need to look into how many decimals can be used for order entry on options. Price levels are typically $0.01 each (so $1.00 in options), so if we can cutr this down to say $0.005 or less, we are able to place more marketable orders without paying the whole dollar

## Notes

* Incorporate other indicators, especially volume
* We want instruments that are less choppy/hold moves
* Optimize choosing expiry that minimizes time decay, but ensures a return

## Platform

* Tradier Brokerage API
* TD Ameritrade API (soon to be Schwab API). Development paused due to TD Ameritrade not supporting asynchronous code. Clowns
* Webull OpenAPI. Development has not started because webull have not exposed their api. Nice
* Python 3.11.3
