# Bot Name Goes Here

## Strategy

* Stream real-time tick data and build OHLC candles at pre-defined intervals
* Direction is determined by the close of the last candle. Short (long puts) if close is below  pre-defined MA else Long (long calls)
* After we have determined direction, we wait for price (does not have to be a candle close) to break out of x candles highs (if long) or lows (if short). Assuming 5min candles, x should probably be somewhere between 5 and 10. This ensures the move is worthy and helps mitigate the downfalls below
* If these criteria are met, we will enter a position. Once in a position, if price (not candle close) crosses 9MA, flatten position.
* Marketable limit orders should be priority here although we are more focused on getting into a position for now and not saving 1 level. Currently sending market orders until we stream options MD to decision engine.

* ### Pitfalls

  * Biggest downfall is number of trades taken can be very excessive without anything to show for it, so fees need to be managed accordingly. This is because when the price chops in a small range, it will continuously cross the MA resulting in sending multiple buy and sell signals. We would essentially be trading nothing and could likely incur losses
  * Due to potential frequency of trades, a cash account is not optimal. We need to be available to freely open and close positions all day. This requires margin or some alternative that allows us to place any number of orders a day.

## Architecture

* ### Startup

  * Market Data Server: `.py311/bin/python md_handler.py --symbols SPY QQQ --exchange fake`
  * Exchange Router: `.py311/bin/python exchange_handler.py --exchange fake **kwargs`
  * Decision Engine: `.py311/bin/python decision_engine.py --symbols SPY QQQ`
  
* ### Decision Engine (Client)

  * subscribes to indicators and price feeds from MDSocketServer
  * Determines whether signal is generated from signal data
  * requests positions, balances, other relevant info from Exchange Handler
  * Produce decisions based on signals, balances, and positions
  * sends order parameters to order router for execution on exchange

* ### Data Handler (Server and Client)

  * Supports strategy discussed above
  * client subscribes to MD from exchange
  * produces signals from MD
  * server sends signals, prices to Decision Engine client

* ### Exchange Handler (Server and Client)

  * Tradier Exchange is built out
  * TDA and Webull exchanges paused indefinitely
  * server handles position management, balances, and order entry requests from Decision Engine
  * client makes positions, balances, and order entry requests to exchange

* ### Book

  * Not needed. Bids/Asks will be streamed directly to decision engine.
  * If we want to store book data to db, we can implement this
  * Recieves market data from Market Data Feed
  * Builds a book of bids and asks
  * Sends bid/ask to Decision Engine

## Instruments

* SPY Options about a week or two from expiry or whatever is specified

## To Do

* Write MD and exchange task management like i do in decision handler
* Need to add support running from remote
* Need to log to files at some point
* Potentially add some timeout logic to the queues and sockets
* Have server respond with list of symbols that fail to subscribe
* Need to get api keys from webull
* If decide to build book, seperate md from signal generator and stream it to both signal generator and book

## Notes

* Decision Engine is built to support only one position per underlying. If needed, can make multi
* Incorporate other indicators, especially volume
* We want instruments that are less choppy/hold moves
* Optimize choosing expiry that minimizes time decay, but ensures a return

## Platform

* Tradier Brokerage API
* TD Ameritrade API (soon to be Schwab API). Development paused due to TD Ameritrade not supporting asynchronous code. Clowns
* Webull OpenAPI. Development has not started because webull have not exposed their api. Nice
* Python 3.11.3
