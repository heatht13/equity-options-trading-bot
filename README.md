# Equity Options Trading Bot

## DISCLAIMER: THIS PROJECT DOES NOT IMPLEMENT A PROFITABLE STRATEGY. FOR REFERENCE ONLY

This is a fully automated equity option trading bot. Provided the user passes valid exchange auth credentials, simply starting the listed programs under [Startup](#startup) will have you kicked back drinking an ice cold beer while the bot trades its strategy. No user interference or guidance necessary.

## Strategy

* Stream real-time tick data and build OHLC candles at pre-defined intervals
* Direction is determined by the close of the last candle. Short (long puts) if close is below pre-defined MA else Long (long calls)
* After we have determined direction, we wait for price (does not have to be a candle close) to break out of x candles highs (if long) or lows (if short)
* If these criteria are met, we will enter a position. Once in a position, if price (not candle close) crosses pre-defined MA, flatten position.
* Focused on getting into a position. Currently sending market orders until we stream options MD to decision engine. Limit orders will then be implemented

## Startup
Refer to the desired handler's source code to retrieve a list of valid startup parameters and their use cases. Auth credentials will be required if running with a live account.
  * Market Data Server: `.py311/bin/python md_handler.py --exchange fake --symbols SPY QQQ --timeframe 5m --ma sma --period 9 --lookback 3`
  * Exchange Router: `.py311/bin/python exchange_handler.py --exchange fake` Note: Currently, there is no `fake` exchange handler implemented, but it is rather easy to do so. I will leave that up to you to build out as you please.
  * Decision Engine: `.py311/bin/python decision_engine.py --symbols SPY QQQ`

## Architecture
  
* ### Decision Engine

  * Subscribes to indicators and price feeds from MDSocketServer
  * Determines whether signal is generated from signal data
  * Requests positions, balances, other relevant info from Exchange Handler
  * Produce decisions based on signals, balances, and positions
  * Sends order parameters to order router for execution on exchange

* ### Data Handler

  * Supports strategy discussed above
  * Subscribes to MD from exchange
  * Produces signals from MD
  * Sends signals, prices to Decision Engine

* ### Exchange Handler

  * Tradier Exchange is built out
  * TDA and Webull exchanges paused indefinitely
  * Handles position management, balances, and order entry requests from Decision Engine and sends to exchange

* ### Book

  * Not built. Bids/Asks will be streamed directly to Decision Engine.
  * If we want to store book data to db, we can implement this
  * Recieves market data from Market Data Feed
  * Builds a book of bids and asks

## Instruments

* SPY Options

## To Do

* Write MD and exchange task management like i do in decision handler
* Need to (properly) close all processes upon insufficient funds or market close.
* Need to integrate limit orders
* Potentially add some timeout logic to the queues and sockets
* Need to add support running from remote
* Have server respond with list of symbols that fail to subscribe
* If decide to build book, seperate md from signal generator and stream it to both signal generator and book

## Notes

* Decision Engine is built to support only one position per underlying. If needed, can make multi
* Incorporate other indicators
* Optimize choosing expiry that minimizes time decay, but ensures a return

## Platform

* Tradier Brokerage API
* TD Ameritrade API. Development paused due to TD Ameritrade not supporting asynchronous code
* Webull OpenAPI. Development has not started
* Python 3.11.3
