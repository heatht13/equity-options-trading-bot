# destiny

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

* ### Market Data Feed

  * Need api keys from webull to build out
  * Simple Client Session websocket will do
  * Stream data to Book and Signal Generator

* ### Signal Generator

  * Supports strategy discussed above
  * Recieves market data and produces signal data
  * Sends signal data to Decision Engine

* ### Book

  * Recieves market data from Market Data Feed
  * Builds a book of bids and asks
  * Sends bid/ask to Decision Engine

* ### Decision Engine

  * Recieves bid/ask from Book and signal data from Signal Generator
  * Determines whether signal is generated from signal data
  * Receives positions, balances, other relevant info
  * Produce decisions based on signals, balances, and positions
  * Sends order parameters to order router for execution on exchange

* ### Order Router

  * Need api keys from webull to build out
  * Provides interface for position management, balances, and order entry
  * Receives order params and executes order on exchanges
  * Sends postions, orders, balances, and any other relevant data to Decision Engine

## Instruments

* SPY Options about a week or two from expiry

## Immediate To Do

* Need to get api keys from webull
* Need to find dependable MD feed cause webull currently doesn't offer it
* Decide whether to build book inside signal generator or in its own process.
  * If in its own process, seperate md from signal generator and stream it to both signal generator and book
* Need to look into use of EMA over MA and vice versa.
  * Currently going to use SMA as its less sensitve to price changes
* Need to look into how many decimals can be used for order entry on options. Price levels are typically $0.01 each (so $1.00 in options), so if we can cutr this down to say $0.005 or less, we are able to place more marketable orders without paying the whole dollar

## Notes

* Incorporate other indicators, especially volume
* We want instruments that are less choppy/hold moves
* Optimize choosing expiry that minimizes time decay, but ensures a return

## Platform

* Webull OpenAPI
* Python 3.11.3
