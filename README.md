# destiny

## Strategy:
* Stream real-time tick data (1s aggregation is fine) and build OHLC candles at 5 minute intervals
* When price crosses 9MA upwards, flatten position and go long
* When price crosses 9MA downwards, flatten position and go short
* After we see either of the two signals above, wait to enter until we are breaking out of previous x number candles' H-L range (or MA slope). Assuming 5min candles, x should probably be somewhere between 5 and 10. This ensures the move is worthy and helps mitigate the below
* Biggest downfall is number of trades taken can be very excessive without anything to show for it, so fees (if they exist) need to be managed closely. This is because when the price chops in a small range, it will continuously cross the 9MA resulting in sending multiple sell and buy signals. We would essentially be trading nothing and could likely be losses
* Marketable limit orders should be priority here although we are more focused on getting into a position than saving 1 level.
## Instruments:
* SPY Options about a week or two from expiry
## Notes:
* Need to look into use of EMA over MA and vice versa
* Need to look at incorporating other indicators, especially volume
* Need to look at instruments that are less choppy/hold moves
* Need to look into seeing which expiry minimizes time decay, but ensures decent returns
## To Do:
* Need to get api keys from webull
* Need to find dependable MD feed cause webull currently doesn't offer it
* Need to look into how many decimals can be used for order entry on options. Price levels are typically $0.01 each (so $1.00 in options), so if we can cutr this down to say $0.005 or less, we are able to place more marketable orders without paying the whole dollar
## Platform
* Webull OpenAPI
* Python 3.11.3
