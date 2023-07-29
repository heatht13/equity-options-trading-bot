# destiny

## Strategy:
* Stream real-time tick data (1s aggregation is fine) and build OHLC candles at 5 minute intervals
* When price crosses 9MA upwards, flatten position and go long
* When price crosses 9MA downwards, flatten position and go short
* After we see either of the two signals above, wait to enter until we are breaking out of previous x number candles H-L range (or MA slope). Assuming 5min candles, x should probably be somewhere between 5 and 10. This ensures the moves worthy and helps mitigate below
* Biggest downfall is number of trades taken can be very excessive without anything to show for it, so fees (if they exist) need to be managed closely and taxes will be deplorable 
## Instruments:
* SPY Options about a week or two from expiry
## Notes:
* Need to look into use of EMA over MA and vice versa
* Need to look at incorporating other indicators, especially volume
* Need to look at instruments that are less choppy/hold moves
* Need to look into seeing which expiry minimizes time decay, but ensures decent returns
## TODO:
* Need to get api keys from webull
* Need to find dependable MD feed cause webull currently doesn't offer it
## Platform
* Webull OpenAPI
* Python 3.11.3
