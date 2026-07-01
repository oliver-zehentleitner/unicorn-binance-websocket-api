[![GitHub Release](https://img.shields.io/github/release/oliver-zehentleitner/unicorn-binance-websocket-api.svg?label=github)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/releases)
[![GitHub Downloads](https://img.shields.io/github/downloads/oliver-zehentleitner/unicorn-binance-websocket-api/total?color=blue)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/releases)
[![PyPi Release](https://img.shields.io/pypi/v/unicorn-binance-websocket-api?color=blue)](https://pypi.org/project/unicorn-binance-websocket-api/)
[![PyPi Downloads](https://pepy.tech/badge/unicorn-binance-websocket-api)](https://pepy.tech/project/unicorn-binance-websocket-api)
[![Conda-Forge Version](https://img.shields.io/conda/v/conda-forge/unicorn-binance-websocket-api?color=blue&label=conda)](https://anaconda.org/conda-forge/unicorn-binance-websocket-api)
[![Conda-Forge Downloads](https://img.shields.io/conda/dn/conda-forge/unicorn-binance-websocket-api?color=blue&label=downloads)](https://anaconda.org/conda-forge/unicorn-binance-websocket-api)
[![License](https://img.shields.io/github/license/oliver-zehentleitner/unicorn-binance-websocket-api.svg?color=blue)](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/license.html)
[![Supported Python Version](https://img.shields.io/pypi/pyversions/unicorn_binance_websocket_api.svg)](https://www.python.org/downloads/)
[![PyPI - Status](https://img.shields.io/pypi/status/unicorn_binance_websocket_api.svg)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues)
[![codecov](https://codecov.io/gh/oliver-zehentleitner/unicorn-binance-websocket-api/branch/master/graph/badge.svg?token=5I03AZ3F5S)](https://codecov.io/gh/oliver-zehentleitner/unicorn-binance-websocket-api)
[![CodeQL](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/codeql-analysis.yml)
[![Unit Tests](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/unit-tests.yml)
[![Build and Publish GH+PyPi](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/build_wheels.yml/badge.svg)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/build_wheels.yml)
[![Conda-Forge Build](https://dev.azure.com/conda-forge/feedstock-builds/_apis/build/status/unicorn-binance-websocket-api-feedstock?branchName=main)](https://github.com/conda-forge/unicorn-binance-websocket-api-feedstock)
[![Read the Docs](https://img.shields.io/badge/read-%20docs-yellow)](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api)
[![Read How To`s](https://img.shields.io/badge/read-%20howto-yellow)](https://blog.technopathy.club/series/unicorn-binance-suite)
[![Github](https://img.shields.io/badge/source-github-cbc2c8)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api)
[![Telegram](https://img.shields.io/badge/community-telegram-41ab8c)](https://t.me/unicorndevs)
[![Reddit](https://img.shields.io/badge/community-reddit-41ab8c)](https://www.reddit.com/r/UNICORNBinanceSuite)

[![UBS-Banner](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-suite/master/images/logo/UBS-Banner-Readme.png)](https://github.com/oliver-zehentleitner/unicorn-binance-suite)

# UNICORN Binance WebSocket API

[Description](#description) | [Installation](#installation-and-upgrade) | [Documentation](#documentation) | [Examples](#examples) | [Change Log](#change-log) | 
[Wiki](#wiki) | [Social](#social) | [Notifications](#receive-notifications) | [Bugs](#how-to-report-bugs-or-suggest-improvements) | [Contributing](#contributing) | [Disclaimer](#disclaimer)

A Python SDK to use the Binance Websocket API`s (com+testnet, com-margin+testnet, com-isolated_margin+testnet, 
com-futures+testnet, com-coin_futures, com-vanilla-options+testnet, us, tr) in a simple, fast, flexible, robust and 
fully-featured way. 

Part of '[UNICORN Binance Suite](https://blog.technopathy.club/page/unicorn-binance-suite)'.

## Receive Data from Binance WebSockets

### [Create a multiplex websocket connection](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream) to Binance with a [`stream_buffer`](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_buffer%60) with just 3 lines of code

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

ubwa = BinanceWebSocketApiManager(exchange="binance.com")
ubwa.create_stream(channels=['trade', 'kline_1m'], markets=['btcusdt', 'bnbbtc', 'ethbtc'])
```

#### And 4 more lines to print out the data

```
while True:
    oldest_data_from_stream_buffer = ubwa.pop_stream_data_from_stream_buffer()
    if oldest_data_from_stream_buffer:
        print(oldest_data_from_stream_buffer)
```

### Or with a [callback function](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=process_stream_data#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream) just do

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

def process_new_receives(stream_data):
    print(str(stream_data))

ubwa = BinanceWebSocketApiManager(exchange="binance.com")
ubwa.create_stream(channels=['trade', 'kline_1m'], 
                   markets=['btcusdt', 'bnbbtc', 'ethbtc'], 
                   process_stream_data=process_new_receives)
```

### Or with an [async callback function](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=process_stream_data#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream) just do

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio

async def process_new_receives(stream_data):
    print(stream_data)
    await asyncio.sleep(1)

ubwa = BinanceWebSocketApiManager()
ubwa.create_stream(channels=['trade', 'kline_1m'],
                   markets=['btcusdt', 'bnbbtc', 'ethbtc'],
                   process_stream_data_async=process_new_receives)
```

### Or await the stream data in an asyncio coroutine

All the methods of data collection presented have their own advantages and disadvantages. However, this is the 
generally recommended method for processing data from streams.

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio

async def main():
    async def process_asyncio_queue(stream_id=None):
        print(f"Start processing the data from stream '{ubwa.get_stream_label(stream_id)}':")
        while ubwa.is_stop_request(stream_id) is False:
            data = await ubwa.get_stream_data_from_asyncio_queue(stream_id)
            print(data)
            ubwa.asyncio_queue_task_done(stream_id)
    ubwa.create_stream(channels=['trade'],
                       markets=['ethbtc', 'btcusdt'],
                       stream_label="TRADES",
                       process_asyncio_queue=process_asyncio_queue)
    while not ubwa.is_manager_stopping():
        await asyncio.sleep(1)

with BinanceWebSocketApiManager(exchange='binance.com') as ubwa:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
    except Exception as e:
        print(f"\r\nERROR: {e}\r\nGracefully stopping ...")
```

Basically that's it, but there are more options.

## Receive private UserData Streams

Create a private `!userData` stream to receive account updates like order fills, balance changes and position updates in 
real time. A valid `api_key` and `api_secret` is required.

### Set the credentials globally on the manager

All streams created on this manager inherit the credentials:

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

async def process_userdata(stream_data):
    print(stream_data)

ubwa = BinanceWebSocketApiManager(exchange="binance.com",
                                  api_key="YOUR_BINANCE_API_KEY",
                                  api_secret="YOUR_BINANCE_API_SECRET")
ubwa.create_stream(channels='arr',
                   markets='!userData',
                   process_stream_data_async=process_userdata)
```

### Or pass the credentials per stream

Useful when running multiple `!userData` streams with different API keys on the same manager:

```
ubwa = BinanceWebSocketApiManager(exchange="binance.com")

ubwa.create_stream(channels='arr',
                   markets='!userData',
                   api_key="API_KEY_ACCOUNT_A",
                   api_secret="API_SECRET_ACCOUNT_A",
                   stream_label="ACCOUNT_A",
                   process_stream_data_async=process_userdata)

ubwa.create_stream(channels='arr',
                   markets='!userData',
                   api_key="API_KEY_ACCOUNT_B",
                   api_secret="API_SECRET_ACCOUNT_B",
                   stream_label="ACCOUNT_B",
                   process_stream_data_async=process_userdata)
```

Per-stream credentials override the manager defaults. Isolated Margin additionally requires the `symbols` parameter:

```
ubwa_im = BinanceWebSocketApiManager(exchange="binance.com-isolated_margin")
ubwa_im.create_stream(channels='arr',
                      markets='!userData',
                      symbols='btcusdt',
                      api_key="YOUR_BINANCE_API_KEY",
                      api_secret="YOUR_BINANCE_API_SECRET",
                      process_stream_data_async=process_userdata)
```

See also [example_multiple_userdata_streams.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_multiple_userdata_streams.py).

## Convert received stream data into well-formed Python dictionaries with [UnicornFy](https://github.com/oliver-zehentleitner/unicorn-fy)

```
unicorn_fied_stream_data = UnicornFy.binance_com_websocket(data)
```

or

```
ubwa.create_stream(['trade'], ['btcusdt'], output="UnicornFy")
```

## [Subscribe](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.subscribe_to_stream) / [unsubscribe](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.unsubscribe_from_stream) new markets and channels

```
markets = ['engbtc', 'zileth']
channels = ['kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'depth5']

ubwa.subscribe_to_stream(stream_id=stream_id, channels=channels, markets=markets)

ubwa.unsubscribe_from_stream(stream_id=stream_id, markets=markets)

ubwa.unsubscribe_from_stream(stream_id=stream_id, channels=channels)
```

## Send Requests to Binance WebSocket API
### [Place orders](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.api.BinanceWebSocketApiApi.create_order), [cancel orders](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.api.BinanceWebSocketApiApi.cancel_order) or [send other requests](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#module-unicorn_binance_websocket_api.api) via WebSocket

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

api_key = "YOUR_BINANCE_API_KEY"
api_secret = "YOUR_BINANCE_API_SECRET"

async def process_api_responses(stream_id=None):
    while ubwa.is_stop_request(stream_id=stream_id) is False:
        data = await ubwa.get_stream_data_from_asyncio_queue(stream_id=stream_id)
        print(data)
        ubwa.asyncio_queue_task_done(stream_id=stream_id)

ubwa = BinanceWebSocketApiManager(exchange="binance.com")
api_stream = ubwa.create_stream(api=True,
                                api_key=api_key,
                                api_secret=api_secret,
                                output="UnicornFy",
                                process_asyncio_queue=process_api_responses)

response = ubwa.api.spot.get_server_time(return_response=True)
print(f"Binance serverTime: {response['result']['serverTime']}")

orig_client_order_id = ubwa.api.spot.create_order(order_type="LIMIT",
                                                  price = 1.1,
                                                  quantity = 15.0,
                                                  side = "SELL",
                                                  symbol = "BUSDUSDT")

ubwa.api.spot.cancel_order(orig_client_order_id=orig_client_order_id, symbol="BUSDUSDT")                                   
```

All available methods:
- [Futures](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#module-unicorn_binance_websocket_api.api.futures)
- [Spot](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#module-unicorn_binance_websocket_api.api.spot)


[Here](https://technopathy.club/create-and-cancel-orders-via-websocket-on-binance-7f828831404) you can find a complete 
guide on 
[how to process requests via the Binance WebSocket API](https://technopathy.club/create-and-cancel-orders-via-websocket-on-binance-7f828831404)!

## Stop `ubwa` after usage to avoid memory leaks

When you instantiate UBWA with `with`, `ubwa.stop_manager()` is automatically executed upon exiting the `with`-block.

```
with BinanceWebSocketApiManager() as ubwa:
    ubwa.create_stream(channels="trade", markets="btcusdt", stream_label="TRADES")
```

Without `with`, you must explicitly execute `ubwa.stop_manager()` yourself.

```
ubwa.stop_manager()
```

## `stream_signals` - know the state of your streams
Usually you want to know when a stream is working and when it is not. This can be useful to know that your own system is 
currently "blind" and you may want to close open positions to be on the safe side, know that indicators will now provide 
incorrect values or that you have to reload the missing data via REST as an alternative. 

For this purpose, the UNICORN Binance WebSocket API provides so-called 
[`stream_signals`](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signals%60), 
which are used to tell your code in real time when a stream is connected, when it received its first data record, when 
it was disconnected and stopped, and when the stream cannot be restored.

```
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import time

def process_stream_signals(signal_type=None, stream_id=None, data_record=None, error_msg=None):
    print(f"Received stream_signal for stream '{ubwa.get_stream_label(stream_id=stream_id)}': "
          f"{signal_type} - {stream_id} - {data_record} - {error_msg}")

with BinanceWebSocketApiManager(process_stream_signals=process_stream_signals) as ubwa:
    ubwa.create_stream(channels="trade", markets="btcusdt", stream_label="TRADES")
    print(f"Waiting a few seconds and then stopping the stream ...")
    time.sleep(7)
```

## More?

[Discover even more possibilities](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html), 
[use this script](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_stream_everything.py) 
to stream everything from "binance.com" or try our [examples](#examples)!

This should be known by everyone using this lib: 

- [Best practice solutions for a maximum stable connection](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/Best-practice-solutions-for-a-maximum-stable-connection!)
- [Do you want consistent data from binance?](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/discussions/254)

## Description
The Python package [UNICORN Binance WebSocket API](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api) 
provides an API to the Binance Websocket API`s of 
[Binance](https://github.com/binance-exchange/binance-official-api-docs) 
([+Testnet](https://testnet.binance.vision/)), 
[Binance Margin](https://binance-docs.github.io/apidocs/spot/en/#user-data-streams) 
([+Testnet](https://testnet.binance.vision/)), 
[Binance Isolated Margin](https://binance-docs.github.io/apidocs/spot/en/#listen-key-isolated-margin)
([+Testnet](https://testnet.binance.vision/)), 
[Binance Futures](https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams) 
([+Testnet](https://testnet.binancefuture.com)), 
[Binance COIN-M Futures](https://binance-docs.github.io/apidocs/delivery/en/#change-log),
[Binance European Options](https://developers.binance.com/docs/derivatives/option/general-info)
([+Testnet](https://testnet.binancefuture.com)),
[Binance US](https://github.com/binance-us/binance-official-api-docs) and
[Binance TR](https://www.trbinance.com/apidocs) and supports sending requests 
to the [Binance Websocket API](https://developers.binance.com/docs/binance-trading-api/websocket_api) and the streaming 
of all public streams like trade, kline, ticker, depth, bookTicker, forceOrder, compositeIndex etc. and 
also all private userData streams which needs to be used with a valid 
[api_key and api_secret](https://technopathy.club/how-to-create-a-binance-api-key-and-api-secret-3bb5f47e360d)
from the Binance Exchange [www.binance.com](https://www.binance.com/), 
[testnet.binance.vision](https://testnet.binance.vision/) or 
[www.binance.us](https://www.binance.us).

Use the [UNICORN Binance REST API](https://github.com/oliver-zehentleitner/unicorn-binance-rest-api) in combination. 

### What are the benefits of the UNICORN Binance WebSocket API?
- Fully managed websockets and 100% auto-reconnect! Also handles maintenance windows!

- No memory leaks from Python version 3.9 to 3.14!

- The full [UBS stack](https://github.com/oliver-zehentleitner/unicorn-binance-suite) is delivered as a compiled C extension for 
  maximum performance.

- Support for [Binance Websocket API](https://developers.binance.com/docs/binance-trading-api/websocket_api), send 
  requests like [create_order](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.ws_api.BinanceWebSocketApiWsApi.create_order),
  [cancel_open_orders](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.ws_api.BinanceWebSocketApiWsApi.cancel_open_orders)
  and [many more](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#module-unicorn_binance_websocket_api.ws_api) directly over websocket!

- [Supported exchanges](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/Binance-websocket-endpoint-configuration-overview): 

| Exchange                                                           | Exchange string                       | [WS](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/readme.html#receive-data-from-binance-websockets)                                                                                                                               | [WS API](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/readme.html#send-requests-to-binance-websocket-api)                                                                                                                           |
|--------------------------------------------------------------------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [Binance](https://www.binance.com)                                 | `binance.com`                         | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) |
| [Binance Testnet](https://testnet.binance.vision/)                 | `binance.com-testnet`                 | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) |
| [Binance Margin](https://www.binance.com)                          | `binance.com-margin`                  | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |                                                                                                                                     
| [Binance Margin Testnet](https://testnet.binance.vision/)          | `binance.com-margin-testnet`          | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance Isolated Margin](https://www.binance.com)                 | `binance.com-isolated_margin`         | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance Isolated Margin Testnet](https://testnet.binance.vision/) | `binance.com-isolated_margin-testnet` | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance USD-M Futures](https://www.binance.com)                   | `binance.com-futures`                 | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) |
| [Binance USD-M Futures Testnet](https://testnet.binancefuture.com) | `binance.com-futures-testnet`         | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) |
| [Binance Coin-M Futures](https://www.binance.com)                  | `binance.com-coin_futures`            | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance European Options](https://www.binance.com)                | `binance.com-vanilla-options`         | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance European Options Testnet](https://testnet.binancefuture.com) | `binance.com-vanilla-options-testnet` | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance Portfolio Margin](https://www.binance.com)*                | `binance.com-portfolio_margin`        | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance US](https://www.binance.us)                               | `binance.us`                          | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |
| [Binance TR](https://www.trbinance.com)                            | `trbinance.com`                       | ![yes](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ok-icon.png) | ![no](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/x-icon.png)   |

\* Portfolio Margin support is currently limited to the user data stream (`!userData`), see the
[Portfolio Margin example](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/tree/master/examples/binance_websocket_api_portfolio_margin)
and [issue #452](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/452).

- Streams are processing asynchronous/concurrent (Python asyncio) and each stream is started in a separate thread, so 
you don't need to deal with asyncio in your code! But you can consume with 
[`await`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/readme.html#or-await-the-webstream-data-in-an-asyncio-task)
, if you want!

- Supports 
[subscribe](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.subscribe_to_stream)/[unsubscribe](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.unsubscribe_from_stream)
on all exchanges! (Take a look to the max supported subscriptions per stream in the [endpoint configuration overview](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/Binance-websocket-endpoint-configuration-overview)!)

- [UNICORN Binance WebSocket API](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api) respects Binance's API guidelines and protects you from avoidable reconnects and bans.

- Support for multiple private `!userData` streams with different `api_key` and `api_secret`. 
  ([example_multiple_userdata_streams.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_multiple_userdata_streams.py))

- [Pick up the received data from the `stream_buffer`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=get_stream_info#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.pop_stream_data_from_stream_buffer) ([FIFO or LIFO](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_buffer%60)) - 
if you can not store your data in cause of a temporary technical issue, you can 
[kick back the data to the `stream_buffer`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=get_stream_info#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.add_to_stream_buffer) 
which stores the receives in the RAM till you are able to process the data in the normal way again. 
[Learn more!](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_buffer%60)

- Use separate `stream_buffers` for 
[specific streams](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_stream_buffer_extended.py) 
or 
[users](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_multiple_userdata_streams.py)!

- Watch the `stream_signals` to receive `CONNECT`, `FIRST_RECEIVED_DATA`, `DISCONNECT`, `STOP` and 
  `STREAM_UNREPAIRABLE` signals from your streams! [Learn more!](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signals%60)

- Get the received data unchanged as received, as Python dictionary or converted with 
[UnicornFy](https://github.com/oliver-zehentleitner/unicorn-fy) into well-formed Python dictionaries. Use the `output`
parameter of 
[`create_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=create_stream#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.create_stream) 
to control the output format.

- Helpful management features like 
[`clear_asyncio_queue()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.clear_asyncio_queue), 
[`clear_stream_buffer()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.clear_stream_buffer), 
[`get_binance_api_status()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_binance_api_status), 
[`get_current_receiving_speed()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_current_receiving_speed), 
[`get_errors_from_endpoints()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_errors_from_endpoints), 
[`get_limit_of_subscriptions_per_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_limit_of_subscriptions_per_stream), 
[`get_request_id()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_request_id), 
[`get_result_by_request_id()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_result_by_request_id),
[`get_results_from_endpoints()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_results_from_endpoints), 
[`get_stream_buffer_length()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_buffer_length), 
[`get_stream_info()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_info), 
[`get_stream_list()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_list), 
[`get_stream_id_by_label()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_id_by_label), 
[`get_stream_statistic()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_statistic), 
[`get_stream_subscriptions()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_subscriptions), 
[`get_version()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_version), 
[`is_update_available()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.is_update_availabe), 
[`get_stream_data_from_asyncio_queue()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_data_from_asyncio_queue), 
[`pop_stream_data_from_stream_buffer()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.pop_stream_data_from_stream_buffer), 
[`print_summary()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.print_summary), 
[`replace_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.replace_stream), 
[`set_stream_label()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.set_stream_label), 
[`set_ringbuffer_error_max_size()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.set_ringbuffer_error_max_size), 
[`subscribe_to_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.subscribe_to_stream), 
[`stop_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.stop_stream),
[`unsubscribe_from_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.unsubscribe_from_stream), 
[`wait_till_stream_has_started()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.wait_till_stream_has_started) 
and many more! Explore them [here](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/modules.html).

- Monitor the status of the created `BinanceWebSocketApiManager()` instance within your code with 
[`get_monitoring_status_plain()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=plain#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.get_monitoring_status_plain)
and specific streams with 
[`get_stream_info()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_stream_info).

- Available as a package via `pip` and `conda` as precompiled C extension with stub files for improved Intellisense 
  functions and source code for easier debugging of the source code. [To the installation.](#installation-and-upgrade)

- Nice to use with [iPython](https://ipython.org/): 
"IPython (Interactive Python) is a command shell for interactive computing that offers introspection, 
rich media, shell syntax, tab completion, and history." 
([example_interactive_mode.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_interactive_mode.py)) 
[![iPython](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/ipython.png)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_interactive_mode.py) 

- Also, nice to use with the [Jupyter Notebook](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/tree/master/ipynb) :)

- Integration of [test cases](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/unit-tests.yml) and [examples](#examples).

- Customizable base URL.

- *Socks5 Proxy* support:

  ```
  ubwa = BinanceWebSocketApiManager(exchange="binance.com", socks5_proxy_server="127.0.0.1:9050") 
  ```
  
  Read the [docs](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager)
  or this [how to](https://medium.com/@oliverzehentleitner/how-to-connect-to-binance-com-websockets-using-python-via-a-socks5-proxy-3c5a3e063f12) 
  for more information or try 
  [example_socks5_proxy.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_socks5_proxy.py).

- Excessively tested on Linux, Mac and Windows on x86, arm32, arm64, ...

If you like the project, please [![star](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/star.png)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/stargazers) it on 
[GitHub](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api)!

## Installation and Upgrade
The module requires Python 3.9 and runs smoothly up to and including Python 3.14.

PyPy wheels are available for all supported Python versions.

**conda-forge note:** Conda packages are provided for Python 3.10 – 3.14. Python 3.9 is not available on conda-forge — it was dropped from the global pinning after reaching end-of-life in October 2025. For Python 3.9, use `pip install`.


The current dependencies are listed [here](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/requirements.txt).

If you run into errors during the installation take a look [here](https://github.com/oliver-zehentleitner/unicorn-binance-suite/wiki/Installation).

### Packages are created automatically with GitHub Actions
When a new release is created, the
[Build and Publish GH+PyPi](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/actions/workflows/build_wheels.yml)
workflow spins up virtual Windows/Linux/Mac runners, compiles the Cython extensions, builds the
wheels and publishes them on GitHub and PyPI. The conda-forge feedstock
[conda-forge/unicorn-binance-websocket-api-feedstock](https://github.com/conda-forge/unicorn-binance-websocket-api-feedstock)
picks up the new PyPI release automatically and builds the Conda packages on its own infrastructure.
This is a transparent method that makes it possible to trace the source code behind a compilation.

### A Cython binary, PyPy or source code based CPython wheel of the latest version with `pip` from [PyPI](https://pypi.org/project/unicorn-binance-rest-api/)
Our [Cython](https://cython.org/) and [PyPy](https://www.pypy.org/) Wheels are available on [PyPI](https://pypi.org/), 
these wheels offer significant advantages for Python developers:

- ***Performance Boost with Cython Wheels:*** Cython is a programming language that supplements Python with static typing and C-level performance. By compiling 
  Python code into C, Cython Wheels can significantly enhance the execution speed of Python code, especially in 
  computationally intensive tasks. This means faster runtimes and more efficient processing for users of our package. 

- ***PyPy Wheels for Enhanced Efficiency:*** PyPy is an alternative Python interpreter known for its speed and efficiency. It uses Just-In-Time (JIT) compilation, 
  which can dramatically improve the performance of Python code. Our PyPy Wheels are tailored for compatibility with 
  PyPy, allowing users to leverage this speed advantage seamlessly.

Both Cython and PyPy Wheels on PyPI make the installation process simpler and more straightforward. They ensure that 
you get the optimized version of our package with minimal setup, allowing you to focus on development rather than 
configuration.

On Raspberry Pi and other architectures for which there are no pre-compiled versions, the package can still be 
installed with PIP. PIP then compiles the package locally on the target system during installation. Please be patient, 
this may take some time!

#### Installation
`pip install unicorn-binance-websocket-api`

#### Update
`pip install unicorn-binance-websocket-api --upgrade`

### conda
```
conda install -c conda-forge unicorn-binance-websocket-api
```

### From source of the latest release with PIP from [GitHub](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api)
#### Linux, macOS, ...
Run in bash:

`pip install https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/archive/$(curl -s https://api.github.com/repos/oliver-zehentleitner/unicorn-binance-websocket-api/releases/latest | grep -oP '"tag_name": "\K(.*)(?=")').tar.gz --upgrade`

#### Windows
Use the below command with the version (such as 2.14.0) you determined 
[here](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/releases/latest):

`pip install https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/archive/2.14.0.tar.gz --upgrade`
### From the latest source (dev-stage) with PIP from [GitHub](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api)
This is not a release version and can not be considered to be stable!

`pip install https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/tarball/master --upgrade`

## Change Log
[https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/changelog.html](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/changelog.html)

## Documentation
- [General](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api)
- [Modules](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/modules.html)

## Examples
- [Look here!](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/tree/master/examples/)

## Related Articles
- [The Complete Binance Python API Guide 2026](https://blog.technopathy.club/the-complete-binance-python-api-guide-2026)
- [How to create a Binance API Key and API Secret?](https://blog.technopathy.club/how-to-create-a-binance-api-key-and-api-secret)
- [Create and Cancel Orders via WebSocket on Binance](https://blog.technopathy.club/create-and-cancel-orders-via-websocket-on-binance)
- [How to Download Klines from Binance using Python?](https://blog.technopathy.club/how-to-download-klines-from-binance-using-python)
- [Passing Binance Market Data to Apache Kafka in Python with aiokafka](https://blog.technopathy.club/passing-binance-market-data-to-apache-kafka-in-python-with-aiokafka)
- [How to Connect to binance.com Websockets using Python via a Socks5 Proxy](https://blog.technopathy.club/binance-websocket-via-socks5)
- [When IP Whitelisting Isn't What It Seems: A Real-World Case Study from the Binance API](https://blog.technopathy.club/when-ip-whitelisting-isn-t-what-it-seems-a-real-world-case-study-from-the-binance-api)
- [UBDCC Deep Dive: Building a Trust Layer for Binance Order Books](https://blog.technopathy.club/ubdcc-deep-dive-building-a-trust-layer-for-binance-order-books)
- [UNICORN Binance Suite Article Series](https://blog.technopathy.club/series/unicorn-binance-suite)

## Project Homepage
[https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api)

## Wiki
[https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki)

## Social
- [Discussions](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/discussions)
- [Telegram](https://t.me/unicorndevs) 
- [Reddit Community](https://www.reddit.com/r/UNICORNBinanceSuite/) 
- [https://dev.binance.vision](https://dev.binance.vision)

## Receive Notifications
To receive notifications on available updates you can 
[![watch](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/watch.png)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/watchers) 
the repository on [GitHub](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api), write your 
[own script](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/examples/_archive/example_version_of_this_package.py) 
with using 
[`is_update_available()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.is_update_availabe) 
or you use [`get_monitoring_status_plain()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_monitoring_status_plain).

To receive news (like inspection windows/maintenance) about the Binance API`s subscribe to their telegram groups: 

- [https://t.me/binance_api_announcements](https://t.me/binance_api_announcements)
- [https://t.me/binance_api_english](https://t.me/binance_api_english)
- [https://t.me/Binance_USA](https://t.me/Binance_USA)
- [https://t.me/TRBinanceTR](https://t.me/TRBinanceTR)
- [https://t.me/BinanceExchange](https://t.me/BinanceExchange)

## How to report Bugs or suggest Improvements?
[List of planned features](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement) - click ![thumbs-up](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/thumbup.png) if you need one of them or suggest a new feature!

Before you report a bug, [try the latest release](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api#installation-and-upgrade). If the issue still exists, provide the error trace, OS 
and Python version and explain how to reproduce the error. A demo script is appreciated.

If you don't find an issue related to your topic, please open a new [issue](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues)!

[Report a security bug!](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/security/policy)

## Contributing
[UNICORN Binance WebSocket API](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api) is an open 
source project which welcomes contributions which can be anything from simple documentation fixes and reporting dead links to new features. To 
contribute follow 
[this guide](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/CONTRIBUTING.md).
 
### Contributors
[![Contributors](https://contributors-img.web.app/image?repo=oliver-zehentleitner/unicorn-binance-websocket-api)](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/graphs/contributors)

We ![love](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/master/images/misc/heart.png) open source!

---

## AI Integration

This project provides a [`llms.txt`](https://raw.githubusercontent.com/oliver-zehentleitner/unicorn-binance-websocket-api/refs/heads/master/llms.txt) file for AI tools (ChatGPT, Claude, Copilot, etc.) with structured 
usage instructions, code examples and module routing.

---

## Disclaimer
This project is for informational purposes only. You should not construe this information or any other material as 
legal, tax, investment, financial or other advice. Nothing contained herein constitutes a solicitation, recommendation, 
endorsement or offer by us or any third party provider to buy or sell any securities or other financial instruments in 
this or any other jurisdiction in which such solicitation or offer would be unlawful under the securities laws of such 
jurisdiction.

### If you intend to use real money, use it at your own risk!

Under no circumstances will we be responsible or liable for any claims, damages, losses, expenses, costs or liabilities 
of any kind, including but not limited to direct or indirect damages for loss of profits.

### SOCKS5 Proxy / Geoblocking
We would like to explicitly point out that in our opinion US citizens are exclusively authorized to trade on Binance.US 
and that this restriction must not be circumvented!

The purpose of supporting a SOCKS5 proxy in the UNICORN Binance Suite and its modules is to allow non-US citizens to use 
US services. For example, GitHub actions with UBS will not work without a SOCKS5 proxy, as they will inevitably run on 
servers in the US and be blocked by Binance.com. Moreover, it also seems justified that traders, data scientists and 
companies from the US analyze binance.com market data - as long as they do not trade there.
