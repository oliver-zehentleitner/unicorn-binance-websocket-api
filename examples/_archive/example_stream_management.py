#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_stream_management.py
#
# Part of ‘UNICORN Binance WebSocket API’
# Project website: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Github: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Documentation: https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api
# PyPI: https://pypi.org/project/unicorn-binance-websocket-api
#
# Author: Oliver Zehentleitner
#
# Copyright (c) 2019-2024, Oliver Zehentleitner (https://about.me/oliver-zehentleitner)
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import logging
import time
import os

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.INFO,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)

# create instance of BinanceWebSocketApiManager and provide the function for stream processing
binance_websocket_api_manager = BinanceWebSocketApiManager()

# define some markets
markets = {
    "bnbbtc",
    "ethbtc",
    "btcusdt",
    "bchabcusdt",
    "xrpusdt",
    "rvnbtc",
    "ltcusdt",
    "adausdt",
    "eosusdt",
    "neousdt",
    "bnbusdt",
    "adabtc",
    "ethusdt",
    "trxbtc",
    "bchabcbtc",
    "ltcbtc",
    "xrpbtc",
    "ontbtc",
    "bttusdt",
    "eosbtc",
    "xlmbtc",
    "bttbtc",
    "tusdusdt",
    "xlmusdt",
    "qkcbtc",
    "zrxbtc",
    "neobtc",
    "adaeth",
    "icxusdt",
    "btctusd",
    "icxbtc",
    "btcusdc",
    "wanbtc",
    "zecbtc",
    "wtcbtc",
    "ethtusd",
    "batusdt",
    "mcobtc",
    "neoeth",
    "bntbtc",
    "eostusd",
    "lrcbtc",
    "funbtc",
    "zecusdt",
    "bnbpax",
    "linkusdt",
    "hceth",
    "zrxeth",
    "icxeth",
    "xmreth",
    "neobnb",
    "etceth",
    "zeceth",
    "xmrbnb",
    "wanbnb",
    "zrxbnb",
    "agibnb",
    "funeth",
    "arketh",
    "engeth",
}

# create stream 1
binance_get_ticker_stream_id = binance_websocket_api_manager.create_stream(
    "arr", "!ticker"
)

# create stream 2
binance_get_kline_stream_id = binance_websocket_api_manager.create_stream(
    ["kline_1m", "kline_5m", "kline_30m", "kline_1h", "kline_15m"], markets
)
binance_get_trade_stream_id = binance_websocket_api_manager.create_stream(
    ["trade"], markets
)

time.sleep(5)

# replace stream 1 with stream 3 (stream 1 will not get stopped till stream 3 received its first data row
binance_get_multi_stream_id = binance_websocket_api_manager.replace_stream(
    binance_get_ticker_stream_id,
    ["trade", "depth", "depth10", "aggTrade", "miniTicker"],
    markets,
)

# subscribe new markets to a stream
markets = [
    "engbtc",
    "zileth",
    "xlmeth",
    "eosbnb",
    "xrppax",
    "lskbtc",
    "npxsbtc",
    "xmrusdt",
    "ltcpax",
    "xmrusdt",
    "ethtusd",
    "batusdt",
    "mcobtc",
    "neoeth",
    "bntbtc",
    "eostusd",
    "lrcbtc",
    "funbtc",
    "zecusdt",
    "bnbpax",
    "linkusdt",
    "hceth",
    "zrxeth",
    "icxeth",
    "xmreth",
    "neobnb",
    "etceth",
    "zeceth",
    "xmrbnb",
    "wanbnb",
    "zrxbnb",
    "agibnb",
    "funeth",
    "arketh",
    "engeth",
]
binance_websocket_api_manager.subscribe_to_stream(
    binance_get_multi_stream_id, markets=markets
)
time.sleep(3)
binance_websocket_api_manager.get_stream_subscriptions(binance_get_multi_stream_id)

# monitor the streams
while True:
    binance_websocket_api_manager.print_stream_info(binance_get_multi_stream_id)
    binance_websocket_api_manager.print_stream_info(binance_get_ticker_stream_id)
    binance_websocket_api_manager.print_stream_info(binance_get_kline_stream_id)
    binance_websocket_api_manager.print_summary(
        add_string="custom text: YOUR TEXT HERE ;)"
    )
    time.sleep(1)
