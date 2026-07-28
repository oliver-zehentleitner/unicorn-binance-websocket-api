#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dev/test_callback.py
#
# Part of ‘UNICORN Binance WebSocket API’
# Project website: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Github: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Documentation: https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api
# PyPI: https://pypi.org/project/unicorn-binance-websocket-api
#
# Author: Oliver Zehentleitner
#
# Copyright (c) 2019-2026, Oliver Zehentleitner (https://about.me/oliver-zehentleitner)
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
import math
import os
import requests
import sys
import time

try:
    import unicorn_binance_rest_api
except ImportError:
    print(
        "Please install `unicorn-binance-rest-api`! https://pypi.org/project/unicorn-binance-rest-api/"
    )
    sys.exit(1)

binance_api_key = ""
binance_api_secret = ""
channels = {
    "aggTrade",
    "trade",
    "kline_1m",
    "kline_5m",
    "kline_15m",
    "kline_30m",
    "kline_1h",
    "kline_2h",
    "kline_4h",
    "kline_6h",
    "kline_8h",
    "kline_12h",
    "kline_1d",
    "kline_3d",
    "kline_1w",
    "kline_1M",
    "miniTicker",
    "ticker",
    "bookTicker",
    "depth5",
    "depth10",
    "depth20",
    "depth",
    "depth@100ms",
}
arr_channels = {"!miniTicker", "!ticker", "!bookTicker"}

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.INFO,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


def print_stream_data(stream_data, stream_buffer_name=False):
    print(str(stream_data))


try:
    binance_rest_client = unicorn_binance_rest_api.BinanceRestApiManager(
        binance_api_key, binance_api_secret
    )
    binance_websocket_api_manager = BinanceWebSocketApiManager()
except requests.exceptions.ConnectionError:
    print("No internet connection?")
    sys.exit(1)


markets = []
data = binance_rest_client.get_all_tickers()
for item in data:
    markets.append(item["symbol"])

private_stream_id = binance_websocket_api_manager.create_stream(
    ["!userData"],
    ["arr"],
    api_key=binance_api_key,
    api_secret=binance_api_secret,
    stream_label="userData stream!",
    process_stream_data=print_stream_data,
)

binance_websocket_api_manager.create_stream(
    arr_channels, "arr", stream_label="`arr` channels"
)

divisor = math.ceil(
    len(markets) / binance_websocket_api_manager.get_limit_of_subscriptions_per_stream()
)
max_subscriptions = math.ceil(len(markets) / divisor)

for channel in channels:
    if len(markets) <= max_subscriptions:
        binance_websocket_api_manager.create_stream(
            channel,
            markets,
            stream_label=channel,
            process_stream_data=print_stream_data,
        )
    else:
        loops = 1
        i = 1
        markets_sub = []
        for market in markets:
            markets_sub.append(market)
            if i == max_subscriptions or loops * max_subscriptions + i == len(markets):
                binance_websocket_api_manager.create_stream(
                    channel,
                    markets_sub,
                    stream_label=str(channel + "_" + str(i)),
                    process_stream_data=print_stream_data,
                )
                markets_sub = []
                i = 1
                loops += 1
            i += 1

while True:
    # binance_websocket_api_manager.print_summary()
    time.sleep(1)
