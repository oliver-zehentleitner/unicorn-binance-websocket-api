#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_binance_futures.py
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
import threading
import os

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


def print_stream_data_from_stream_buffer(binance_websocket_api_manager):
    while True:
        if binance_websocket_api_manager.is_manager_stopping():
            exit(0)
        oldest_stream_data_from_stream_buffer = (
            binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        )
        if oldest_stream_data_from_stream_buffer is None:
            time.sleep(0.01)


# create instance of BinanceWebSocketApiManager for Binance.com Futures
binance_websocket_api_manager = BinanceWebSocketApiManager(
    exchange="binance.com-futures"
)

# Set api_key and secret for userData stream
binance_api_key = ""
binance_api_secret = ""
userdata_stream_id = binance_websocket_api_manager.create_stream(
    ["arr"], ["!userData"], api_key=binance_api_key, api_secret=binance_api_secret
)

bookticker_all_stream_id = binance_websocket_api_manager.create_stream(
    ["arr"], ["!bookTicker"]
)

# https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream-for-all-market
binance_websocket_api_manager.create_stream(
    ["!markPrice"], "arr@1s", stream_label="!markPrice@arr@1s"
)

markets = {"btcusdt", "bchusdt", "ethusdt"}
binance_websocket_api_manager.create_stream(["aggTrade"], markets)
binance_websocket_api_manager.create_stream(["markPrice"], markets)
binance_websocket_api_manager.create_stream(["kline_1m"], markets)
binance_websocket_api_manager.create_stream(["kline_5m"], markets)
binance_websocket_api_manager.create_stream(["kline_15m"], markets)
binance_websocket_api_manager.create_stream(["kline_1h"], markets)
binance_websocket_api_manager.create_stream(["kline_12h"], markets)
binance_websocket_api_manager.create_stream(["kline_1w"], markets)
binance_websocket_api_manager.create_stream(["ticker"], markets)
binance_websocket_api_manager.create_stream(["miniTicker"], markets)
binance_websocket_api_manager.create_stream(["bookTicker"], markets)
binance_websocket_api_manager.create_stream(["depth"], markets)
binance_websocket_api_manager.create_stream(["depth@2500ms"], markets)
binance_websocket_api_manager.create_stream(["depth5"], markets)
binance_websocket_api_manager.create_stream(["depth5@100ms"], markets)
binance_websocket_api_manager.create_stream(["depth10"], markets)
binance_websocket_api_manager.create_stream(["depth20"], markets)
binance_websocket_api_manager.create_stream(
    ["compositeIndex"], markets, stream_label="compositeIndex"
)

channels = {
    "aggTrade",
    "markPrice" "kline_1m",
    "kline_5m",
    "kline_15m",
    "kline_30m",
    "kline_1h",
    "kline_12h",
    "!miniTicker",
    "depth20@100ms",
    "bookTicker",
    "!forceOrder",
    "kline_1w@250ms",
    "compositeIndex",
}
binance_websocket_api_manager.create_stream(channels, markets)

# start a worker process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(binance_websocket_api_manager,)
)
worker_thread.start()

# show an overview
while True:
    binance_websocket_api_manager.print_summary()
    time.sleep(1)
