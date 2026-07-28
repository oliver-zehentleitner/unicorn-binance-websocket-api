#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_userdata_stream_new_style.py
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
    level=logging.INFO,
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
        else:
            print(oldest_stream_data_from_stream_buffer)


# configure api key and secret for binance.com
api_key = ""
api_secret = ""

# create instances of BinanceWebSocketApiManager
ubwa_com = BinanceWebSocketApiManager(exchange="binance.com")

# create the userData streams
user_stream_id = ubwa_com.create_stream(
    "arr", "!userData", api_key=api_key, api_secret=api_secret
)

# start a worker process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(ubwa_com,)
)
worker_thread.start()

# configure api key and secret for binance.com Isolated Margin
api_key = ""
api_secret = ""

# create instances of BinanceWebSocketApiManager
ubwa_com_im = BinanceWebSocketApiManager(exchange="binance.com-isolated_margin")

# create the userData streams
user_stream_id_im = ubwa_com_im.create_stream(
    "arr", "!userData", symbols="trxbtc", api_key=api_key, api_secret=api_secret
)

# start a worker process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(ubwa_com_im,)
)
worker_thread.start()

# monitor the streams
while True:
    ubwa_com.print_stream_info(user_stream_id)
    ubwa_com_im.print_stream_info(user_stream_id_im)
    time.sleep(1)
