#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dev/test_dex_subscribe.py
#
# Part of ‘UNICORN Binance WebSocket API’
# Project website: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Github: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Documentation: https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api
# PyPI: https://pypi.org/project/unicorn-binance-websocket-api
#
# Author: Oliver Zehentleitner
#
# Copyright (c) 2019-2024, LUCIT Systems and Development (https://www.lucit.tech)
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
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")


def print_stream_data_from_stream_buffer(binance_websocket_api_manager):
    while True:
        if binance_websocket_api_manager.is_manager_stopping():
            exit(0)
        oldest_stream_data_from_stream_buffer = binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        if oldest_stream_data_from_stream_buffer is None:
            time.sleep(0.01)
        else:
            #pass
            print(oldest_stream_data_from_stream_buffer)


# To use this library you need a valid UNICORN Binance Suite License:
# https://shop.lucit.services
# create instance of BinanceWebSocketApiManager for Binance Chain DEX
binance_websocket_api_manager = BinanceWebSocketApiManager(exchange="binance.org")

# start a worker process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(target=print_stream_data_from_stream_buffer, args=(binance_websocket_api_manager,))
worker_thread.start()

markets = ['RAVEN-F66_BNB', 'ANKR-E97_BNB', 'AWC-986_BNB', 'COVA-218_BNB', 'BCPT-95A_BNB', 'WISH-2D5_BNB',
           'MITH-C76_BNB', 'BNB_BTCB-1DE', 'BNB_USDSB-1AC', 'BTCB-1DE_USDSB-1AC', 'NEXO-A84_BNB']

stream_id = binance_websocket_api_manager.create_stream(["kline_1m"], markets)

markets = ['RAVEN-F66_BNB', 'ANKR-E97_BNB', 'AWC-986_BNB', 'COVA-218_BNB', 'BCPT-95A_BNB', 'WISH-2D5_BNB',
           'MITH-C76_BNB', 'BNB_BTCB-1DE', 'BTCB-1DE_USDSB-1AC', 'NEXO-A84_BNB']
channels = ['kline_5m', 'kline_15m']

binance_websocket_api_manager.subscribe_to_stream(stream_id,
                                                  channels=['kline_1m', 'kline_5m', 'marketDepth',
                                                            'ticker', 'miniTicker', 'marketDiff'])

binance_websocket_api_manager.unsubscribe_from_stream(stream_id,
                                                      channels=['kline_1m', 'marketDepth',
                                                                'ticker', 'miniTicker', 'marketDiff'],
                                                      markets=markets)

binance_websocket_api_manager.get_stream_subscriptions(stream_id)


while True:
    #binance_websocket_api_manager.print_summary()
    binance_websocket_api_manager.print_stream_info(stream_id)
    time.sleep(1)
