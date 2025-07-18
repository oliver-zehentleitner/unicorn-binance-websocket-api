#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dev/test_binance_futures.py
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

import logging
import os
import sys
import time
from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
from lucit_licensing_python.exceptions import NoValidatedLucitLicense


logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

try:
    # To use this library you need a valid UNICORN Binance Suite License:
    # https://shop.lucit.services
    ubwa = BinanceWebSocketApiManager(lucit_license_profile="LUCIT")
except NoValidatedLucitLicense as error_msg:
    print(f"ERROR LEVEL 1: {error_msg}")
    sys.exit(1)

ubwa.create_stream("trade", "btcusdt", output="UnicornFy")

try:
    while ubwa.is_manager_stopping() is False:
        time.sleep(1)
        print(f"Trades: {ubwa.pop_stream_data_from_stream_buffer()}")
except KeyboardInterrupt:
    print(f"Stopping ...")
    ubwa.stop_manager()
