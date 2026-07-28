#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_remove_streams_from_manager.py
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
import os
import time

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


def callback_data(stream_data):
    print(f"DATA: {stream_data}")


def callback_signals(
    signal_type=None, stream_id=None, data_record=None, error_msg=None
):
    print(f"SIGNAL: {signal_type} - {stream_id} - {data_record} - {error_msg}")


with BinanceWebSocketApiManager(
    auto_data_cleanup_stopped_streams=True,
    exchange="binance.com",
    process_stream_signals=callback_signals,
) as ubwa:
    stream_id = ubwa.create_stream(
        "depth20@1000ms", "BNBBUSD", output="dict", process_stream_data=callback_data
    )
    time.sleep(5)
    ubwa.stop_stream(stream_id)
    print(f"waiting till stream has stopped")
    if ubwa.wait_till_stream_has_stopped(stream_id):
        deleted = ubwa.remove_all_data_of_stream_id(stream_id)
        print(f"deleted data of stream_id {stream_id}")
