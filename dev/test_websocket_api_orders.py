#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: binance_websocket_api_futures.py
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

from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os

api_key = ""
api_secret = ""
symbol = "ETHUSDT"


async def binance_stream(ubwa):
    def handle_socket_message(data):
        print(f"received data:\r\n{data}\r\n")

    api_stream = ubwa.create_stream(
        api=True,
        api_key=api_key,
        api_secret=api_secret,
        process_stream_data=handle_socket_message,
    )

    # LIMIT ORDER
    ubwa.api.create_order(
        stream_id=api_stream,
        price=2888.48,
        order_type="LIMIT",
        quantity=0.0048,
        side="SELL",
        symbol=symbol,
    )

    # LIMIT_MAKER ORDER

    # MARKET ORDER
    ubwa.api.create_order(
        stream_id=api_stream,
        order_type="MARKET",
        quantity=0.0048,
        side="SELL",
        symbol=symbol,
    )

    # STOP_LOSS ORDER

    # STOP_LOSS_LIMIT ORDER

    # TAKE_PROFIT ORDER

    # TAKE_PROFIT_LIMIT ORDER

    await asyncio.sleep(5)
    print(f"Stopping!")
    ubwa.stop_manager()


if __name__ == "__main__":
    logging.getLogger("unicorn_binance_websocket_api")
    logging.basicConfig(
        level=logging.DEBUG,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )
    ubwa = BinanceWebSocketApiManager(exchange="binance.com")
    try:
        asyncio.run(binance_stream(ubwa))
    except KeyboardInterrupt:
        print("\r\nGracefully stopping the websocket manager...")
        ubwa.stop_manager()
