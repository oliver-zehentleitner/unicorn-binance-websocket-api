#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dev/test_websocket_api.py
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
import unicorn_binance_rest_api
import asyncio
import logging
import os
import time

api_key = ""
api_secret = ""


async def binance_stream(ubwa):
    def handle_socket_message(data):
        print(f"received data:\r\n{data}\r\n")

    def handle_ping_response(data):
        print(f"received ping response:\r\n{data}\r\n")

    def handle_servertime_response(data):
        print(f"received server time response:\r\n{data}\r\n")

    ubwa.create_stream(
        api=True,
        api_key=api_key,
        api_secret=api_secret,
        output="dict",
        process_stream_data=handle_socket_message,
        stream_label="Bobs Websocket API",
    )
    print(f"Start:")
    ubwa.api.get_server_time(
        process_response=handle_servertime_response, stream_label="Bobs Websocket API"
    )
    ubwa.api.get_account_status(stream_label="Bobs Websocket API")
    orig_client_order_id = ubwa.api.create_order(
        price=1.0, order_type="LIMIT", quantity=15.0, side="SELL", symbol="BUSDUSDT"
    )
    ubwa.api.create_test_order(
        price=1.2, order_type="LIMIT", quantity=12.0, side="SELL", symbol="BUSDUSDT"
    )
    ubwa.api.ping(process_response=handle_ping_response)
    ubwa.api.get_exchange_info(symbols=["BUSDUSDT"])
    ubwa.api.get_order_book(symbol="BUSDUSDT", limit=2)
    ubwa.api.cancel_order(symbol="BUSDUSDT", orig_client_order_id=orig_client_order_id)
    ubwa.api.get_open_orders(symbol="BUSDUSDT")
    ubwa.api.cancel_open_orders(symbol="BUSDUSDT")
    ubwa.api.get_order(symbol="BUSDUSDT", orig_client_order_id=orig_client_order_id)

    print(f"Finished! Waiting for responses:")
    await asyncio.sleep(5)
    print(f"Timestamp: {time.time()}")
    print(f"Test WS ping:")
    time1 = time.time()
    ping = ubwa.api.ping(return_response=True)
    print(f"Timestamp: {time.time()}")
    print(f"Awaited ping response: {ping}")
    print(f"Timestamp: {time.time()}")
    time2 = time.time()
    print(f"time: {time2-time1}")

    print(f"Test REST ping:")
    ubra = unicorn_binance_rest_api.BinanceRestApiManager()
    time1 = time.time()
    print(f"Timestamp: {time.time()}")
    print(f"Awaited ping response: {ubra.get_server_time()['serverTime']}")
    print(f"Timestamp: {time.time()}")
    time2 = time.time()
    print(f"time: {time2-time1}")

    print(f"Stopping!")
    ubwa.stop_manager_with_all_streams()


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
        ubwa.stop_manager_with_all_streams()
