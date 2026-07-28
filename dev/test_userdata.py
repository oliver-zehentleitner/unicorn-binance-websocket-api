#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dev/test_userdata.py
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
import time

from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
from unicorn_binance_websocket_api.exceptions import Socks5ProxyConnectionError
import asyncio
import logging
import os
import tracemalloc

tracemalloc.start(25)

# socks5_proxy = "127.0.0.1:1080"
socks5_proxy = None
socks5_ssl_verification = True

api_key = ""
api_secret = ""


async def binance_stream(ubwa):
    def handle_socket_message(data):
        print(f"received data: {data}")

    ubwa.create_stream(
        markets="arr",
        channels="!userData",
        api_key=api_key,
        api_secret=api_secret,
        stream_label="Bobs UserData",
        process_stream_data=handle_socket_message,
    )

    while ubwa.is_manager_stopping() is False:
        await asyncio.sleep(1)
        ubwa.print_summary()
        # ubwa.print_stream_info(ubwa.get_stream_id_by_label("Bobs UserData"))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )
    try:
        ubwa = BinanceWebSocketApiManager(
            exchange="binance.com",
            show_secrets_in_logs=True,
            socks5_proxy_server=socks5_proxy,
            socks5_proxy_ssl_verification=socks5_ssl_verification,
        )
    except Socks5ProxyConnectionError as error_msg:
        print(f"Socks5ProxyConnectionError: {error_msg}")
        exit(1)

    try:
        asyncio.run(binance_stream(ubwa))
    except KeyboardInterrupt:
        print("\r\nGracefully stopping the websocket manager...")
        ubwa.stop_manager()
        print("Done! Good bye!")
