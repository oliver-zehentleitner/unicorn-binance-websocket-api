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

from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
from unicorn_binance_rest_api import BinanceRestApiManager
import asyncio
import logging
import os
import time

api_key = ""
api_secret = ""


async def binance_stream(ubwa):
    async def handle_socket_message(stream_id=None):
        while ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await ubwa.get_stream_data_from_asyncio_queue(stream_id=stream_id)
            try:
                if data["result"]["serverTime"] is not None:
                    print(
                        f"UBWA: Time between request and answer: {(time.time() - start_time_get_server_time_ubwa)} "
                        f"seconds\r\nreceived data:\r\n{data}\r\n"
                    )
            except KeyError:
                print(f"received data:\r\n{data}\r\n")
            except TypeError:
                print(f"received data:\r\n{data}\r\n")

    api_stream = ubwa.create_stream(
        api=True,
        api_key=api_key,
        api_secret=api_secret,
        stream_label="Bobs Websocket API",
        process_asyncio_queue=handle_socket_message,
    )
    time.sleep(5)
    for i in range(10):
        print(f"##################################################################")
        start_time_get_server_time_ubwa = time.time()
        ubwa.api.get_server_time(stream_id=api_stream)
        ubwa.api.get_server_time(stream_id=api_stream)
        ubwa_server_time = ubwa.api.get_server_time(
            stream_id=api_stream, return_response=True
        )
        print(
            f"UBRA: Time between request and answer: {(time.time() - start_time_get_server_time_ubwa)} "
            f"seconds\r\nreceived data:\r\n{ubwa_server_time}\r\n"
        )
        print(f"Finished! Waiting for responses ...")
        time.sleep(2)
        print(f"------------------------------------------------------------------")
        start_time_get_server_time_ubra = time.time()
        ubra_server_time = ubra.get_server_time()
        print(
            f"UBRA: Time between request and answer: {(time.time() - start_time_get_server_time_ubra)} "
            f"seconds\r\nreceived data:\r\n{ubra_server_time}\r\n"
        )
        ubra_server_time = ubra.get_server_time()
        print(
            f"UBRA: Time between request and answer: {(time.time() - start_time_get_server_time_ubra)} "
            f"seconds\r\nreceived data:\r\n{ubra_server_time}\r\n"
        )
        ubra_server_time = ubra.get_server_time()
        print(
            f"UBRA: Time between request and answer: {(time.time() - start_time_get_server_time_ubra)} "
            f"seconds\r\nreceived data:\r\n{ubra_server_time}\r\n"
        )
    print(f"Stopping!")
    ubwa.stop_manager()
    ubra.stop_manager()


if __name__ == "__main__":
    logging.getLogger("unicorn_binance_websocket_api")
    logging.basicConfig(
        level=logging.DEBUG,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )

    ubwa = BinanceWebSocketApiManager(exchange="binance.com", output_default="dict")
    ubra = BinanceRestApiManager(exchange="binance.com")
    try:
        asyncio.run(binance_stream(ubwa))
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
        ubwa.stop_manager()
        ubra.stop_manager()
