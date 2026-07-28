#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_stream_buffer_extended.py
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

from __future__ import print_function
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

# create instance of BinanceWebSocketApiManager
ubwa = BinanceWebSocketApiManager()


markets = [
    "xrpbearbusd",
    "zeceth",
    "cndbtc",
    "dashbtc",
    "atompax",
    "perlbtc",
    "ardreth",
    "zecbnb",
    "bchabctusd",
    "usdsbusdt",
    "winbnb",
    "xzcxrp",
    "bchusdc",
    "wavesbnb",
    "kavausdt",
    "btsusdt",
    "chzbnb",
    "tusdbnb",
    "xtzbusd",
    "bcptusdc",
    "dogebnb",
    "eosbearusdt",
    "ambbnb",
    "wrxbnb",
    "poabtc",
    "wanbtc",
    "ardrbtc",
    "icnbtc",
    "tusdusdt",
    "atombusd",
    "nxseth",
    "bnbusdt",
    "trxxrp",
    "erdpax",
    "erdbtc",
    "icxbusd",
    "nulsbtc",
    "hotusdt",
    "wavespax",
    "zilbnb",
    "arnbtc",
    "nulsusdt",
    "wintrx",
    "npxsbtc",
    "busdtry",
    "qtumbnb",
    "eosbtc",
    "xlmpax",
    "tomobnb",
    "eosbnb",
    "engbtc",
    "linketh",
    "xrpbtc",
    "fetbtc",
    "stratusdt",
    "navbnb",
    "bcneth",
    "yoyobtc",
    "nanobnb",
    "saltbtc",
    "tfuelusdc",
    "skybnb",
    "fuelbtc",
    "bnbusdc",
    "inseth",
    "btcpax",
    "batbtc",
    "rlceth",
    "arketh",
    "ltcpax",
    "ltcbusd",
    "duskbtc",
    "mftusdt",
    "bntusdt",
    "mdabtc",
    "enjbtc",
    "poabnb",
    "nanobusd",
    "paxtusd",
    "hotbtc",
    "bcdbtc",
    "beambnb",
    "trxeth",
    "omgbnb",
    "cdtbtc",
    "eosusdc",
    "dashbusd",
    "cocosbtc",
    "dasheth",
    "xrptusd",
    "atomtusd",
    "rcneth",
    "rpxeth",
    "xlmusdc",
    "aionbusd",
    "nxsbtc",
    "chateth",
    "repbtc",
    "tctusdt",
    "linkusdt",
    "nasbtc",
    "usdsusdc",
    "xvgbtc",
    "elfeth",
    "ctxcbtc",
    "cmteth",
    "gnteth",
    "usdspax",
]

channels = ["kline_1m", "trade", "depth20"]

for channel in channels:
    ubwa.create_stream(channel, markets, stream_buffer_name=channel)


def print_stream_data_from_stream_buffer(ubwa):
    print("print trades only")
    time.sleep(10)
    while True:
        if ubwa.is_manager_stopping():
            exit(0)
        oldest_stream_data_from_stream_buffer = ubwa.pop_stream_data_from_stream_buffer(
            "kline_1m"
        )
        if oldest_stream_data_from_stream_buffer is None:
            time.sleep(0.01)
        else:
            try:
                print(oldest_stream_data_from_stream_buffer)
            except Exception:
                # not able to process the data? write it back to the stream_buffer
                ubwa.add_to_stream_buffer(oldest_stream_data_from_stream_buffer)


# start a worker to process the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(ubwa,)
)
worker_thread.start()
