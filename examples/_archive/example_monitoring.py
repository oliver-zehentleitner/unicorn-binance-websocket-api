#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_monitoring.py
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
from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
import logging
import os
import sys
import time
import threading

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
            sys.exit(0)
        oldest_stream_data_from_stream_buffer = (
            binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        )
        if oldest_stream_data_from_stream_buffer is None:
            time.sleep(0.01)
        else:
            try:
                # remove # to activate the print function:
                # print(oldest_stream_data_from_stream_buffer)
                pass
            except KeyError:
                # Any kind of error...
                # not able to process the data? write it back to the stream_buffer to pick it up later.
                binance_websocket_api_manager.add_to_stream_buffer(
                    oldest_stream_data_from_stream_buffer
                )


# create instance of BinanceWebSocketApiManager and provide the function for stream processing
ubwa = BinanceWebSocketApiManager()

# create streams
ticker_all_stream_id = ubwa.create_stream(["arr"], ["!ticker"])
miniticker_stream_id = ubwa.create_stream(["arr"], ["!miniTicker"])

markets = {
    "bnbbtc",
    "ethbtc",
    "btcusdt",
    "bchabcusdt",
    "xrpusdt",
    "rvnbtc",
    "ltcusdt",
    "adausdt",
    "eosusdt",
    "neousdt",
    "bnbusdt",
    "adabtc",
    "ethusdt",
    "trxbtc",
    "bchabcbtc",
    "ltcbtc",
    "xrpbtc",
    "ontbtc",
    "bttusdt",
    "eosbtc",
    "xlmbtc",
    "bttbtc",
    "tusdusdt",
    "xlmusdt",
    "qkcbtc",
    "zrxbtc",
    "neobtc",
    "adaeth",
    "icxusdt",
    "btctusd",
    "icxbtc",
    "btcusdc",
    "wanbtc",
    "zecbtc",
    "wtcbtc",
    "batbtc",
    "adabnb",
    "etcusdt",
    "qtumusdt",
    "xmrbtc",
    "trxeth",
    "adatusd",
    "trxxrp",
    "trxbnb",
    "dashbtc",
    "rvnbnb",
    "bchabctusd",
    "etcbtc",
    "bnbeth",
    "ethpax",
    "nanobtc",
    "xembtc",
    "xrpbnb",
    "bchabcpax",
    "xrpeth",
    "bttbnb",
    "ltcbnb",
    "agibtc",
    "zrxusdt",
    "xlmbnb",
    "ltceth",
    "eoseth",
    "ltctusd",
    "polybnb",
    "scbtc",
    "steembtc",
    "trxtusd",
    "npxseth",
    "kmdbtc",
    "polybtc",
    "gasbtc",
    "engbtc",
    "zileth",
    "xlmeth",
    "eosbnb",
    "xrppax",
    "lskbtc",
    "npxsbtc",
    "xmrusdt",
    "ltcpax",
    "ethtusd",
    "batusdt",
    "mcobtc",
    "neoeth",
    "bntbtc",
    "eostusd",
    "lrcbtc",
    "funbtc",
    "zecusdt",
    "bnbpax",
    "linkusdt",
    "hceth",
    "zrxeth",
    "icxeth",
    "xmreth",
    "neobnb",
    "etceth",
    "zeceth",
    "xmrbnb",
    "wanbnb",
    "zrxbnb",
    "agibnb",
    "funeth",
    "arketh",
    "engeth",
}

ubwa.create_stream(["aggTrade"], markets)
ubwa.create_stream(["trade"], markets)
ubwa.create_stream(["kline_1m"], markets)
ubwa.create_stream(["kline_5m"], markets)
ubwa.create_stream(["kline_15m"], markets)
ubwa.create_stream(["kline_1h"], markets)
ubwa.create_stream(["kline_12h"], markets)
ubwa.create_stream(["kline_1w"], markets)
ubwa.create_stream(["ticker"], markets)
ubwa.create_stream(["miniTicker"], markets)
ubwa.create_stream(["depth"], markets)
ubwa.create_stream(["depth5"], markets)
ubwa.create_stream(["depth10"], markets)
ubwa.create_stream(["depth20"], markets)
ubwa.create_stream(["aggTrade"], markets)

markets = {
    "bnbbtc",
    "ethbtc",
    "btcusdt",
    "bchabcusdt",
    "xrpusdt",
    "rvnbtc",
    "ltcusdt",
    "adausdt",
    "eosusdt",
    "neobtc",
    "adaeth",
    "icxusdt",
    "btctusd",
    "icxbtc",
    "btcusdc",
    "wanbtc",
    "zecbtc",
    "wtcbtc",
    "batbtc",
    "adabnb",
    "etcusdt",
    "qtumusdt",
    "xmrbtc",
    "trxeth",
    "adatusd",
    "trxxrp",
    "trxbnb",
    "ltctusd",
    "polybnb",
    "scbtc",
    "steembtc",
    "trxtusd",
    "npxseth",
    "kmdbtc",
    "polybtc",
    "gasbtc",
    "bnbpax",
    "linkusdt",
    "hceth",
    "zrxeth",
    "icxeth",
    "xmreth",
    "neobnb",
    "etceth",
    "zeceth",
    "xmrbnb",
}
channels = {
    "trade",
    "kline_1m",
    "kline_5m",
    "kline_15m",
    "kline_30m",
    "kline_1h",
    "kline_12h",
    "kline_1w",
    "miniTicker",
    "depth20",
}
ubwa.create_stream(channels, markets)

# start a worker process to process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(ubwa,)
)
worker_thread.start()

# start a restful api server to report the current status to 'tools/icinga/check_binance_websocket_manager' which can be
# used as a check_command for ICINGA/Nagios
# ubwa.start_monitoring_api(warn_on_update=False)
ubwa.start_monitoring_api()

# if you like to not only listen on localhost use 'host="0.0.0.0"'
# for a specific port do 'port=80'
# ubwa.start_monitoring_api(host="0.0.0.0", port=80)

print("18 websockets started!")
print(
    "Continue here: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/"
    "UNICORN-Monitoring-API-Service"
)
