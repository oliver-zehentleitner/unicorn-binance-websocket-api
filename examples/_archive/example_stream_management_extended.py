#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_stream_management_extended.py
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

# import class to process stream data
from example_process_streams import BinanceWebSocketApiProcessStreams

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)

# create instance of BinanceWebSocketApiManager and provide the function for stream processing
binance_websocket_api_manager = BinanceWebSocketApiManager(
    BinanceWebSocketApiProcessStreams.process_stream_data
)

# define markets
markets = {"bnbbtc", "ethbtc", "btcusdt", "bchabcusdt", "eosusdt"}

markets_mega_list = {
    "xrpusdt",
    "rvnbtc",
    "ltcusdt",
    "adausdt",
    "eosusdt",
    "xmrusdt",
    "xmrbnb",
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
    "wanbnb",
    "zrxbnb",
    "agibnb",
    "funeth",
    "arketh",
    "engeth",
}

markets_mega_list.update(
    markets
)  # merge elements of set `markets` to set `markets_mega_list`

# define channels
channels = [
    "trade",
    "kline_1m",
    "kline_5m",
    "kline_15m",
    "kline_30m",
    "kline_1h",
    "kline_12h",
    "kline_1w",
    "miniTicker",
]

print(
    "############################################################################################################\r\n"
)

# create and start some streams
first_multi_stream_id = binance_websocket_api_manager.create_stream(
    channels, markets, stream_label="test1"
)
ticker_stream_id = binance_websocket_api_manager.create_stream(
    ["arr"], ["!miniTicker"], stream_label="ticker"
)
miniticker_stream_id = binance_websocket_api_manager.create_stream(["arr"], ["!ticker"])
time.sleep(4)

first_multi_stream_info = binance_websocket_api_manager.get_stream_info(
    first_multi_stream_id
)
print(
    binance_websocket_api_manager.get_stream_label(first_multi_stream_id),
    "received",
    first_multi_stream_info["processed_receives_total"],
    "records till now! ##\r\n",
)

# stop the streams
binance_websocket_api_manager.stop_stream(first_multi_stream_id)
binance_websocket_api_manager.stop_stream(miniticker_stream_id)
print(
    "############################################################################################################\r\n"
    f"# waiting, till stream {binance_websocket_api_manager.get_stream_label(first_multi_stream_id)} has stopped!\r\n"
    "############################################################################################################"
)
binance_websocket_api_manager.wait_till_stream_has_stopped(first_multi_stream_id)
print(
    "############################################################################################################\r\n"
    "#",
    binance_websocket_api_manager.get_stream_label(first_multi_stream_id),
    "stopped\r\n"
    "############################################################################################################",
)
first_multi_stream_runtime = time.time() - first_multi_stream_info["start_time"]
print(
    "############################################################################################################\r\n"
    "# stopping ticker stream with label",
    binance_websocket_api_manager.get_stream_label(ticker_stream_id),
    "(received",
    first_multi_stream_info["processed_receives_total"],
    "records in ",
    first_multi_stream_runtime,
    " seconds!)\r\n"
    "############################################################################################################",
)
binance_websocket_api_manager.stop_stream(ticker_stream_id)
if binance_websocket_api_manager.wait_till_stream_has_stopped(ticker_stream_id):
    print(
        "\r\n#### ticker stream with stream_label (",
        binance_websocket_api_manager.get_stream_label(ticker_stream_id),
        "has stopped! ####",
    )

# get stream infos
print("\r\n\r\ntrade_stream_info:")
stream_list = binance_websocket_api_manager.get_stream_list()
print(stream_list, "\r\n")
time.sleep(3)

# replace stream with an other one:
# first add or remove markets from the markets list
markets.add("neobtc")
markets.add("rvnbtc")
markets.remove("eosusdt")

# edit channels list
channels.remove("miniTicker")
channels.append("kline_2w")
channels.append("depth5")

# start the new multi stream
second_multi_stream_id = binance_websocket_api_manager.create_stream(channels, markets)

# get info about the new stream
second_multi_stream_info = binance_websocket_api_manager.get_stream_info(
    second_multi_stream_id
)
print(second_multi_stream_info)

# wait till second multi stream socket received its first data row
if binance_websocket_api_manager.wait_till_stream_has_started(second_multi_stream_id):
    # now close the first multi socket stream
    binance_websocket_api_manager.stop_stream(first_multi_stream_id)

time.sleep(3)
binance_websocket_api_manager.stop_stream(second_multi_stream_id)


# print stream list
print("\r\n\r\ntrade_stream_list:")
print(binance_websocket_api_manager.get_stream_list())

print("\r\ninfo first multi stream")
print(binance_websocket_api_manager.get_stream_info(first_multi_stream_id))

print("\r\ninfo ticker stream:")
print(binance_websocket_api_manager.get_stream_info(ticker_stream_id))

print("\r\ninfo miniTicker stream")
print(binance_websocket_api_manager.get_stream_info(miniticker_stream_id))

print("\r\ninfo second multi stream")
print(binance_websocket_api_manager.get_stream_info(second_multi_stream_id))

second_multi_stream_info = binance_websocket_api_manager.get_stream_info(
    second_multi_stream_id
)
print(
    "\r\n####",
    binance_websocket_api_manager.get_stream_label(second_multi_stream_id),
    "status",
    second_multi_stream_info["status"],
    "####\r\n",
)

while binance_websocket_api_manager.get_active_stream_list():
    print(binance_websocket_api_manager.get_active_stream_list())
    time.sleep(2)

second_multi_stream_info = binance_websocket_api_manager.get_stream_info(
    second_multi_stream_id
)
print(
    "\r\n####",
    binance_websocket_api_manager.get_stream_label(second_multi_stream_id),
    "status:",
    second_multi_stream_info["status"],
    "and received",
    second_multi_stream_info["processed_receives_total"],
    "records! ####\r\n",
)

print("\r\ntrade_stream_list:")
print(binance_websocket_api_manager.get_stream_list())


binance_websocket_api_manager.print_stream_info(first_multi_stream_id)
binance_websocket_api_manager.print_stream_info(ticker_stream_id)
binance_websocket_api_manager.print_stream_info(miniticker_stream_id)
binance_websocket_api_manager.print_stream_info(second_multi_stream_id)
binance_websocket_api_manager.print_summary()

print(
    "\r\n=============================== Stopping BinanceWebSocketManager ======================================\r\n"
)
binance_websocket_api_manager.stop_manager_with_all_streams()
print("finished!")
