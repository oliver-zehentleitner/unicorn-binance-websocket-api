#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: tools/find_max_websocket_uri_length.py
#
# Part of ‘UNICORN Binance WebSocket API’
# Project website: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Github: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Documentation: https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api
# PyPI: https://pypi.org/project/unicorn-binance-websocket-api
#
# License: MIT
# https://github.com/oliver-zehentleitner/unicorn-binance-rest-api/blob/master/LICENSE
#
# Author: Oliver Zehentleitner
#
# Copyright (c) 2019-2026, Oliver Zehentleitner (https://about.me/oliver-zehentleitner)
#
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
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import websocket
import time


def binance_test_uri_length(query):
    websocket_con = websocket.create_connection(
        "wss://stream.binance.com:9443/stream?streams=" + query
    )
    while True:
        result = websocket_con.recv()
        websocket_con.close()
        print("Received '%s'\r\n" % result)
        break


markets = [
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
    "xmrusdt",
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
]

channels = [
    "trade",
    "kline_1m",
    "kline_5m",
    "kline_15m",
    "kline_30m",
    "kline_1h",
    "kline_12h",
    "depth5",
    "depth10",
    "depth20",
]

streams = []
for market in markets:
    for channel in channels:
        streams.append(market + "@" + channel)

print("generated stream items:", len(streams), "\r\n")

query = ""
round = 0
# start_testing = int(input("start at round (try 484): "))
start_testing = 1013
if start_testing != "":
    for stream in streams:
        if round < len(streams):
            query += stream + "/"
            if round >= start_testing:
                print("round:", round)
                try:
                    binance_test_uri_length(query[:-1])
                except websocket._exceptions.WebSocketBadStatusException as error_msg:
                    print("error_msg:", error_msg)
                    print(
                        "uri length:",
                        str(
                            len(
                                "wss://stream.binance.com:9443/stream?streams="
                                + query[:-1]
                            )
                        ),
                    )
                    print("query:")
                    print(query[:-1])
                    if "Large" in str(error_msg):
                        print("\r\nfound too long URI!")
                        break
                time.sleep(11)
            round += 1

print("\r\nstart shorting the URI:")
while True:
    shorted_query = query[:-1]
    try:
        binance_test_uri_length(shorted_query)
        print("\r\nfound first valid URI length before URI too long error")
        print(
            "longest valid URI length:",
            str(len("wss://stream.binance.com:9443/stream?streams=" + shorted_query)),
        )
        print("longest valid URI string:")
        print("wss://stream.binance.com:9443/stream?streams=" + shorted_query)
        exit(0)
    except websocket._exceptions.WebSocketBadStatusException as error_msg:
        pass

    time.sleep(11)
