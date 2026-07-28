#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_process_streams.py
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

import logging
import sys

try:
    from unicorn_fy.unicorn_fy import UnicornFy
except ImportError:
    print("Please install `unicorn-fy`! https://pypi.org/project/unicorn-fy/")
    sys.exit(1)


class BinanceWebSocketApiProcessStreams(object):
    @staticmethod
    def process_stream_data(received_stream_data_json, stream_buffer_name="False"):
        #
        #  START HERE!
        #
        # `received_stream_data_json` contains one record of raw data from the stream
        # print it and you see the data like its given from Binance, its hard to work with them, because keys of
        # parameters are changing from stream to stream and they are not self explaining.
        #
        # So if you want, you can use the class `UnicornFy`, it converts the json to a dict and prepares the values.
        # `depth5` for example doesnt include the symbol, but the unicornfied set includes them, because the class
        # extracts it from the channel name, makes it upper size and adds it to the returned values.. just print both
        # to see the difference.
        # Github: https://github.com/oliver-zehentleitner/unicorn-fy
        # PyPI: https://pypi.org/project/unicorn-fy/
        exchange = "binance.com"
        if exchange == "binance.com" or exchange == "binance.com-testnet":
            unicorn_fied_stream_data = UnicornFy.binance_com_websocket(
                received_stream_data_json
            )
        elif (
            exchange == "binance.com-futures"
            or exchange == "binance.com-futures-testnet"
        ):
            unicorn_fied_stream_data = UnicornFy.binance_com_futures_websocket(
                received_stream_data_json
            )
        elif (
            exchange == "binance.com-margin" or exchange == "binance.com-margin-testnet"
        ):
            unicorn_fied_stream_data = UnicornFy.binance_com_margin_websocket(
                received_stream_data_json
            )
        elif (
            exchange == "binance.com-isolated_margin"
            or exchange == "binance.com-isolated_margin-testnet"
        ):
            unicorn_fied_stream_data = UnicornFy.binance_com_margin_websocket(
                received_stream_data_json
            )
        elif exchange == "binance.je":
            unicorn_fied_stream_data = UnicornFy.binance_je_websocket(
                received_stream_data_json
            )
        elif exchange == "binance.us":
            unicorn_fied_stream_data = UnicornFy.binance_us_websocket(
                received_stream_data_json
            )
        else:
            logging.error("Not a valid exchange: " + str(exchange))

        # Now you can call different methods for different `channels`, here called `event_types`.
        # Its up to you if you call the methods in the bottom of this file or to call other classes which do what
        # ever you want to be done.
        try:
            if unicorn_fied_stream_data["event_type"] == "aggTrade":
                BinanceWebSocketApiProcessStreams.aggtrade(unicorn_fied_stream_data)
            elif unicorn_fied_stream_data["event_type"] == "trade":
                BinanceWebSocketApiProcessStreams.trade(unicorn_fied_stream_data)
            elif unicorn_fied_stream_data["event_type"] == "kline":
                BinanceWebSocketApiProcessStreams.kline(unicorn_fied_stream_data)
            elif unicorn_fied_stream_data["event_type"] == "24hrMiniTicker":
                BinanceWebSocketApiProcessStreams.miniticker(unicorn_fied_stream_data)
            elif unicorn_fied_stream_data["event_type"] == "24hrTicker":
                BinanceWebSocketApiProcessStreams.ticker(unicorn_fied_stream_data)
            elif unicorn_fied_stream_data["event_type"] == "depth":
                BinanceWebSocketApiProcessStreams.miniticker(unicorn_fied_stream_data)
            else:
                BinanceWebSocketApiProcessStreams.anything_else(
                    unicorn_fied_stream_data
                )
        except KeyError:
            BinanceWebSocketApiProcessStreams.anything_else(unicorn_fied_stream_data)
        except TypeError:
            pass

    @staticmethod
    def aggtrade(stream_data):
        # print `aggTrade` data
        print(stream_data)

    @staticmethod
    def trade(stream_data):
        # print `trade` data
        print(stream_data)

    @staticmethod
    def kline(stream_data):
        # print `kline` data
        print(stream_data)

    @staticmethod
    def miniticker(stream_data):
        # print `miniTicker` data
        print(stream_data)

    @staticmethod
    def ticker(stream_data):
        # print `ticker` data
        print(stream_data)

    @staticmethod
    def depth(stream_data):
        # print `depth` data
        print(stream_data)

    @staticmethod
    def outboundAccountInfo(stream_data):
        # print `outboundAccountInfo` data from userData stream
        print(stream_data)

    @staticmethod
    def executionReport(stream_data):
        # print `executionReport` data from userData stream
        print(stream_data)

    @staticmethod
    def anything_else(stream_data):
        print(stream_data)


if __name__ == "__main__":
    print("Dont run this script, its for imports only!")
