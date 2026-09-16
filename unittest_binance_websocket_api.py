#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: unittest_binance_websocket_api.py
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

from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
from unicorn_binance_websocket_api.exceptions import *
from unicorn_binance_websocket_api.restclient import BinanceWebSocketApiRestclient
from unicorn_binance_rest_api import BinanceRestApiManager
import asyncio
import logging
import orjson
import unittest
import os
import platform
import time
import threading
import socket
from unittest.mock import Mock, patch

import tracemalloc

tracemalloc.start(25)

BINANCE_COM_API_KEY = ""
BINANCE_COM_API_SECRET = ""

BINANCE_COM_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY")
BINANCE_COM_TESTNET_API_SECRET = os.getenv("BINANCE_TESTNET_API_SECRET")

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)

print(f"Starting unittests!")


async def processing_of_new_data_async(data):
    print(f"`processing_of_new_data_async()` test - Received: {data}")
    await asyncio.sleep(0.001)
    print("AsyncIO Check done!")


def handle_socket_message(data):
    print(f"Received ws api data:\r\n{data}\r\n")


def processing_of_new_data(data):
    print(f"`processing_of_new_data()` test - Received: {data}")


class TestBinanceComManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestBinanceComManager:")
        cls.ubwa = BinanceWebSocketApiManager(
            exchange="binance.us", disable_colorama=True, debug=True
        )
        cls.binance_com_api_key = ""
        cls.binance_com_api_secret = ""

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_manager()
        print(f"\r\nTestBinanceComManager threads:")
        for thread in threading.enumerate():
            print(thread.name)
        print(f"TestBinanceComManager stopping:")

    def test_create_uri_miniticker_regular_com(self):
        print(f"test_create_uri_miniticker_regular_com():")
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["!miniTicker"], ["arr"]),
            "wss://stream.binance.us:9443/ws/!miniTicker@arr",
        )

    def test_create_uri_miniticker_reverse_com(self):
        print(f"test_create_uri_miniticker_reverse_com():")
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["arr"], ["!miniTicker"]),
            "wss://stream.binance.us:9443/ws/!miniTicker@arr",
        )

    def test_create_uri_ticker_regular_com(self):
        print(f"test_create_uri_ticker_regular_com():")
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["!ticker"], ["arr"]),
            "wss://stream.binance.us:9443/ws/!ticker@arr",
        )

    def test_create_uri_ticker_reverse_com(self):
        print(f"test_create_uri_ticker_reverse_com():")
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["arr"], ["!ticker"]),
            "wss://stream.binance.us:9443/ws/!ticker@arr",
        )

    def test_create_uri_userdata_regular_false_com(self):
        print(f"test_create_uri_userdata_regular_false_com():")
        self.assertFalse(
            self.__class__.ubwa.create_websocket_uri(["!userData"], ["arr"])
        )

    def test_create_uri_userdata_reverse_false_com(self):
        print(f"test_create_uri_userdata_reverse_false_com():")
        self.assertFalse(
            self.__class__.ubwa.create_websocket_uri(["arr"], ["!userData"])
        )

    def test_create_uri_userdata_regular_com(self):
        print(f"test_create_uri_userdata_regular_com():")
        if len(self.binance_com_api_key) == 0 or len(self.binance_com_api_secret) == 0:
            print(
                "\r\nempty API key and/or secret: can not successfully test test_create_uri_userdata_regular_com() "
                "for binance.com"
            )
        else:
            stream_id = self.ubwa.get_new_uuid_id()
            self.__class__.ubwa._add_stream_to_stream_list(
                stream_id, ["!userData"], ["arr"]
            )
            self.assertRegex(
                self.__class__.ubwa.create_websocket_uri(
                    ["!userData"],
                    ["arr"],
                    stream_id,
                    self.binance_com_api_key,
                    self.binance_com_api_secret,
                ),
                r"wss://stream.binance.com:9443/ws/.",
            )

    def test_create_uri_userdata_reverse_com(self):
        print(f"test_create_uri_userdata_reverse_com():")
        if len(self.binance_com_api_key) == 0 or len(self.binance_com_api_secret) == 0:
            print(
                "\r\nempty API key and/or secret: can not successfully test test_create_uri_userdata_reverse_com() "
                "for binance.com"
            )
        else:
            self.stream_id = self.__class__.ubwa.get_new_uuid_id()
            self.__class__.ubwa._add_stream_to_stream_list(
                self.stream_id, ["arr"], ["!userData"]
            )
            self.assertRegex(
                self.__class__.ubwa.create_websocket_uri(
                    ["arr"],
                    ["!userData"],
                    self.stream_id,
                    self.binance_com_api_key,
                    self.binance_com_api_secret,
                ),
                "wss://stream.binance.com:9443/ws/.",
            )

    def test_is_exchange_type_cex(self):
        print(f"test_is_exchange_type_cex():")
        self.assertEqual(self.__class__.ubwa.is_exchange_type("cex"), True)

    def test_is_exchange_type_dex(self):
        print(f"test_is_exchange_type_dex():")
        self.assertEqual(self.__class__.ubwa.is_exchange_type("dex"), False)

    def test_is_update_available(self):
        print(f"test_is_update_available():")
        result = self.__class__.ubwa.is_update_available()
        is_valid_result = result is True or result is False
        self.assertTrue(is_valid_result, False)

    def test_is_manager_stopping(self):
        print(f"test_is_manager_stopping():")
        self.assertEqual(self.__class__.ubwa.is_manager_stopping(), False)

    def test_get_human_uptime(self):
        print(f"test_get_human_uptime():")
        self.assertEqual(
            self.__class__.ubwa.get_human_uptime(60 * 60 * 60 * 61), "152d:12h:0m:0s"
        )
        self.assertEqual(
            self.__class__.ubwa.get_human_uptime(60 * 60 * 24), "24h:0m:0s"
        )
        self.assertEqual(self.__class__.ubwa.get_human_uptime(60 * 60), "60m:0s")
        self.assertEqual(self.__class__.ubwa.get_human_uptime(60), "60 seconds")

    def test_get_human_bytesize(self):
        print(f"test_get_human_bytesize():")
        self.assertEqual(
            self.__class__.ubwa.get_human_bytesize(1024 * 1024 * 1024 * 1024 * 1024),
            "1024.0 tB",
        )
        self.assertEqual(
            self.__class__.ubwa.get_human_bytesize(1024 * 1024 * 1024 * 1024),
            "1024.0 gB",
        )
        self.assertEqual(
            self.__class__.ubwa.get_human_bytesize(1024 * 1024 * 1024), "1024.0 mB"
        )
        self.assertEqual(
            self.__class__.ubwa.get_human_bytesize(1024 * 1024), "1024.0 kB"
        )
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1024), "1024 B")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1), "1 B")

    def test_get_exchange(self):
        print(f"test_get_exchange():")
        self.assertEqual(self.__class__.ubwa.get_exchange(), "binance.us")

    def test_get_listenkey_from_restclient(self):
        print(f"test_get_listenkey_from_restclient():")
        self.assertEqual(
            self.__class__.ubwa.get_listen_key_from_restclient("ID"), False
        )

    def test_delete_listen_key_by_stream_id(self):
        print(f"test_delete_listen_key_by_stream_id():")
        stream_id = self.__class__.ubwa.get_new_uuid_id()
        self.assertEqual(
            self.__class__.ubwa.delete_listen_key_by_stream_id(stream_id), False
        )

    def test_create_payload_subscribe(self):
        print(f"test_create_payload_subscribe():")
        result = "[{'method': 'SUBSCRIBE', 'params': ['bnbbtc@kline_1m'], 'id': 1}]"
        stream_id = self.__class__.ubwa.get_new_uuid_id()
        self.assertEqual(
            str(
                self.__class__.ubwa.create_payload(
                    stream_id, "subscribe", ["kline_1m"], ["bnbbtc"]
                )
            ),
            result,
        )

    def test_split_payload_exact_batch_multiple(self):
        print(f"test_split_payload_exact_batch_multiple():")
        # Regression test for issue #374: split_payload() returned None when len(params) was an
        # exact multiple of 351 (max_items_per_request + 1), because all batches were flushed
        # inside the loop and add_params was empty afterwards.
        params_351 = [f"market{i}@trade" for i in range(351)]
        result = self.__class__.ubwa.split_payload(params_351, "SUBSCRIBE")
        self.assertIsNotNone(
            result, "split_payload returned None for 351 params (1x batch boundary)"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]["params"]), 351)

        params_702 = [f"market{i}@trade" for i in range(702)]
        result = self.__class__.ubwa.split_payload(params_702, "SUBSCRIBE")
        self.assertIsNotNone(
            result, "split_payload returned None for 702 params (2x batch boundary)"
        )
        self.assertEqual(len(result), 2)

    def test_stream_is_restarting_clears_payload_and_resubscribes(self):
        print(f"test_stream_is_restarting_clears_payload_and_resubscribes():")
        ubwa = self.__class__.ubwa
        stream_id = ubwa.get_new_uuid_id()
        # Inject a minimal stream_list entry for a regular CEX market-data stream
        with ubwa.stream_list_lock:
            ubwa.stream_list[stream_id] = {
                "status": "running",
                "payload": [
                    {"method": "SUBSCRIBE", "params": ["stale@trade"], "id": 99}
                ],
                "api": False,
                "channels": ["trade"],
                "markets": ["bnbbtc", "ethbtc"],
                "subscriptions": 2,
            }
        ubwa._stream_is_restarting(stream_id=stream_id)
        # Stale payload must be gone, replaced by fresh re-subscribe
        payload = ubwa.stream_list[stream_id]["payload"]
        self.assertIsNotNone(payload)
        self.assertGreater(
            len(payload), 0, "Re-subscribe payload must be queued on reconnect"
        )
        all_params = [p for batch in payload for p in batch["params"]]
        self.assertIn("bnbbtc@trade", all_params)
        self.assertIn("ethbtc@trade", all_params)
        self.assertNotIn(
            "stale@trade",
            all_params,
            "Stale payload must be cleared before re-subscribe",
        )
        del ubwa.stream_list[stream_id]

    def test_stream_is_restarting_skips_resubscribe_for_userdata(self):
        print(f"test_stream_is_restarting_skips_resubscribe_for_userdata():")
        ubwa = self.__class__.ubwa
        stream_id = ubwa.get_new_uuid_id()
        with ubwa.stream_list_lock:
            ubwa.stream_list[stream_id] = {
                "status": "running",
                "payload": [
                    {"method": "SUBSCRIBE", "params": ["stale@trade"], "id": 99}
                ],
                "api": False,
                "channels": ["!userData"],
                "markets": ["arr"],
                "subscriptions": 0,
            }
        ubwa._stream_is_restarting(stream_id=stream_id)
        # UserData streams use listen-key URI — payload must be cleared and stay empty
        self.assertEqual(
            ubwa.stream_list[stream_id]["payload"],
            [],
            "UserData streams must not get a re-subscribe payload",
        )
        del ubwa.stream_list[stream_id]

    def test_stream_is_restarting_skips_resubscribe_for_api_streams(self):
        print(f"test_stream_is_restarting_skips_resubscribe_for_api_streams():")
        ubwa = self.__class__.ubwa
        stream_id = ubwa.get_new_uuid_id()
        with ubwa.stream_list_lock:
            ubwa.stream_list[stream_id] = {
                "status": "running",
                "payload": [
                    {"method": "SUBSCRIBE", "params": ["stale@trade"], "id": 99}
                ],
                "api": True,
                "channels": ["trade"],
                "markets": ["bnbbtc"],
                "subscriptions": 0,
            }
        ubwa._stream_is_restarting(stream_id=stream_id)
        # WebSocket-API streams must not get a SUBSCRIBE re-queue
        self.assertEqual(
            ubwa.stream_list[stream_id]["payload"],
            [],
            "WebSocket-API streams must not get a re-subscribe payload",
        )
        del ubwa.stream_list[stream_id]

    def test_fill_up_space_centered(self):
        print(f"test_fill_up_space_centered():")
        result = "==========test text=========="
        self.assertEqual(
            str(self.__class__.ubwa.fill_up_space_centered(30, "test text", "=")),
            result,
        )

    def test_fill_up_space_right(self):
        print(f"test_fill_up_space_right():")
        result = "|test text||||||||||||||||||||"
        self.assertEqual(
            str(self.__class__.ubwa.fill_up_space_right(30, "test text", "|")), result
        )

    def test_fill_up_space_left(self):
        print(f"test_fill_up_space_left():")
        result = "||||||||||||||||||||test text|"
        self.assertEqual(
            str(self.__class__.ubwa.fill_up_space_left(30, "test text", "|")), result
        )

    def test_create_stream_userdata_with(self):
        print(f"test_create_stream_userdata_with():")
        with BinanceWebSocketApiManager(exchange="binance.us") as ubwa:
            ubwa.create_stream("arr", "!userData", stream_label="userDataBad")
            time.sleep(10)
        print(f"Leaving ... ")

    def test_create_stream(self):
        print(f"test_create_stream():")
        self.assertTrue(
            bool(
                self.__class__.ubwa.create_stream(
                    markets=["bnbbtc"], channels="trade", stream_label="test_stream"
                )
            )
        )
        stream_id = self.__class__.ubwa.get_stream_id_by_label("test_stream")
        time.sleep(5)
        self.__class__.ubwa.unsubscribe_from_stream(stream_id, markets=["bnbbtc"])
        self.__class__.ubwa.unsubscribe_from_stream(stream_id, channels=["trade"])
        time.sleep(6)
        self.__class__.ubwa.print_summary(title="Unittests")
        self.__class__.ubwa.print_stream_info(stream_id, title="Unittests")


class TestBinanceComManagerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestBinanceComManagerTest:")
        cls.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com-testnet", debug=True
        )
        cls.binance_com_testnet_api_key = BINANCE_COM_TESTNET_API_KEY
        cls.binance_com_testnet_api_secret = BINANCE_COM_TESTNET_API_SECRET

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_manager()
        print(f"\r\nTestBinanceComManagerTest threads:")
        for thread in threading.enumerate():
            print(thread.name)
        print(f"TestBinanceComManagerTest stopping:")

    def test_create_uri_miniticker_regular_com(self):
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["!miniTicker"], ["arr"]),
            "wss://stream.testnet.binance.vision/ws/!miniTicker@arr",
        )

    def test_create_uri_miniticker_reverse_com(self):
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["arr"], ["!miniTicker"]),
            "wss://stream.testnet.binance.vision/ws/!miniTicker@arr",
        )

    def test_create_uri_ticker_regular_com(self):
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["!ticker"], ["arr"]),
            "wss://stream.testnet.binance.vision/ws/!ticker@arr",
        )

    def test_create_uri_ticker_reverse_com(self):
        self.assertEqual(
            self.__class__.ubwa.create_websocket_uri(["arr"], ["!ticker"]),
            "wss://stream.testnet.binance.vision/ws/!ticker@arr",
        )

    def test_create_uri_userdata_regular_false_com(self):
        self.assertFalse(
            self.__class__.ubwa.create_websocket_uri(["!userData"], ["arr"])
        )

    def test_create_uri_userdata_reverse_false_com(self):
        self.assertFalse(
            self.__class__.ubwa.create_websocket_uri(["arr"], ["!userData"])
        )

    def test_create_uri_userdata_regular_com(self):
        if (
            BINANCE_COM_TESTNET_API_KEY is not None
            and BINANCE_COM_TESTNET_API_SECRET is not None
        ):
            print(
                "\r\nempty API key and/or secret: can not successfully test test_create_uri_userdata_regular_com() "
                "for binance.com-testnet"
            )
        else:
            stream_id = self.__class__.ubwa.get_new_uuid_id()
            self.__class__.ubwa._add_stream_to_stream_list(
                stream_id, ["!userData"], ["arr"]
            )
            self.assertRegex(
                self.__class__.ubwa.create_websocket_uri(
                    ["!userData"],
                    ["arr"],
                    stream_id,
                    self.__class__.binance_com_testnet_api_key,
                    self.__class__.binance_com_testnet_api_secret,
                ),
                r"wss://ws-api.testnet.binance.vision/ws-api/v3",
            )

    def test_create_uri_userdata_reverse_com(self):
        if (
            BINANCE_COM_TESTNET_API_KEY is not None
            and BINANCE_COM_TESTNET_API_SECRET is not None
        ):
            print(
                "\r\nempty API key and/or secret: can not successfully test test_create_uri_userdata_reverse_com() "
                "for binance.com-testnet"
            )
        else:
            stream_id = self.__class__.ubwa.get_new_uuid_id()
            self.__class__.ubwa._add_stream_to_stream_list(
                stream_id, ["arr"], ["!userData"]
            )
            self.assertRegex(
                self.__class__.ubwa.create_websocket_uri(
                    ["arr"],
                    ["!userData"],
                    stream_id,
                    self.binance_com_testnet_api_key,
                    self.binance_com_testnet_api_secret,
                ),
                r"wss://ws-api.testnet.binance.vision/ws-api/v3",
            )

    def test_is_exchange_type_cex(self):
        self.assertEqual(self.__class__.ubwa.is_exchange_type("cex"), True)

    def test_float_to_str_no_scientific_notation(self):
        print(f"test_float_to_str_no_scientific_notation():")
        from decimal import Decimal

        test_cases = [
            (1.9e-07, "0.00000019"),
            (1.9e-05, "0.000019"),
            (1.9e-06, "0.0000019"),
            (23416.1, "23416.1"),
            (0.00001, "0.00001"),
            (100.0, "100.0"),
        ]
        for value, expected in test_cases:
            result = format(Decimal(repr(value)), "f")
            self.assertEqual(
                result,
                expected,
                f"Failed for {value!r}: got {result!r}, expected {expected!r}",
            )
            self.assertNotIn(
                "e",
                result.lower(),
                f"Scientific notation found in {result!r} for input {value!r}",
            )

    def test_stop_manager_delete_listen_key_false(self):
        print(f"test_stop_manager_delete_listen_key_false():")
        with BinanceWebSocketApiManager(
            exchange="binance.com-testnet", debug=True
        ) as ubwa:
            self.assertTrue(ubwa.stop_manager(delete_listen_key=False))

    def test_stop_manager_with_all_streams_delete_listen_key_false(self):
        print(f"test_stop_manager_with_all_streams_delete_listen_key_false():")
        with BinanceWebSocketApiManager(
            exchange="binance.com-testnet", debug=True
        ) as ubwa:
            self.assertTrue(ubwa.stop_manager_with_all_streams(delete_listen_key=False))

    def test_z_stop_manager(self):
        time.sleep(6)
        self.__class__.ubwa.stop_manager()

    class TestWSApiLive(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            print(f"\r\nTestWSApiLive:")

        @classmethod
        def tearDownClass(cls):
            print(f"\r\nTestWSApiLive threads:")
            for thread in threading.enumerate():
                print(thread.name)
            print(f"TestApiLive stopping:")

    def test_live_api_ws(self):
        print(f"Test Websocket API ...")
        market = "BUSDUSDT"
        ubwam = BinanceWebSocketApiManager(exchange="binance.com-testnet")
        api_stream = ubwam.create_stream(
            api=True,
            api_key=BINANCE_COM_TESTNET_API_KEY,
            api_secret=BINANCE_COM_TESTNET_API_SECRET,
            stream_label="Bobs Websocket API",
            process_stream_data=handle_socket_message,
        )
        time.sleep(5)
        current_average_price = ubwam.api.spot.get_current_average_price(
            stream_id=api_stream, symbol=market, return_response=True
        )
        print(f"current_average_price: {current_average_price}\r\n")
        order_book = ubwam.api.spot.get_order_book(
            stream_id=api_stream, symbol=market, limit=10, return_response=True
        )
        if type(order_book) is not bool:
            print(
                f"Orderbook, lastUpdateId={order_book['result']['lastUpdateId']}: {order_book['result']['asks']}, "
                f"{order_book['result']['bids']}\r\n"
            )
        aggregate_trades = ubwam.api.spot.get_aggregate_trades(
            stream_id=api_stream, symbol=market, return_response=True
        )
        if type(aggregate_trades) is not bool:
            print(f"aggregate_trades: {aggregate_trades['result'][:5]}\r\n")
        historical_trades = ubwam.api.spot.get_historical_trades(
            stream_id=api_stream, symbol=market, return_response=True
        )
        if type(historical_trades) is not bool:
            print(f"historical_trades: {historical_trades['result'][:5]}\r\n")
        recent_trades = ubwam.api.spot.get_recent_trades(
            stream_id=api_stream, symbol=market, return_response=True
        )
        if type(recent_trades) is not bool:
            print(f"recent_trades: {recent_trades['result'][:5]}\r\n")
        klines = ubwam.api.spot.get_klines(
            stream_id=api_stream, symbol=market, interval="1m", return_response=True
        )
        if type(klines) is not bool:
            print(f"A few klines: {klines['result'][:5]}\r\n")
        ui_klines = ubwam.api.spot.get_ui_klines(
            stream_id=api_stream, symbol=market, interval="1d", return_response=True
        )
        if type(ui_klines) is not bool:
            print(f"A few ui_klines: {ui_klines['result'][:5]}\r\n")
        ubwam.api.spot.get_listen_key(stream_id=api_stream)
        ubwam.api.spot.get_server_time(stream_id=api_stream)
        ubwam.api.spot.get_account_status(stream_id=api_stream)
        orig_client_order_id = ubwam.api.spot.create_order(
            stream_id=api_stream,
            price=1.0,
            order_type="LIMIT",
            quantity=15.0,
            side="SELL",
            symbol="BUSDUSDT",
        )
        ubwam.api.spot.create_test_order(
            stream_id=api_stream,
            price=1.2,
            order_type="LIMIT",
            quantity=12.0,
            side="SELL",
            symbol="BUSDUSDT",
        )
        ubwam.api.spot.ping(stream_id=api_stream)
        ubwam.api.spot.get_exchange_info(stream_id=api_stream, symbols=["BUSDUSDT"])
        ubwam.api.spot.get_order_book(stream_id=api_stream, symbol="BUSDUSDT", limit=2)
        replaced_client_order_id = ubwam.api.spot.cancel_and_replace_order(
            stream_id=api_stream,
            price=1.1,
            order_type="LIMIT",
            quantity=15.0,
            side="SELL",
            symbol=market,
            cancel_orig_client_order_id=orig_client_order_id,
        )
        ubwam.api.spot.cancel_order(
            stream_id=api_stream,
            symbol="BUSDUSDT",
            orig_client_order_id=replaced_client_order_id,
        )
        ubwam.api.spot.get_open_orders(stream_id=api_stream, symbol="BUSDUSDT")
        ubwam.api.spot.get_open_orders(stream_id=api_stream)
        ubwam.api.spot.cancel_open_orders(stream_id=api_stream, symbol="BUSDUSDT")
        ubwam.api.spot.get_order(
            stream_id=api_stream,
            symbol="BUSDUSDT",
            orig_client_order_id=replaced_client_order_id,
        )
        time.sleep(5)
        ubwam.stop_manager()


class TestApiLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestApiLive:")
        cls.ubwa = BinanceWebSocketApiManager(
            exchange="binance.us",
            debug=True,
            enable_stream_signal_buffer=True,
            auto_data_cleanup_stopped_streams=True,
        )
        cls.count_receives = 0

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_manager()
        print(f"\r\nTestApiLive threads:")
        for thread in threading.enumerate():
            print(thread.name)
        print(f"TestApiLive stopping:")

    def test_get_new_uuid_id(self):
        self.__class__.ubwa.get_new_uuid_id()

    def test_rest_binance_com(self):
        BinanceWebSocketApiRestclient(self.__class__.ubwa)

    def test_z_rest_binance_com_isolated_margin(self):
        ubwa = BinanceWebSocketApiManager(exchange="binance.com-isolated_margin")
        BinanceWebSocketApiRestclient(ubwa)
        ubwa.stop_manager()

    def test_z_rest_binance_com_isolated_margin_testnet(self):
        ubwa = BinanceWebSocketApiManager(
            exchange="binance.com-isolated_margin-testnet"
        )
        BinanceWebSocketApiRestclient(ubwa)
        ubwa.stop_manager()

    def test_z_invalid_exchange(self):
        with self.assertRaises(UnknownExchange):
            ubwa_error = BinanceWebSocketApiManager(exchange="invalid-exchange.com")
            ubwa_error.stop_manager()

    # Todo: Needs a proxy ...
    #    def test_isolated_margin(self):
    #        self.__class__.ubwa = BinanceWebSocketApiManager(exchange="binance.com-isolated_margin")
    #        stream_id = self.__class__.ubwa.create_stream('arr', '!userData', symbols="CELRBTC",
    #                                                      api_key="key", api_secret="secret")
    #        time.sleep(10)
    #        print("\r\n")
    #        self.__class__.ubwa.print_stream_info(stream_id)
    #        self.__class__.ubwa.stop_manager()

    def test_live_receives_stream_specific_with_stream_buffer(self):
        print(f"Test receiving with stream specific stream_buffer ...")
        stream_id = self.__class__.ubwa.create_stream(
            ["arr"], ["!miniTicker"], stream_buffer_name=True
        )
        count_receives = 0
        while count_receives < 5:
            received = self.__class__.ubwa.pop_stream_data_from_stream_buffer(stream_id)
            if received:
                print(f"Received: {received}")
                count_receives += 1
        self.assertEqual(count_receives, 5)

    def test_live_receives_asyncio_queue(self):
        async def process_asyncio_queue(stream_id=None):
            print(f"Start processing data of {stream_id} from asyncio_queue...")
            self.count_receives = 0
            while self.count_receives < 5:
                data = await self.__class__.ubwa.get_stream_data_from_asyncio_queue(
                    stream_id
                )
                print(f"Received async: {data}")
                self.count_receives += 1
                self.__class__.ubwa.asyncio_queue_task_done(stream_id)
            print(f"Closing asyncio_queue consumer!")

        print(f"Test receiving with stream specific asyncio_queue ...")
        stream_id_1 = self.__class__.ubwa.create_stream(
            ["arr"], ["!miniTicker"], process_asyncio_queue=process_asyncio_queue
        )
        while self.count_receives < 5:
            time.sleep(1)
        self.assertEqual(self.count_receives, 5)
        time.sleep(3)
        self.__class__.ubwa.stop_stream(stream_id=stream_id_1)

    def test_exception_streamisstopping(self):
        with self.assertRaises(StreamIsStopping):
            raise StreamIsStopping(stream_id="blah", reason="test")

    def test_exception_streamiscrashing(self):
        with self.assertRaises(StreamIsCrashing):
            raise StreamIsCrashing(stream_id="blah", reason="test")

    def test_exception_streamisrestarting(self):
        with self.assertRaises(StreamIsRestarting):
            raise StreamIsRestarting(stream_id="blah", reason="test")

    def test_exception_maximumsubscriptionsexceeded(self):
        with self.assertRaises(MaximumSubscriptionsExceeded):
            raise MaximumSubscriptionsExceeded(
                exchange="binance.com", max_subscriptions_per_stream=1024
            )

    def test_live_run(self):
        self.__class__.ubwa.get_active_stream_list()
        self.__class__.ubwa.get_limit_of_subscriptions_per_stream()
        self.__class__.ubwa.get_stream_list()

        markets = [
            "xrpbearbusd",
            "zeceth",
            "cndbtc",
            "dashbtc",
            "atompax",
            "perlbtc",
            "ardreth",
            "zecbnb",
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
            "tusdusdt",
            "atombusd",
            "nxseth",
            "bnbusdt",
            "trxxrp",
            "erdpax",
            "erdbtc",
            "icxbusd",
            "nulsbtc",
            "wavespax",
            "zilbnb",
            "arnbtc",
            "nulsusdt",
            "wintrx",
            "npxsbtc",
            "busdtry",
            "qtumbnb",
            "eosbtc",
            "tomobnb",
            "eosbnb",
            "engbtc",
            "linketh",
            "xrpbtc",
            "fetbtc",
            "stratusdt",
            "navbnb",
            "bcneth",
            "nanobnb",
            "saltbtc",
            "tfuelusdc",
            "skybnb",
            "fuelbtc",
            "bnbusdc",
            "inseth",
            "btcpax",
            "batbtc",
            "arketh",
            "ltcpax",
            "ltcbusd",
            "duskbtc",
            "mftusdt",
            "bntusdt",
            "mdabtc",
            "enjbtc",
            "poabnb",
            "paxtusd",
            "hotbtc",
            "bcdbtc",
            "beambnb",
            "trxeth",
            "omgbnb",
            "cdtbtc",
            "eosusdc",
            "dashbusd",
            "dasheth",
            "xrptusd",
            "atomtusd",
            "rcneth",
            "rpxeth",
            "xlmusdc",
            "aionbusd",
            "nxsbtc",
            "chateth",
            "tctusdt",
            "linkusdt",
            "nasbtc",
            "usdsusdc",
            "xvgbtc",
            "elfeth",
            "ctxcbtc",
            "cmteth",
            "gnteth",
            "zilbtc",
            "batpax",
            "stratbtc",
            "xzcbtc",
            "iotausdt",
            "etcbnb",
            "ankrusdt",
            "xlmeth",
            "loombtc",
            "rdnbnb",
            "icneth",
            "vetbtc",
            "cvcusdt",
            "ftmpax",
            "ethbullusdt",
            "edoeth",
            "steemeth",
            "gobnb",
            "ambbtc",
            "bchabcbtc",
            "dntbtc",
            "btctusd",
            "denteth",
            "snglsbtc",
            "eosbullusdt",
            "xlmtusd",
            "sysbnb",
            "renusdt",
            "zrxusdt",
            "xlmbtc",
            "stormbtc",
            "ncashbnb",
            "omgusdt",
            "troyusdt",
            "venbtc",
            "dogepax",
            "ontusdc",
            "eurbusd",
            "tctbnb",
            "gxsbtc",
            "celrbnb",
            "adausdt",
            "beambtc",
            "elfbtc",
            "rvnusdt",
            "poaeth",
            "wavesusdc",
            "trxbnb",
            "trxusdc",
            "ethbearusdt",
            "ethpax",
            "bateth",
            "kavabtc",
            "paxbtc",
            "trigbnb",
            "btcusdc",
            "oneusdc",
            "xrptry",
            "stxusdt",
            "strateth",
            "lendeth",
            "neousdc",
            "mithusdt",
            "btcngn",
            "blzeth",
            "evxeth",
            "dnteth",
            "grsbtc",
            "arneth",
            "iotabnb",
            "waneth",
            "subeth",
            "btsbtc",
            "cvceth",
            "ethusdc",
            "etctusd",
            "cloakbtc",
            "grseth",
            "eospax",
            "cdteth",
            "lskusdt",
            "enjbusd",
            "drepbtc",
            "manaeth",
            "tomousdt",
            "algobnb",
            "wtceth",
            "linkpax",
            "batbnb",
            "rvnbusd",
            "cvcbnb",
            "manabtc",
            "gasbtc",
            "stxbtc",
            "cloaketh",
            "neotusd",
            "lrceth",
            "thetabtc",
            "aionbnb",
            "viabtc",
            "keyeth",
            "nanoeth",
            "ncasheth",
            "bgbpusdc",
            "ltobnb",
            "snmeth",
            "adabtc",
            "qtumbusd",
            "wtcbnb",
            "dcrbtc",
            "fttbnb",
            "paxbnb",
            "insbtc",
            "gntbnb",
            "etheur",
            "dashusdt",
            "btcusdt",
            "wanusdt",
            "powrbnb",
            "xmrbnb",
            "trigeth",
            "xzceth",
            "bchbtc",
            "qspbnb",
            "scbnb",
            "powrbtc",
            "algotusd",
            "ankrbtc",
            "tusdeth",
            "keybtc",
            "usdcusdt",
            "ftmusdc",
            "atombnb",
            "zenbtc",
            "neobtc",
            "phbbnb",
            "bnbpax",
            "brdbnb",
            "trxusdt",
            "trxbusd",
            "mtlbtc",
            "ftmtusd",
            "perlusdc",
            "eosbullbusd",
            "reqeth",
            "bccbnb",
            "veneth",
            "loombnb",
            "trxpax",
            "usdcpax",
            "stormusdt",
            "ognbtc",
            "iotaeth",
            "naseth",
            "drepusdt",
            "gvteth",
            "wrxusdt",
            "bchabcpax",
            "ongbtc",
            "usdcbnb",
            "dgdeth",
            "mtleth",
            "bcnbnb",
            "neblbnb",
            "wanbnb",
            "ontusdt",
            "npxsusdt",
            "mftbtc",
            "eosbearbusd",
            "bntbtc",
            "modeth",
            "etcusdc",
            "veteth",
            "bcptpax",
            "atomusdc",
            "duskpax",
            "kavabnb",
            "lunbtc",
            "adxbtc",
            "funbtc",
            "knceth",
            "dogebtc",
            "bchsvpax",
            "bcpttusd",
            "osteth",
            "oaxeth",
            "wabibtc",
            "appcbtc",
            "nanousdt",
            "wingsbtc",
            "hbarusdt",
            "eurusdt",
            "waveseth",
            "asteth",
            "linkbusd",
            "btttusd",
            "bnbusds",
            "linkbtc",
            "venusdt",
            "hotbnb",
            "usdtrub",
            "tctbtc",
            "ankrpax",
            "btctry",
            "adabnb",
            "bcceth",
            "enjeth",
            "bnbbusd",
            "repbnb",
            "bullusdt",
            "vitebtc",
            "btgbtc",
            "renbtc",
            "thetausdt",
            "dentbtc",
            "ostbtc",
            "nxsbnb",
            "mithbtc",
            "xmrbtc",
            "tomobtc",
            "nulseth",
            "phbbtc",
            "duskbnb",
            "ontbusd",
            "btgeth",
            "etcusdt",
            "atomusdt",
            "hcbtc",
            "brdbtc",
            "fttbtc",
            "celrusdt",
            "lskbnb",
            "xtzbtc",
            "batusdt",
            "viteusdt",
            "trxbtc",
            "bchtusd",
            "xtzusdt",
            "ftmbtc",
            "enjbnb",
            "arkbtc",
            "ftmusdt",
            "neobusd",
            "stormbnb",
            "luneth",
            "gntbtc",
            "gtousdt",
            "chzusdt",
            "sntbtc",
            "bandbnb",
            "wingseth",
            "mcobtc",
            "docketh",
            "drepbnb",
            "eosusdt",
            "eostusd",
            "npxseth",
            "thetaeth",
            "iotxbtc",
            "enjusdt",
            "tfuelbnb",
            "mcobnb",
            "ontpax",
            "dcrbnb",
            "batusdc",
            "snglseth",
            "qlcbtc",
            "qspeth",
            "appcbnb",
            "wprbtc",
            "sysbtc",
            "iostusdt",
            "btceur",
            "mtlusdt",
            "ethrub",
            "tfuelpax",
            "maticusdt",
            "xrpbusd",
            "iotxusdt",
            "tusdbtusd",
            "trigbtc",
            "atombtc",
            "bchpax",
            "eosbusd",
            "zileth",
            "gtotusd",
            "xrpbullusdt",
            "onetusd",
            "algobtc",
            "bchsvusdt",
            "gtopax",
            "etceth",
            "vibebtc",
            "bttusdt",
            "repeth",
            "iostbnb",
            "usdttry",
            "btsbnb",
            "ankrbnb",
            "dltbnb",
            "snteth",
            "linktusd",
            "nknusdt",
            "rpxbtc",
            "cocosusdt",
            "etcbusd",
            "btttrx",
            "bandbtc",
            "steembnb",
            "zecpax",
            "viabnb",
            "cosbnb",
            "mtheth",
            "xemeth",
            "pivxbnb",
            "phxbtc",
            "zilusdt",
            "poeeth",
            "bnbeur",
            "bandusdt",
            "vetbnb",
            "lendbtc",
            "duskusdt",
            "mfteth",
            "funusdt",
            "adabusd",
            "perlbnb",
            "btcbusd",
            "ltobtc",
            "nasbnb",
            "algousdt",
            "bchsvusdc",
            "mcousdt",
            "venbnb",
            "hceth",
            "fetusdt",
            "edobtc",
            "mftbnb",
            "cosusdt",
            "arpausdt",
            "ctxcusdt",
            "bqxbtc",
            "npxsusdc",
            "icxbnb",
            "bchbnb",
            "phbusdc",
            "tomousdc",
            "nulsbnb",
            "rcnbnb",
            "qtumbtc",
            "keyusdt",
            "agibtc",
            "mblbtc",
            "eoseth",
            "tusdbtc",
            "aioneth",
            "storjbtc",
            "lsketh",
            "bntbusd",
            "ncashbtc",
            "mblbnb",
            "polybnb",
            "aebnb",
            "ltceth",
            "dogeusdc",
            "wpreth",
            "syseth",
            "ognusdt",
            "nanobtc",
            "astbtc",
            "zrxeth",
            "adxeth",
            "gxseth",
            "ethbearbusd",
            "onepax",
            "scbtc",
            "ontbnb",
            "qlceth",
            "btsbusd",
            "rlcbtc",
            "chatbtc",
            "wabibnb",
            "renbnb",
            "xrpbullbusd",
            "wavesbtc",
            "rlcbnb",
            "phxeth",
            "winbtc",
            "storjeth",
            "wavesbusd",
            "iostbtc",
            "icxeth",
            "adatusd",
            "nknbnb",
            "pivxbtc",
            "perlusdt",
            "bullbusd",
            "bttusdc",
            "bcptbtc",
            "aebtc",
            "ethusdt",
            "ltousdt",
            "subbtc",
            "blzbtc",
            "tfuelusdt",
            "evxbtc",
            "hbarbtc",
            "ambeth",
            "winusdt",
            "qtumeth",
            "dgdbtc",
            "adaeth",
            "xrpbnb",
            "adapax",
            "usdsbusds",
            "cocosbnb",
            "navbtc",
            "rvnbtc",
            "tnbbtc",
            "bnbbtc",
            "neopax",
            "neobnb",
            "cosbtc",
            "powreth",
            "rlcusdt",
            "hbarbnb",
            "wabieth",
            "bqxeth",
            "aionbtc",
            "aeeth",
            "wrxbtc",
            "pptbtc",
            "nknbtc",
            "zecusdt",
            "stormeth",
            "qtumusdt",
        ]

        channels = [
            "kline_1m",
            "kline_5m",
            "kline_15m",
            "kline_30m",
            "kline_1h",
            "kline_12h",
            "kline_1w",
            "trade",
            "miniTicker",
            "depth20",
        ]

        stream_id1 = ""
        for channel in channels:
            stream_id1 = self.__class__.ubwa.create_stream(channel, markets)

        time.sleep(6)

        markets = [
            "xrpbearbusd",
            "zeceth",
            "cndbtc",
            "dashbtc",
            "atompax",
            "perlbtc",
            "ardreth",
            "zecbnb",
            "erdbnb",
            "xrpbearusdt",
            "stratbnb",
            "cmtbtc",
            "cvcbtc",
            "kncbtc",
            "rpxbnb",
            "zenbnb",
            "cndbnb",
            "wrxbtc",
            "pptbtc",
            "nknbtc",
            "zecusdt",
            "stormeth",
            "qtumusdt",
        ]

        streams = []
        for channel in channels:
            stream_id = self.__class__.ubwa.create_stream(
                channel,
                markets,
                stream_buffer_name=channel,
                ping_interval=10,
                ping_timeout=10,
                close_timeout=5,
            )
            streams.append(stream_id)

        time.sleep(1)
        stream_id2 = streams.pop()
        stream_id3 = streams.pop()
        stream_id4 = streams.pop()
        self.__class__.ubwa.create_stream("depth20", markets, stream_buffer_name=True)
        self.__class__.ubwa.create_stream(
            "kline_1s", "btceth", process_stream_data=processing_of_new_data
        )
        self.__class__.ubwa.create_stream(
            "kline_1s", "btceth", process_stream_data_async=processing_of_new_data_async
        )
        time.sleep(6)
        self.__class__.ubwa.print_summary()
        self.__class__.ubwa.print_stream_info(stream_id4)
        print(f"Stop stream as crash ...")
        self.__class__.ubwa._crash_stream(streams.pop())
        print(f"Stop stream as crash ... done")
        print(f"create_websocket_uri ...")
        self.__class__.ubwa.create_websocket_uri(False, False, stream_id1)
        print(f"create_websocket_uri ... done")
        print(f"unsubscribe_from_stream ...")
        self.__class__.ubwa.unsubscribe_from_stream(stream_id2, markets="erdbnb")
        self.__class__.ubwa.unsubscribe_from_stream(stream_id2, channels="trade")
        print(f"unsubscribe_from_stream ... done")
        self.__class__.ubwa.pop_stream_data_from_stream_buffer()
        self.__class__.ubwa.pop_stream_data_from_stream_buffer()
        self.__class__.ubwa.pop_stream_data_from_stream_buffer()
        self.__class__.ubwa.pop_stream_data_from_stream_buffer(
            stream_buffer_name="invalid"
        )
        print(f"Replace stream ...")
        for i in range(1):
            self.__class__.ubwa.print_summary()
            self.__class__.ubwa.print_stream_info(stream_id4)
            time.sleep(1)
        stream_id_1_1 = self.__class__.ubwa.replace_stream(
            stream_id4, "trade", "btceth", "name"
        )
        self.__class__.ubwa.replace_stream(
            stream_id_1_1,
            "trade",
            "btceth",
            "name2",
            new_ping_interval=10,
            new_ping_timeout=10,
            new_close_timeout=5,
        )
        print(f"Replace stream ... Done")
        self.__class__.ubwa.get_results_from_endpoints()
        self.__class__.ubwa.get_used_weight()
        self.__class__.ubwa.get_start_time()
        self.__class__.ubwa.get_stream_label(stream_id1)
        self.__class__.ubwa.get_stream_label(False)
        self.__class__.ubwa.get_keep_max_received_last_second_entries()
        request_id = self.__class__.ubwa.get_stream_subscriptions(stream_id2)
        self.__class__.ubwa.get_result_by_request_id(request_id)
        self.__class__.ubwa.get_reconnects()
        self.__class__.ubwa.get_errors_from_endpoints()
        self.__class__.ubwa.get_monitoring_status_plain()
        self.__class__.ubwa.get_ringbuffer_error_max_size()
        self.__class__.ubwa.get_ringbuffer_result_max_size()
        self.__class__.ubwa.set_ringbuffer_error_max_size(200)
        self.__class__.ubwa.set_ringbuffer_result_max_size(300)
        self.__class__.ubwa.set_stream_label(stream_id2, "blub")
        self.__class__.ubwa._add_stream_to_stream_list(
            self.__class__.ubwa.get_new_uuid_id(), "trade", "btceth"
        )
        self.__class__.ubwa.delete_stream_from_stream_list(stream_id1)
        self.__class__.ubwa.delete_listen_key_by_stream_id(stream_id1)
        self.__class__.ubwa.is_update_available_unicorn_fy()
        self.__class__.ubwa.get_version_unicorn_fy()
        self.__class__.ubwa.create_payload(stream_id2, "invalid", channels="trade")
        time.sleep(6)
        self.__class__.ubwa.get_result_by_request_id(request_id)
        self.__class__.ubwa.get_result_by_request_id()
        self.__class__.ubwa.set_keep_max_received_last_second_entries(30)
        self.__class__.ubwa._crash_stream(stream_id2)
        time.sleep(6)
        print(f"Waiting for {stream_id3} has started ...")
        self.__class__.ubwa.print_summary()
        self.__class__.ubwa.print_stream_info(stream_id3)
        self.__class__.ubwa.wait_till_stream_has_started(stream_id3)
        print(f"Waiting for {stream_id3} has started ... done!")
        print(f"Start Stop stream {stream_id2}...")
        self.__class__.ubwa.stop_stream(stream_id2)
        print(f" done!")
        self.__class__.ubwa.add_to_ringbuffer_error("test")
        self.__class__.ubwa.add_to_ringbuffer_result("test")
        self.__class__.ubwa.get_number_of_free_subscription_slots(stream_id2)
        self.__class__.ubwa.get_most_receives_per_second()
        self.__class__.ubwa.get_number_of_streams_in_stream_list()
        print(f"Waiting for {stream_id2} has stopped")
        self.__class__.ubwa.wait_till_stream_has_stopped(stream_id2)
        print(f"Done!")
        self.__class__.ubwa.print_stream_info(stream_id2)
        self.__class__.ubwa.print_summary()
        if platform.system() != "Windows":
            self.__class__.ubwa.print_summary_to_png(
                ".", 12.5, add_string="test: blah", footer="UBWA", title="UBWA Unittest"
            )
        self.__class__.ubwa.get_latest_release_info()
        self.__class__.ubwa.get_version()
        self.__class__.ubwa.help()
        self.__class__.ubwa.get_current_receiving_speed_global()
        self.__class__.ubwa.remove_ansi_escape_codes("test text")
        self.__class__.ubwa.pop_stream_signal_from_stream_signal_buffer()

        with BinanceRestApiManager(exchange="binance.us") as ubra:
            markets = []
            data = ubra.get_all_tickers()
            for item in data:
                markets.append(item["symbol"])
        self.__class__.ubwa.create_stream("trade", markets, stream_label="too much!")
        time.sleep(1)
        self.__class__.ubwa.stop_manager()
        time.sleep(1)


class TestBinanceOptionsManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestBinanceOptionsManager:")
        cls.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com-vanilla-options", disable_colorama=True
        )

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_manager()

    def test_exchange_string(self):
        self.assertEqual(
            str(self.__class__.ubwa.get_exchange()), "binance.com-vanilla-options"
        )

    def test_is_exchange_type_cex(self):
        self.assertTrue(self.__class__.ubwa.is_exchange_type("cex"))

    def test_max_subscriptions(self):
        self.assertEqual(
            self.__class__.ubwa.get_limit_of_subscriptions_per_stream(), 200
        )

    def test_create_stream(self):
        stream_id = self.__class__.ubwa.create_stream(
            markets=["btc-260626-120000-c"],
            channels=["depth@500ms"],
            stream_label="options_test",
        )
        self.assertTrue(bool(stream_id))
        time.sleep(3)
        self.__class__.ubwa.stop_stream(stream_id)


class TestBinancePortfolioMarginManager(unittest.TestCase):
    """Tests for the `binance.com-portfolio_margin` exchange (issue #452).

    Scope is currently limited to user data stream (listenKey) support - there
    is no public market data endpoint at `/pm/` to subscribe to, so these tests
    stay off the network by injecting a `provided_listen_key` instead of
    talking to `restclient`.
    """

    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestBinancePortfolioMarginManager:")
        cls.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com-portfolio_margin", disable_colorama=True
        )

    @classmethod
    def tearDownClass(cls):
        # `delete_listen_key=False`: the injected `provided_listen_key` in
        # `test_create_uri_userdata_uses_pm_ws_path` is a dummy value, deleting it
        # would trigger a real (and unwanted) REST call.
        cls.ubwa.stop_manager(delete_listen_key=False)

    def test_exchange_string(self):
        self.assertEqual(
            str(self.__class__.ubwa.get_exchange()), "binance.com-portfolio_margin"
        )

    def test_is_exchange_type_cex(self):
        self.assertTrue(self.__class__.ubwa.is_exchange_type("cex"))

    def test_max_subscriptions(self):
        self.assertEqual(
            self.__class__.ubwa.get_limit_of_subscriptions_per_stream(), 200
        )

    def test_websocket_base_uri(self):
        self.assertEqual(
            self.__class__.ubwa.websocket_base_uri, "wss://fstream.binance.com/pm/"
        )

    def test_websocket_api_base_uri_not_supported(self):
        # Portfolio Margin only covers user data streams for now, no WS API.
        self.assertIsNone(self.__class__.ubwa.websocket_api_base_uri)

    def test_futures_path_prefix_is_empty(self):
        # Portfolio Margin is NOT part of BINANCE_FUTURES_EXCHANGES: it keeps the
        # legacy `/ws/<listenKey>` path form and must not get a /public,/market,
        # /private category segment prepended.
        self.assertEqual(
            self.__class__.ubwa._futures_path_prefix(["!userData"], ["arr"]), ""
        )

    def test_create_uri_userdata_uses_pm_ws_path(self):
        stream_id = self.__class__.ubwa.get_new_uuid_id()
        self.__class__.ubwa._add_stream_to_stream_list(
            stream_id, ["!userData"], ["arr"], provided_listen_key="unittest-listen-key"
        )
        uri = self.__class__.ubwa.create_websocket_uri(
            ["!userData"], ["arr"], stream_id
        )
        self.assertEqual(uri, "wss://fstream.binance.com/pm/ws/unittest-listen-key")


class TestPortfolioMarginRestclientRouting(unittest.TestCase):
    """Regression test for the `binance.com-portfolio_margin` routing added in
    `restclient.py` for issue #452. Uses a mocked `ubra` so it doesn't depend
    on a released UBRA version already exposing the `portfolio_margin_stream_*`
    methods.
    """

    @staticmethod
    def _make_restclient():
        stream_list = {
            "id1": {
                "api_key": "key",
                "api_secret": "secret",
                "symbols": None,
                "listen_key": "existing-key",
            }
        }
        rc = BinanceWebSocketApiRestclient(
            exchange="binance.com-portfolio_margin", stream_list=stream_list
        )
        rc.ubra = Mock()
        rc.ubra.get_used_weight.return_value = {"weight": 0}
        return rc

    def test_get_listen_key_routes_to_portfolio_margin(self):
        rc = self._make_restclient()
        rc.ubra.portfolio_margin_stream_get_listen_key.return_value = {
            "listenKey": "new-key"
        }
        response, _ = rc.get_listen_key(stream_id="id1")
        rc.ubra.portfolio_margin_stream_get_listen_key.assert_called_once_with(
            output="raw_data", throw_exception=False, api_key="key", api_secret="secret"
        )
        self.assertEqual(response, {"listenKey": "new-key"})

    def test_keepalive_routes_to_portfolio_margin(self):
        rc = self._make_restclient()
        rc.ubra.portfolio_margin_stream_keepalive.return_value = {}
        rc.keepalive_listen_key(stream_id="id1")
        rc.ubra.portfolio_margin_stream_keepalive.assert_called_once_with(
            listenKey="existing-key",
            throw_exception=False,
            api_key="key",
            api_secret="secret",
        )

    def test_delete_routes_to_portfolio_margin(self):
        rc = self._make_restclient()
        rc.ubra.portfolio_margin_stream_close.return_value = {}
        rc.delete_listen_key(stream_id="id1")
        rc.ubra.portfolio_margin_stream_close.assert_called_once_with(
            listenKey="existing-key",
            throw_exception=False,
            api_key="key",
            api_secret="secret",
        )


class TestRestclientUnknownExchangeIsHandled(unittest.TestCase):
    """Regression test: `_init_ubra()` must not let `UnknownExchange` propagate uncaught.

    This can happen for ANY exchange string UBWA knows but the installed UBRA doesn't (yet) -
    e.g. right after a new exchange is added to UBWA ahead of the corresponding UBRA release,
    as was the case for `binance.com-portfolio_margin` in issue #452. Uses a bogus exchange
    string instead of `binance.com-portfolio_margin` so this test stays meaningful even after
    UBRA ships Portfolio Margin support.
    """

    @staticmethod
    def _make_restclient():
        stream_list = {
            "id1": {
                "api_key": "key",
                "api_secret": "secret",
                "symbols": None,
                "listen_key": "existing-key",
            }
        }
        return BinanceWebSocketApiRestclient(
            exchange="bogus-exchange-for-unittest", stream_list=stream_list
        )

    def test_get_listen_key_returns_none_instead_of_raising(self):
        rc = self._make_restclient()
        self.assertEqual(rc.get_listen_key(stream_id="id1"), (None, None))
        self.assertIsNone(rc.ubra)

    def test_keepalive_listen_key_returns_none_instead_of_raising(self):
        rc = self._make_restclient()
        self.assertEqual(rc.keepalive_listen_key(stream_id="id1"), (None, None))

    def test_delete_listen_key_returns_none_instead_of_raising(self):
        rc = self._make_restclient()
        self.assertEqual(rc.delete_listen_key(stream_id="id1"), (None, None))


class _LocalWebSocketServer:
    """
    Deterministic local stand-in for Binance, used by `TestWebSocketLibrary`.

    One `websockets` server; the scenario is selected by the market name in
    UBWA's stream URI (`/stream?streams=<market>@<channel>`), the WebSocket API
    base URI maps to the `api` scenario. Every connection answers client
    requests like Binance does (`{"result": null, "id": ...}`, WS API requests
    with a `status`/`result` envelope) and sends a heartbeat message every
    0.5 s so that UBWA's untimed `recv()` (see context/stream-loop.md) can be
    stopped.
    """

    def __init__(self):
        self.port = None
        self.ready = threading.Event()
        self.connections = {}
        self.pongs = {}
        self.thread = None

    def start(self):
        self.thread = threading.Thread(
            target=lambda: asyncio.run(self._serve()), daemon=True
        )
        self.thread.start()
        self.ready.wait(10)

    async def _serve(self):
        import websockets

        async with websockets.serve(
            self._handler,
            "127.0.0.1",
            0,
            process_request=self._process_request,
            max_size=None,
        ) as server:
            self.port = server.sockets[0].getsockname()[1]
            self.ready.set()
            await asyncio.Future()

    @staticmethod
    def _scenario_of(path):
        if path.startswith("/ws-api"):
            return "api"
        streams = (
            path.split("streams=")[-1]
            if "streams=" in path
            else path.rsplit("/", 1)[-1]
        )
        return streams.split("@")[0].split("/")[0]

    def _process_request(self, connection, request):
        scenario = self._scenario_of(request.path)
        self.connections[scenario] = self.connections.get(scenario, 0) + 1
        if scenario == "http429":
            return connection.respond(429, "Too Many Requests\n")
        if scenario == "http404":
            return connection.respond(404, "Not Found\n")
        return None

    async def _handler(self, ws):
        scenario = self._scenario_of(ws.request.path)
        connection_number = self.connections.get(scenario, 1)

        async def heartbeat():
            while True:
                await asyncio.sleep(0.5)
                await ws.send('{"heartbeat":true}')

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            scenario_coroutine = getattr(self, f"_scenario_{scenario}", None)
            if scenario_coroutine is not None:
                await scenario_coroutine(ws, connection_number)
            async for msg in ws:
                request = orjson.loads(msg)
                if request.get("method") == "time":
                    await ws.send(
                        f'{{"id":"{request["id"]}","status":200,'
                        f'"result":{{"serverTime":1234567890123}}}}'
                    )
                else:
                    await ws.send(f'{{"result":null,"id":{request["id"]}}}')
        except Exception:
            pass
        finally:
            heartbeat_task.cancel()

    @staticmethod
    def _trade(scenario, number):
        return f'{{"stream":"{scenario}@trade","data":{{"e":"trade","s":"TEST","t":{number}}}}}'

    async def _scenario_btcusdt(self, ws, connection_number):
        # Stay idle first: exercises UBWA's `asyncio.wait_for(recv(), 1)`
        # timeout/cancel path before the first message arrives.
        await asyncio.sleep(2)
        for i in range(25):
            await ws.send(self._trade("btcusdt", i))

    async def _scenario_reconnect(self, ws, connection_number):
        for i in range(5):
            await ws.send(
                self._trade("reconnect", i + (5 if connection_number > 1 else 0))
            )
        if connection_number == 1:
            # Let the client's subscribe request through first, otherwise its
            # `send()` hits the closed connection before `recv()` ever read
            # the five trades above.
            request = orjson.loads(await ws.recv())
            await ws.send(f'{{"result":null,"id":{request["id"]}}}')
            await ws.close(1001, "going away")

    async def _scenario_fragment(self, ws, connection_number):
        # An iterable makes `websockets` send one message as several frames
        # (FIN only on the last one) - the client has to reassemble it.
        message = self._trade("fragment", 0)
        third = len(message) // 3
        await ws.send(
            [message[:third], message[third : 2 * third], message[2 * third :]]
        )

    async def _scenario_large(self, ws, connection_number):
        # ~450 KB, the size of a real `!ticker@arr` message, below the 1 MiB
        # default `max_size` of both libraries.
        items = ",".join(f'{{"s":"SYM{i:04d}","c":"{i}.0"}}' for i in range(20000))
        await ws.send(f'{{"stream":"large@trade","data":[{items}]}}')

    async def _scenario_toolarge(self, ws, connection_number):
        # 1.5 MiB on the first connection only: above `max_size`, the client
        # must close with 1009 and UBWA must reconnect - not hang.
        if connection_number == 1:
            await ws.send(
                '{"stream":"toolarge@trade","data":"' + "x" * (1536 * 1024) + '"}'
            )
        else:
            await ws.send(self._trade("toolarge", 0))

    async def _scenario_ping(self, ws, connection_number):
        # Binance pings its clients and drops them without a pong; both
        # libraries have to answer server pings automatically.
        pong_waiter = await ws.ping()
        await asyncio.wait_for(pong_waiter, 5)
        self.pongs["ping"] = True
        await ws.send(self._trade("ping", 0))

    async def _scenario_unicode(self, ws, connection_number):
        await ws.send('{"stream":"unicode@trade","data":{"e":"trade","s":"€ÜŸ–日本"}}')

    async def _scenario_markers(self, ws, connection_number):
        # A ~400 KB data message whose payload contains the words "error" and
        # "result" far behind the JSON head must reach the callback as data,
        # not the error/result ringbuffers; a genuine endpoint error must.
        items = ",".join(
            f'{{"s":"SYM{i:04d}","c":"{i}.0"'
            + (',"note":"result of error"' if i >= 100 else "")
            + "}"
            for i in range(10000)
        )
        await ws.send(f'{{"stream":"markers@trade","data":[{items}]}}')
        await ws.send(self._trade("markers", 0))
        await ws.send('{"error":{"code":2,"msg":"Invalid request"},"id":424242}')

    async def _scenario_bytes(self, ws, connection_number):
        for i in range(3):
            await ws.send(self._trade("bytes", i) + " " * (100 * i))


class _LocalSocks5Proxy:
    """
    Minimal SOCKS5 server (RFC 1928, user/password auth per RFC 1929) for
    `TestWebSocketLibrary`: no auth or user/password, CONNECT to IPv4/IPv6/
    domain targets, then pipes bytes in both directions. Counts established
    tunnels and rejected logins.
    """

    def __init__(self, user=None, password=None):
        self.user = user
        self.password = password
        self.port = None
        self.ready = threading.Event()
        self.connections = 0
        self.rejected = 0
        self.thread = None

    def start(self):
        self.thread = threading.Thread(
            target=lambda: asyncio.run(self._serve()), daemon=True
        )
        self.thread.start()
        self.ready.wait(10)

    async def _serve(self):
        server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = server.sockets[0].getsockname()[1]
        self.ready.set()
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer):
        try:
            version, method_count = await reader.readexactly(2)
            methods = await reader.readexactly(method_count)
            if self.user is not None:
                if 2 not in methods:
                    writer.write(b"\x05\xff")
                    await writer.drain()
                    return
                writer.write(b"\x05\x02")
                await writer.drain()
                await reader.readexactly(1)  # auth version
                user_length = (await reader.readexactly(1))[0]
                user = (await reader.readexactly(user_length)).decode()
                password_length = (await reader.readexactly(1))[0]
                password = (await reader.readexactly(password_length)).decode()
                if user != self.user or password != self.password:
                    self.rejected += 1
                    writer.write(b"\x01\x01")
                    await writer.drain()
                    return
                writer.write(b"\x01\x00")
                await writer.drain()
            else:
                writer.write(b"\x05\x00")
                await writer.drain()
            version, command, reserved, address_type = await reader.readexactly(4)
            if address_type == 1:
                host = socket.inet_ntoa(await reader.readexactly(4))
            elif address_type == 3:
                host_length = (await reader.readexactly(1))[0]
                host = (await reader.readexactly(host_length)).decode()
            else:
                host = socket.inet_ntop(socket.AF_INET6, await reader.readexactly(16))
            port = int.from_bytes(await reader.readexactly(2), "big")
            if command != 1:
                writer.write(b"\x05\x07\x00\x01" + b"\x00" * 6)
                await writer.drain()
                return
            try:
                target_reader, target_writer = await asyncio.open_connection(host, port)
            except OSError:
                writer.write(b"\x05\x05\x00\x01" + b"\x00" * 6)
                await writer.drain()
                return
            self.connections += 1
            writer.write(b"\x05\x00\x00\x01" + b"\x00" * 6)
            await writer.drain()

            async def pipe(source, sink):
                try:
                    while True:
                        data = await source.read(65536)
                        if not data:
                            break
                        sink.write(data)
                        await sink.drain()
                except Exception:
                    pass
                finally:
                    try:
                        sink.close()
                    except Exception:
                        pass

            await asyncio.gather(
                pipe(reader, target_writer), pipe(target_reader, writer)
            )
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass


class _LocalHttpConnectProxy:
    """
    Minimal HTTP proxy for `TestWebSocketLibrary`: answers `CONNECT host:port`
    (optionally with Proxy-Authorization: Basic) with `200 Connection
    established` and pipes bytes. Counts tunnels and rejected logins.
    """

    def __init__(self, user=None, password=None):
        self.user = user
        self.password = password
        self.port = None
        self.ready = threading.Event()
        self.connections = 0
        self.rejected = 0
        self.thread = None

    def start(self):
        self.thread = threading.Thread(
            target=lambda: asyncio.run(self._serve()), daemon=True
        )
        self.thread.start()
        self.ready.wait(10)

    async def _serve(self):
        server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = server.sockets[0].getsockname()[1]
        self.ready.set()
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer):
        import base64

        try:
            head = await reader.readuntil(b"\r\n\r\n")
            request_line, *header_lines = head.decode().split("\r\n")
            method, target, _ = request_line.split(" ", 2)
            headers = {}
            for line in header_lines:
                if ":" in line:
                    name, value = line.split(":", 1)
                    headers[name.strip().lower()] = value.strip()
            if method != "CONNECT":
                writer.write(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                await writer.drain()
                return
            if self.user is not None:
                expected = (
                    "Basic "
                    + base64.b64encode(f"{self.user}:{self.password}".encode()).decode()
                )
                if headers.get("proxy-authorization") != expected:
                    self.rejected += 1
                    writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                    await writer.drain()
                    return
            host, port = target.rsplit(":", 1)
            try:
                target_reader, target_writer = await asyncio.open_connection(
                    host, int(port)
                )
            except OSError:
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await writer.drain()
                return
            self.connections += 1
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()

            async def pipe(source, sink):
                try:
                    while True:
                        data = await source.read(65536)
                        if not data:
                            break
                        sink.write(data)
                        await sink.drain()
                except Exception:
                    pass
                finally:
                    try:
                        sink.close()
                    except Exception:
                        pass

            await asyncio.gather(
                pipe(reader, target_writer), pipe(target_reader, writer)
            )
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass


class TestWebSocketLibrary(unittest.TestCase):
    """
    `websocket_library` switch: `websockets` (default) vs. the optional `picows`
    (`picows.websockets` compatibility API). Scenario tests run against the
    local server above for both libraries, so they are deterministic and work
    without internet access. picows scenarios are skipped if it isn't installed.
    """

    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestWebSocketLibrary:")
        cls.server = _LocalWebSocketServer()
        cls.server.start()
        from unicorn_binance_websocket_api import websocket_library

        cls.libraries = ["websockets"]
        if websocket_library.is_picows_available():
            cls.libraries.append("picows")

    def _manager(self, websocket_library, scenario=None, **kwargs):
        # Scenarios that behave differently per connection count from zero
        # for every (library, scenario) sub-test.
        if scenario is not None:
            self.server.connections.pop(scenario, None)
        options = dict(
            exchange="binance.com",
            websocket_library=websocket_library,
            websocket_base_uri=f"ws://127.0.0.1:{self.server.port}/",
            websocket_api_base_uri=f"ws://127.0.0.1:{self.server.port}/ws-api/v3",
            output_default="dict",
            warn_on_update=False,
            disable_colorama=True,
        )
        options.update(kwargs)
        return BinanceWebSocketApiManager(**options)

    @staticmethod
    def _wait_for(condition, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return True
            time.sleep(0.05)
        return condition()

    @staticmethod
    def _trades(received):
        return [item for item in received if isinstance(item, dict) and "data" in item]

    # --- switch -------------------------------------------------------------

    def test_default_is_websockets(self):
        print(f"test_default_is_websockets():")
        with BinanceWebSocketApiManager(
            exchange="binance.com", warn_on_update=False, disable_colorama=True
        ) as ubwa:
            self.assertEqual(ubwa.websocket_library, "websockets")

    def test_unknown_library_raises(self):
        print(f"test_unknown_library_raises():")
        with self.assertRaises(ValueError):
            BinanceWebSocketApiManager(
                exchange="binance.com",
                websocket_library="hyperws",
                warn_on_update=False,
                disable_colorama=True,
            )

    def test_picows_not_installed_raises(self):
        print(f"test_picows_not_installed_raises():")
        from unicorn_binance_websocket_api import websocket_library

        with patch.object(websocket_library, "is_picows_available", return_value=False):
            with self.assertRaises(ImportError):
                BinanceWebSocketApiManager(
                    exchange="binance.com",
                    websocket_library="picows",
                    warn_on_update=False,
                    disable_colorama=True,
                )

    # --- scenarios, both libraries -------------------------------------------

    def test_roundtrip(self):
        print(f"test_roundtrip():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(library, process_stream_data=received.append)
                try:
                    self.assertEqual(ubwa.websocket_library, library)
                    stream_id = ubwa.create_stream(["trade"], ["btcusdt"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 25)
                    )
                    self.assertEqual(len(self._trades(received)), 25)
                    self.assertEqual(self._trades(received)[0]["data"]["t"], 0)
                    results_before = len(ubwa.get_results_from_endpoints())
                    ubwa.subscribe_to_stream(stream_id, channels=["kline_1m"])
                    self.assertTrue(
                        self._wait_for(
                            lambda: len(ubwa.get_results_from_endpoints())
                            > results_before,
                            10,
                        )
                    )
                    self.assertEqual(ubwa.get_stream_info(stream_id)["reconnects"], 0)
                finally:
                    ubwa.stop_manager()

    def test_response_markers_scanned_in_head_only(self):
        print(f"test_response_markers_scanned_in_head_only():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(library, process_stream_data=received.append)
                try:
                    ubwa.create_stream(["trade"], ["markers"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 2)
                    )
                    self.assertTrue(
                        self._wait_for(
                            lambda: any(
                                "424242" in error
                                for error in ubwa.get_errors_from_endpoints()
                            ),
                            10,
                        )
                    )
                    # the big data message arrived as data ...
                    big = [
                        t for t in self._trades(received) if isinstance(t["data"], list)
                    ]
                    self.assertEqual(len(big), 1)
                    self.assertEqual(len(big[0]["data"]), 10000)
                    # ... and not as a result or error of the endpoint
                    self.assertFalse(
                        any("SYM0000" in r for r in ubwa.get_results_from_endpoints())
                    )
                    self.assertFalse(
                        any("SYM0000" in e for e in ubwa.get_errors_from_endpoints())
                    )
                    # the subscribe acknowledgment is still classified as result
                    self.assertTrue(
                        any('"result"' in r for r in ubwa.get_results_from_endpoints())
                    )
                finally:
                    ubwa.stop_manager()

    def test_reconnect_after_server_close(self):
        print(f"test_reconnect_after_server_close():")
        for library in self.libraries:
            with self.subTest(library=library):
                received, signals = [], []
                ubwa = self._manager(
                    library,
                    scenario="reconnect",
                    process_stream_data=received.append,
                    process_stream_signals=lambda signal_type=None, stream_id=None, data_record=None, error_msg=None: signals.append(
                        signal_type
                    ),
                )
                try:
                    stream_id = ubwa.create_stream(["trade"], ["reconnect"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 10)
                    )
                    self.assertEqual(
                        [t["data"]["t"] for t in self._trades(received)],
                        list(range(10)),
                    )
                    self.assertTrue(
                        self._wait_for(
                            lambda: ubwa.get_stream_info(stream_id)["reconnects"] == 1,
                            5,
                        )
                    )
                    self.assertIn("DISCONNECT", signals)
                    self.assertEqual(signals.count("CONNECT"), 2)
                finally:
                    ubwa.stop_manager()

    def test_fragmented_message(self):
        print(f"test_fragmented_message():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(library, process_stream_data=received.append)
                try:
                    ubwa.create_stream(["trade"], ["fragment"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 1)
                    )
                    self.assertEqual(
                        self._trades(received)[0]["data"],
                        {"e": "trade", "s": "TEST", "t": 0},
                    )
                finally:
                    ubwa.stop_manager()

    def test_large_message(self):
        print(f"test_large_message():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(library, process_stream_data=received.append)
                try:
                    stream_id = ubwa.create_stream(["trade"], ["large"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 1)
                    )
                    self.assertEqual(len(self._trades(received)[0]["data"]), 20000)
                    self.assertEqual(ubwa.get_stream_info(stream_id)["reconnects"], 0)
                finally:
                    ubwa.stop_manager()

    def test_message_above_max_size_reconnects(self):
        print(f"test_message_above_max_size_reconnects():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(
                    library, scenario="toolarge", process_stream_data=received.append
                )
                try:
                    stream_id = ubwa.create_stream(["trade"], ["toolarge"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 1)
                    )
                    self.assertEqual(self._trades(received)[0]["data"]["t"], 0)
                    self.assertGreaterEqual(
                        ubwa.get_stream_info(stream_id)["reconnects"], 1
                    )
                finally:
                    ubwa.stop_manager()

    def test_server_ping_is_answered(self):
        print(f"test_server_ping_is_answered():")
        for library in self.libraries:
            with self.subTest(library=library):
                self.server.pongs.pop("ping", None)
                received = []
                ubwa = self._manager(library, process_stream_data=received.append)
                try:
                    ubwa.create_stream(["trade"], ["ping"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 1)
                    )
                    self.assertTrue(self.server.pongs.get("ping"))
                finally:
                    ubwa.stop_manager()

    def test_unicode_payload(self):
        print(f"test_unicode_payload():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(library, process_stream_data=received.append)
                try:
                    ubwa.create_stream(["trade"], ["unicode"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 1)
                    )
                    self.assertEqual(self._trades(received)[0]["data"]["s"], "€ÜŸ–日本")
                finally:
                    ubwa.stop_manager()

    def test_received_bytes_statistic(self):
        print(f"test_received_bytes_statistic():")
        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(
                    library,
                    output_default="raw_data",
                    process_stream_data=received.append,
                )
                try:
                    ubwa.create_stream(["trade"], ["bytes"])
                    self.assertTrue(
                        self._wait_for(
                            lambda: sum(1 for m in received if '"data"' in m) >= 3
                        )
                    )
                    time.sleep(0.2)
                    expected = sum(len(m) for m in received)
                    self.assertEqual(ubwa.get_total_received_bytes(), expected)
                finally:
                    ubwa.stop_manager()

    # --- SOCKS5 proxy -------------------------------------------------------

    def test_socks5_proxy_roundtrip(self):
        # Both libraries tunnel through a SOCKS5 proxy with user/password
        # auth via their native `proxy=` support; the legacy `socks5_proxy_*`
        # parameters and the URL form must be equivalent. `high_performance`
        # keeps `create_stream()` from blocking forever if the tunnel never
        # comes up - the test then fails instead of hanging.
        print(f"test_socks5_proxy_roundtrip():")
        proxy = _LocalSocks5Proxy(user="alice", password="s3cret")
        proxy.start()
        forms = {
            "legacy": dict(
                socks5_proxy_server=f"127.0.0.1:{proxy.port}",
                socks5_proxy_user="alice",
                socks5_proxy_pass="s3cret",
            ),
            "url": dict(proxy=f"socks5://alice:s3cret@127.0.0.1:{proxy.port}"),
        }
        for library in self.libraries:
            for form, proxy_kwargs in forms.items():
                with self.subTest(library=library, form=form):
                    proxy.connections = 0
                    received = []
                    ubwa = self._manager(
                        library,
                        scenario="bytes",
                        process_stream_data=received.append,
                        high_performance=True,
                        **proxy_kwargs,
                    )
                    try:
                        self.assertEqual(
                            ubwa.proxy, f"socks5://alice:s3cret@127.0.0.1:{proxy.port}"
                        )
                        stream_id = ubwa.create_stream(["trade"], ["bytes"])
                        self.assertTrue(
                            self._wait_for(lambda: len(self._trades(received)) >= 3)
                        )
                        self.assertEqual(proxy.connections, 1)
                        self.assertEqual(
                            ubwa.get_stream_info(stream_id)["reconnects"], 0
                        )
                    finally:
                        ubwa.stop_manager()

    def test_socks5_proxy_credentials_with_reserved_characters(self):
        # `websockets` does not percent-decode proxy credentials (reported
        # upstream), UBWA refuses them at construction; `picows` decodes and
        # must log in with them.
        print(f"test_socks5_proxy_credentials_with_reserved_characters():")
        proxy = _LocalSocks5Proxy(user="alice", password="s3cret:@/")
        proxy.start()
        for proxy_kwargs in (
            dict(proxy=f"socks5://alice:s3cret%3A%40%2F@127.0.0.1:{proxy.port}"),
            dict(
                socks5_proxy_server=f"127.0.0.1:{proxy.port}",
                socks5_proxy_user="alice",
                socks5_proxy_pass="s3cret:@/",
            ),
        ):
            with self.assertRaises(ValueError):
                self._manager("websockets", **proxy_kwargs)
        if "picows" not in self.libraries:
            return
        received = []
        ubwa = self._manager(
            "picows",
            scenario="bytes",
            process_stream_data=received.append,
            high_performance=True,
            proxy=f"socks5://alice:s3cret%3A%40%2F@127.0.0.1:{proxy.port}",
        )
        try:
            ubwa.create_stream(["trade"], ["bytes"])
            self.assertTrue(self._wait_for(lambda: len(self._trades(received)) >= 3))
            self.assertEqual(proxy.connections, 1)
            self.assertEqual(proxy.rejected, 0)
        finally:
            ubwa.stop_manager()

    def test_http_proxy_roundtrip(self):
        # `http://` proxy (CONNECT tunnel with basic auth) through the
        # libraries' native proxy support, both libraries.
        print(f"test_http_proxy_roundtrip():")
        proxy = _LocalHttpConnectProxy(user="alice", password="s3cret")
        proxy.start()
        for library in self.libraries:
            with self.subTest(library=library):
                proxy.connections = 0
                received = []
                ubwa = self._manager(
                    library,
                    scenario="bytes",
                    process_stream_data=received.append,
                    high_performance=True,
                    proxy=f"http://alice:s3cret@127.0.0.1:{proxy.port}",
                )
                try:
                    stream_id = ubwa.create_stream(["trade"], ["bytes"])
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 3)
                    )
                    self.assertEqual(proxy.connections, 1)
                    self.assertEqual(ubwa.get_stream_info(stream_id)["reconnects"], 0)
                finally:
                    ubwa.stop_manager()

    def test_http_proxy_rejected_credentials_keeps_restarting(self):
        print(f"test_http_proxy_rejected_credentials_keeps_restarting():")
        proxy = _LocalHttpConnectProxy(user="alice", password="right")
        proxy.start()
        for library in self.libraries:
            with self.subTest(library=library):
                proxy.rejected = 0
                ubwa = self._manager(
                    library,
                    scenario="bytes",
                    restart_timeout=1,
                    high_performance=True,
                    proxy=f"http://alice:wrong@127.0.0.1:{proxy.port}",
                )
                try:
                    with self.assertLogs(
                        "unicorn_binance_websocket_api", level="ERROR"
                    ) as logs:
                        stream_id = ubwa.create_stream(["trade"], ["bytes"])
                        self.assertTrue(self._wait_for(lambda: proxy.rejected >= 3, 30))
                    self.assertEqual(
                        ubwa.get_stream_info(stream_id)["status"], "restarting"
                    )
                    self.assertTrue(
                        any("ProxyConnectionError" in line for line in logs.output)
                    )
                finally:
                    ubwa.stop_manager()

    def test_proxy_configuration_fails_loud(self):
        print(f"test_proxy_configuration_fails_loud():")
        with self.assertRaises(ValueError):
            self._manager("websockets", proxy="ftp://127.0.0.1:1")
        with self.assertRaises(ValueError):
            self._manager("websockets", proxy="socks5://:1080")
        with self.assertRaises(ValueError):
            self._manager(
                "websockets",
                proxy="socks5://127.0.0.1:1080",
                socks5_proxy_server="127.0.0.1:1080",
            )
        with self.assertRaises(ValueError):
            self._manager("websockets", socks5_proxy_server="127.0.0.1")
        ubwa = self._manager("websockets", proxy="socks5://alice:pass@127.0.0.1:1080")
        try:
            # Legacy attributes are derived for the REST client, password masked in info.
            self.assertEqual(ubwa.socks5_proxy_server, "127.0.0.1:1080")
            self.assertEqual(ubwa.socks5_proxy_pass, "pass")
            self.assertEqual(ubwa.get_proxy_info(), "socks5://alice:***@127.0.0.1:1080")
        finally:
            ubwa.stop_manager()

    def test_socks5_proxy_rejected_credentials_keeps_restarting(self):
        # Rejected login must surface as `ProxyConnectionError` and a
        # restarting stream for both libraries - not as a silently dead
        # stream thread (PySocks raises `SOCKS5AuthError`, python-socks
        # `ProxyError`).
        print(f"test_socks5_proxy_rejected_credentials_keeps_restarting():")
        proxy = _LocalSocks5Proxy(user="alice", password="right")
        proxy.start()
        for library in self.libraries:
            with self.subTest(library=library):
                proxy.rejected = 0
                ubwa = self._manager(
                    library,
                    scenario="bytes",
                    restart_timeout=1,
                    high_performance=True,
                    proxy=f"socks5://alice:wrong@127.0.0.1:{proxy.port}",
                )
                try:
                    with self.assertLogs(
                        "unicorn_binance_websocket_api", level="ERROR"
                    ) as logs:
                        stream_id = ubwa.create_stream(["trade"], ["bytes"])
                        self.assertTrue(self._wait_for(lambda: proxy.rejected >= 3, 30))
                    self.assertEqual(
                        ubwa.get_stream_info(stream_id)["status"], "restarting"
                    )
                    self.assertTrue(
                        any("ProxyConnectionError" in line for line in logs.output)
                    )
                finally:
                    ubwa.stop_manager()

    def test_socks5_proxy_unreachable_keeps_restarting(self):
        print(f"test_socks5_proxy_unreachable_keeps_restarting():")
        for library in self.libraries:
            with self.subTest(library=library):
                ubwa = self._manager(
                    library,
                    scenario="bytes",
                    restart_timeout=1,
                    high_performance=True,
                    socks5_proxy_server="127.0.0.1:1",
                )
                try:
                    with self.assertLogs(
                        "unicorn_binance_websocket_api", level="ERROR"
                    ) as logs:
                        stream_id = ubwa.create_stream(["trade"], ["bytes"])
                        self.assertTrue(
                            self._wait_for(
                                lambda: sum(
                                    "ProxyConnectionError" in line
                                    for line in logs.output
                                )
                                >= 2,
                                20,
                            )
                        )
                    self.assertEqual(
                        ubwa.get_stream_info(stream_id)["status"], "restarting"
                    )
                finally:
                    ubwa.stop_manager()

    def test_handshake_429_crashes_stream(self):
        print(f"test_handshake_429_crashes_stream():")
        for library in self.libraries:
            with self.subTest(library=library):
                # `create_stream()` blocks until the socket is ready unless
                # `high_performance=True`; a rejected handshake never gets there.
                ubwa = self._manager(library, scenario="http429", high_performance=True)
                try:
                    stream_id = ubwa.create_stream(["trade"], ["http429"])
                    self.assertTrue(
                        self._wait_for(
                            lambda: ubwa.get_stream_info(stream_id)[
                                "status"
                            ].startswith("crashed"),
                            15,
                        )
                    )
                    self.assertEqual(ubwa.get_stream_info(stream_id)["reconnects"], 0)
                finally:
                    ubwa.stop_manager()

    def test_handshake_404_keeps_restarting(self):
        print(f"test_handshake_404_keeps_restarting():")
        for library in self.libraries:
            with self.subTest(library=library):
                ubwa = self._manager(
                    library,
                    scenario="http404",
                    restart_timeout=1,
                    high_performance=True,
                )
                try:
                    stream_id = ubwa.create_stream(["trade"], ["http404"])
                    self.assertTrue(
                        self._wait_for(
                            lambda: self.server.connections.get("http404", 0) >= 3,
                            20,
                        )
                    )
                    self.assertEqual(
                        ubwa.get_stream_info(stream_id)["status"], "restarting"
                    )
                finally:
                    ubwa.stop_manager()

    def test_websocket_api_request(self):
        print(f"test_websocket_api_request():")
        for library in self.libraries:
            with self.subTest(library=library):
                ubwa = self._manager(library)
                try:
                    stream_id = ubwa.create_stream(
                        api=True, api_key="test-key", api_secret="test-secret"
                    )
                    self.assertTrue(
                        self._wait_for(
                            lambda: ubwa.is_socket_ready(stream_id=stream_id)
                        )
                    )
                    response = ubwa.api.spot.get_server_time(
                        stream_id=stream_id, return_response=True
                    )
                    self.assertEqual(response["result"]["serverTime"], 1234567890123)
                finally:
                    ubwa.stop_manager()

    def test_keepalive_ping_timeout_reconnects(self):
        # A server that never answers client pings: with `ping_interval=1`,
        # `ping_timeout=1` both libraries must give up the connection and UBWA
        # must reconnect. The server side needs picows (`websockets` always
        # pongs), so this runs only when picows is installed.
        print(f"test_keepalive_ping_timeout_reconnects():")
        try:
            import picows
        except ImportError:
            self.skipTest("picows is not installed")

        ready = threading.Event()
        state = {"port": None}

        class Listener(picows.WSListener):
            def on_ws_connected(self, transport):
                transport.send(
                    picows.WSMsgType.TEXT,
                    b'{"stream":"silent@trade","data":{"e":"trade","t":0}}',
                )

            def on_ws_frame(self, transport, frame):
                if frame.msg_type == picows.WSMsgType.CLOSE:
                    transport.send_close(
                        frame.get_close_code(), frame.get_close_message()
                    )
                    transport.disconnect()
                elif frame.msg_type == picows.WSMsgType.TEXT:
                    request = orjson.loads(frame.get_payload_as_bytes())
                    transport.send(
                        picows.WSMsgType.TEXT,
                        orjson.dumps({"result": None, "id": request["id"]}),
                    )

        async def serve():
            server = await picows.ws_create_server(
                lambda request: Listener(), "127.0.0.1", 0, enable_auto_pong=False
            )
            state["port"] = server.sockets[0].getsockname()[1]
            ready.set()
            async with server:
                await server.serve_forever()

        threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()
        self.assertTrue(ready.wait(10))

        for library in self.libraries:
            with self.subTest(library=library):
                received = []
                ubwa = self._manager(
                    library,
                    websocket_base_uri=f"ws://127.0.0.1:{state['port']}/",
                    process_stream_data=received.append,
                    restart_timeout=1,
                )
                try:
                    stream_id = ubwa.create_stream(
                        ["trade"], ["silent"], ping_interval=1, ping_timeout=1
                    )
                    self.assertTrue(
                        self._wait_for(lambda: len(self._trades(received)) >= 1)
                    )
                    self.assertTrue(
                        self._wait_for(
                            lambda: ubwa.get_stream_info(stream_id)["reconnects"] >= 1,
                            15,
                        )
                    )
                finally:
                    ubwa.stop_manager()


if __name__ == "__main__":
    try:
        unittest.main()
    except KeyboardInterrupt:
        pass
