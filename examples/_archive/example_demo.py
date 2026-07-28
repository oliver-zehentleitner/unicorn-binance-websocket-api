#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_demo.py
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

import unicorn_binance_websocket_api
import logging
import os
import time
import threading

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)

ubwa = unicorn_binance_websocket_api.BinanceWebSocketApiManager()


def print_stream_data_from_stream_buffer(binance_websocket_api_manager):
    while True:
        if binance_websocket_api_manager.is_manager_stopping():
            exit(0)
        oldest_stream_data_from_stream_buffer = (
            binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        )
        if oldest_stream_data_from_stream_buffer is None:
            time.sleep(0.01)
        else:
            try:
                # remove # to activate the print function:
                print(oldest_stream_data_from_stream_buffer)
            except Exception:
                # not able to process the data? write it back to the stream_buffer
                binance_websocket_api_manager.add_to_stream_buffer(
                    oldest_stream_data_from_stream_buffer
                )


# start a worker process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(ubwa,)
)
worker_thread.start()

print(
    "\r\n========================================== Starting aggTrade ==========================================\r\n"
)
# start

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
    "bchabcbusd",
    "ltcbnb",
    "pivxeth",
    "skybtc",
    "tntbtc",
    "poebtc",
    "steembtc",
    "icxusdt",
    "tfuelbtc",
    "chzbtc",
    "vibeth",
    "winusdc",
    "gtobtc",
    "linkusdc",
    "batbusd",
    "rdnbtc",
    "dataeth",
    "bttpax",
    "zrxbnb",
    "vibbtc",
    "neobnb",
    "cosbtc",
    "powreth",
    "rlcusdt",
    "hbarbnb",
    "wabieth",
    "bqxeth",
    "aionbtc",
    "aeeth",
    "mthbtc",
    "wrxbtc",
    "pptbtc",
    "nknbtc",
    "zecusdt",
    "stormeth",
    "qtumusdt",
]

aggtrade_stream_id = ubwa.create_stream(["aggTrade"], markets)
time.sleep(7)
# stop
ubwa.stop_stream(aggtrade_stream_id)
time.sleep(2)
print(
    "\r\n=========================================== Stopped aggTrade ==========================================\r\n"
)

print(
    "\r\n====================================== Starting trade and kline_1m ====================================\r\n"
)
trade_stream_id = ubwa.create_stream(["trade"], markets)
kline_1m_stream_id = ubwa.create_stream("kline_1m", markets)
time.sleep(7)
ubwa.stop_stream(trade_stream_id)
ubwa.stop_stream(kline_1m_stream_id)
time.sleep(2)
print(
    "\r\n====================================== Stopped trade and kline_1m =====================================\r\n"
)

print(
    "\r\n======================================== Starting ticker ==============================================\r\n"
)
ticker_bnbbtc_stream_id = ubwa.create_stream(["ticker"], markets)
time.sleep(7)
ubwa.stop_stream(ticker_bnbbtc_stream_id)
time.sleep(2)
print(
    "\r\n======================================== Stopped ticker ===============================================\r\n"
)

print(
    "\r\n========================================== Starting miniticker ========================================\r\n"
)
miniticker_stream_id = ubwa.create_stream(["miniTicker"], markets)
time.sleep(7)
ubwa.stop_stream(miniticker_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped miniticker  =========================================\r\n"
)

print(
    "\r\n========================================== Starting kline_5m ==========================================\r\n"
)
kline_5m_stream_id = ubwa.create_stream(["kline_5m"], markets)
time.sleep(7)
ubwa.stop_stream(kline_5m_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped kline_5m  ===========================================\r\n"
)

print(
    "\r\n=========================================== Starting depth5 ===========================================\r\n"
)
depth5_stream_id = ubwa.create_stream(["depth5"], markets)
time.sleep(7)
ubwa.stop_stream(depth5_stream_id)
time.sleep(2)
print(
    "\r\n========================================== Stopped depth5  ============================================\r\n"
)

print(
    "\r\n========================================== Starting depth =============================================\r\n"
)
depth_stream_id = ubwa.create_stream(["depth"], markets)
time.sleep(7)
ubwa.stop_stream(depth_stream_id)
time.sleep(2)
print(
    "\r\n============================================ Stopped depth  ===========================================\r\n"
)

print(
    "\r\n========================================== Starting !miniticker ========================================\r\n"
)
miniticker_stream_id = ubwa.create_stream(["arr"], ["!miniTicker"])
time.sleep(7)
ubwa.stop_stream(miniticker_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped !miniticker  =========================================\r\n"
)

print(
    "\r\n========================================== Starting ticker all ========================================\r\n"
)
ticker_all_stream_id = ubwa.create_stream(["arr"], ["!ticker"])
time.sleep(7)
ubwa.stop_stream(ticker_all_stream_id)
time.sleep(2)
print(
    "\r\n=========================================== Stopped ticker all ========================================\r\n"
)

print(
    "\r\n=================================== Starting multi multi socket =======================================\r\n"
)
channels = {
    "trade",
    "kline_1",
    "kline_5",
    "kline_15",
    "kline_30",
    "kline_1h",
    "kline_12h",
    "kline_1w",
    "miniTicker",
    "depth20",
}
print(channels)
print(markets, "\r\n")
time.sleep(3)
multi_multi_stream_id = ubwa.create_stream(channels, markets)
time.sleep(3)
ubwa.stop_stream(multi_multi_stream_id)
time.sleep(2)
print(
    "\r\n================================== Stopped multi multi socket  ========================================\r\n"
)

print(
    "\r\n============================= Starting multi multi socket subscribe ===================================\r\n"
)
channels = {
    "trade",
    "kline_1",
    "kline_5",
    "kline_15",
    "kline_30",
    "kline_1h",
    "kline_12h",
    "kline_1w",
    "miniTicker",
    "depth20",
    "!miniTicker",
    "!ticker",
}
multi_multi_stream_id = ubwa.create_stream(channels, markets)
time.sleep(5)
ubwa.stop_stream(multi_multi_stream_id)
time.sleep(2)
print(
    "\r\n============================== Stopped multi multi socket subscribe ===================================\r\n"
)


print(
    "\r\n=============================== Stopping BinanceWebSocketManager ======================================\r\n"
)
ubwa.stop_manager_with_all_streams()
print("finished!")
