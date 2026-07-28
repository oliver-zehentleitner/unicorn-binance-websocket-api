#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dev/test_cex_full.py
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
                print(oldest_stream_data_from_stream_buffer)
            except Exception:
                # not able to process the data? write it back to the stream_buffer
                binance_websocket_api_manager.add_to_stream_buffer(
                    oldest_stream_data_from_stream_buffer
                )


# create instance of BinanceWebSocketApiManager and provide the function for stream processing
binance_websocket_api_manager = BinanceWebSocketApiManager()

# start a worker to process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(
    target=print_stream_data_from_stream_buffer, args=(binance_websocket_api_manager,)
)
worker_thread.start()

print("testing ws/ single streams")

print(
    "\r\n========================================== Starting ticker all ========================================\r\n"
)
ticker_all_stream_id = binance_websocket_api_manager.create_stream(["arr"], ["!ticker"])
time.sleep(6)
binance_websocket_api_manager.stop_stream(ticker_all_stream_id)
time.sleep(2)
print(
    "\r\n=========================================== Stopped ticker all ========================================\r\n"
)

print(
    "\r\n========================================= Starting !miniticker ========================================\r\n"
)
miniticker_stream_id = binance_websocket_api_manager.create_stream(
    ["arr"], ["!miniTicker"]
)
time.sleep(5)
binance_websocket_api_manager.stop_stream(miniticker_stream_id)
time.sleep(2)
print(
    "\r\n======================================== Stopped !miniticker  =========================================\r\n"
)

print(
    "\r\n========================================== Starting !userData ========================================\r\n"
)
userdata_stream_id = binance_websocket_api_manager.create_stream(["arr"], ["!userData"])
time.sleep(5)
binance_websocket_api_manager.stop_stream(userdata_stream_id)
time.sleep(2)
print(
    "\r\n========================================== Stopped !userData ==========================================\r\n"
)

print("\r\n\r\nTesting multi streams with just one market")
print(
    "\r\n========================================== Starting aggTrade ==========================================\r\n"
)
markets = {"bnbbtc"}
aggtrade_stream_id = binance_websocket_api_manager.create_stream(["aggTrade"], markets)
time.sleep(5)
binance_websocket_api_manager.stop_stream(aggtrade_stream_id)
time.sleep(2)
print(
    "\r\n=========================================== Stopped aggTrade ==========================================\r\n"
)

print(
    "\r\n====================================== Starting trade and kline_1m ====================================\r\n"
)
trade_stream_id = binance_websocket_api_manager.create_stream(["trade"], markets)
kline_1m_stream_id = binance_websocket_api_manager.create_stream(["kline_1m"], markets)
time.sleep(5)
binance_websocket_api_manager.stop_stream(trade_stream_id)
binance_websocket_api_manager.stop_stream(kline_1m_stream_id)
time.sleep(2)
print(
    "\r\n====================================== Stopped trade and kline_1m =====================================\r\n"
)

print(
    "\r\n======================================== Starting ticker BNB/BTC ======================================\r\n"
)
ticker_bnbbtc_stream_id = binance_websocket_api_manager.create_stream(
    ["ticker"], markets
)
time.sleep(5)
binance_websocket_api_manager.stop_stream(ticker_bnbbtc_stream_id)
time.sleep(2)
print(
    "\r\n======================================== Stopped ticker BNB/BTC =======================================\r\n"
)

print(
    "\r\n========================================== Starting miniticker ========================================\r\n"
)
miniticker_stream_id = binance_websocket_api_manager.create_stream(
    ["miniTicker"], markets
)
time.sleep(5)
binance_websocket_api_manager.stop_stream(miniticker_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped miniticker  =========================================\r\n"
)

print(
    "\r\n========================================== Starting kline_5m ==========================================\r\n"
)
kline_5m_stream_id = binance_websocket_api_manager.create_stream(["kline_5m"], markets)
time.sleep(5)
binance_websocket_api_manager.stop_stream(kline_5m_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped kline_5m  ===========================================\r\n"
)

print(
    "\r\n=========================================== Starting depth10 ===========================================\r\n"
)
depth5_stream_id = binance_websocket_api_manager.create_stream(["depth10"], markets)
time.sleep(5)
binance_websocket_api_manager.stop_stream(depth5_stream_id)
time.sleep(2)
print(
    "\r\n========================================== Stopped depth5  ============================================\r\n"
)

print(
    "\r\n========================================== Starting depth =============================================\r\n"
)
depth_stream_id = binance_websocket_api_manager.create_stream(["depth"], markets)
time.sleep(4)
binance_websocket_api_manager.stop_stream(depth_stream_id)
time.sleep(2)
print(
    "\r\n============================================ Stopped depth  ===========================================\r\n"
)

print("\r\n\r\nTesting multi markets with one stream_type")
markets = {
    "xrpusdt",
    "rvnbtc",
    "ltcusdt",
    "adausdt",
    "eosusdt",
    "neousdt",
    "btcusdt",
    "bnbusdt",
}
print(
    "\r\n========================================== Starting aggTrade ==========================================\r\n"
)
aggtrade_stream_id = binance_websocket_api_manager.create_stream(["aggTrade"], markets)
time.sleep(3)
binance_websocket_api_manager.stop_stream(aggtrade_stream_id)
time.sleep(2)
print(
    "\r\n=========================================== Stopped aggTrade ==========================================\r\n"
)

print(
    "\r\n====================================== Starting trade and kline_1m ====================================\r\n"
)
trade_stream_id = binance_websocket_api_manager.create_stream(["trade"], markets)
kline_1m_stream_id = binance_websocket_api_manager.create_stream(["kline_1m"], markets)
time.sleep(3)
binance_websocket_api_manager.stop_stream(trade_stream_id)
binance_websocket_api_manager.stop_stream(kline_1m_stream_id)
time.sleep(2)
print(
    "\r\n====================================== Stopped trade and kline_1m =====================================\r\n"
)

print(
    "\r\n======================================= Starting ticker ===============================================\r\n"
)
ticker_bnbbtc_stream_id = binance_websocket_api_manager.create_stream(
    ["ticker"], markets
)
time.sleep(4)
binance_websocket_api_manager.stop_stream(ticker_bnbbtc_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped ticker ==============================================\r\n"
)

print(
    "\r\n========================================== Starting miniticker ========================================\r\n"
)
miniticker_stream_id = binance_websocket_api_manager.create_stream(
    ["miniTicker"], markets
)
time.sleep(4)
binance_websocket_api_manager.stop_stream(miniticker_stream_id)
time.sleep(2)
print(
    "\r\n========================================== Stopped miniticker  ========================================\r\n"
)

print(
    "\r\n========================================== Starting kline_5m ==========================================\r\n"
)
kline_5m_stream_id = binance_websocket_api_manager.create_stream(["kline_5m"], markets)
time.sleep(4)
binance_websocket_api_manager.stop_stream(kline_5m_stream_id)
time.sleep(2)
print(
    "\r\n========================================= Stopped kline_5m  ===========================================\r\n"
)

print(
    "\r\n=========================================== Starting depth5 ===========================================\r\n"
)
depth5_stream_id = binance_websocket_api_manager.create_stream(["depth5"], markets)
time.sleep(4)
binance_websocket_api_manager.stop_stream(depth5_stream_id)
time.sleep(2)
print(
    "\r\n========================================== Stopped depth5  ============================================\r\n"
)

print(
    "\r\n========================================== Starting depth =============================================\r\n"
)
depth_stream_id = binance_websocket_api_manager.create_stream(["depth"], markets)
time.sleep(4)
binance_websocket_api_manager.stop_stream(depth_stream_id)
time.sleep(2)
print(
    "\r\n============================================ Stopped depth  ===========================================\r\n"
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
    "!miniTicker",
    "!ticker",
}
multi_multi_stream_id = binance_websocket_api_manager.create_stream(channels, markets)
time.sleep(4)
binance_websocket_api_manager.stop_stream(multi_multi_stream_id)
time.sleep(2)
print(
    "\r\n================================== Stopped multi multi socket  ========================================\r\n"
)

# print summary
binance_websocket_api_manager.print_summary()
print(
    "\r\n=============================== Stopping BinanceWebSocketManager ======================================\r\n"
)
binance_websocket_api_manager.stop_manager_with_all_streams()
time.sleep(7)

stream_list = binance_websocket_api_manager.get_stream_list()

for stream_id in stream_list:
    binance_websocket_api_manager.print_stream_info(stream_id)
    time.sleep(3.5)
print("finished!")
