#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_pandas_ta-lib.py
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

try:
    import pandas as pd
except ImportError:
    print("Please install `pandas`!")
    exit(1)
try:
    import pandas_ta as ta
except ImportError:
    print("Please install `pandas_ta`!")
    exit(1)


data_list = []
min_items = 11

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)

ubwa = BinanceWebSocketApiManager(exchange="binance.com", output_default="UnicornFy")
ubwa.create_stream("kline_1m", "btcusdt")

print(
    f"For our calculations we need the klines_1m of the last 10 minutes. Normally we would download the history\r\n"
    f"via REST API. In this demo, we wait 10 minutes for the websocket connection to receive 10 klines and then\r\n"
    f"output the entire dataframe to the console every minute from then on."
)
print(f"Learn about pandas-ta: https://twopirllc.github.io/pandas-ta")

while True:
    ohlcv = False
    data = ubwa.pop_stream_data_from_stream_buffer()
    if data is False:
        time.sleep(0.1)
    else:
        try:
            # Use only the closing kline
            if data["kline"]["is_closed"]:
                data_list.append(
                    [
                        int(data["event_time"] / 1000),
                        data["kline"]["open_price"],
                        data["kline"]["high_price"],
                        data["kline"]["low_price"],
                        data["kline"]["close_price"],
                        data["kline"]["base_volume"],
                    ]
                )
                if len(data_list) < min_items:
                    print(
                        f"Please wait, we need {min_items - len(data_list)} more klines!"
                    )
                else:
                    # We create a completely new dataframe with each loop. There is also the possibility to work with
                    # `pd.concat()` or `df.append()`:
                    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.append.html
                    df = pd.DataFrame(
                        data_list,
                        columns=["time", "open", "high", "low", "close", "volume"],
                    )
                    # Now we extend the array with new columns, add the date and the values of the RSI and SMA
                    df["date"] = pd.to_datetime(df["time"], unit="s")
                    df["rsi"] = ta.rsi(close=df.close, length=10)
                    df["sma10"] = ta.sma(df.close, length=10)
                    print(
                        f"Extended dataframe with RSI ({df['rsi'].iloc[-1]}) and SMA10 ({df['sma10'].iloc[-1]}) for "
                        f"symbol '{data['symbol']}':\r\n{df}\r\n"
                    )
        except TypeError as error_msg:
            print("TypeError: " + str(error_msg))
        except KeyError as error_msg:
            if "kline" not in str(error_msg):
                print("KeyError: " + str(error_msg))
