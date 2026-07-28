#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_plotting_last_price.py
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

import unicorn_binance_websocket_api.manager as ubwam
import datetime as dt

try:
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
except ImportError:
    print("Please install `matplotlib`! https://pypi.org/project/matplotlib")
    exit(1)

binance_websocket_api_manager = ubwam.BinanceWebSocketApiManager()
binance_websocket_api_manager.create_stream("trade", "btcusdt", output="UnicornFy")

xs = []
ys = []
title = "Live BTC Price @ Binance.com"
fig = plt.figure()
fig.canvas.set_window_title(title)
ax = fig.add_subplot(1, 1, 1)

print("Please wait a few seconds until enough data has been received!")


def animate(i, xs, ys):
    data = binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
    try:
        if data["stream_type"]:
            xs.append(dt.datetime.fromtimestamp(data["trade_time"] / 1000))
            ys.append(float(data["price"]))
            ax.clear()
            ax.plot(xs, ys)
            plt.xticks(rotation=45, ha="right")
            plt.subplots_adjust(bottom=0.30)
            plt.title(title)
            plt.ylabel("USDT Value")
    except KeyError:
        pass
    except TypeError:
        pass


ani = animation.FuncAnimation(fig, animate, fargs=(xs, ys), interval=5)
plt.show()
