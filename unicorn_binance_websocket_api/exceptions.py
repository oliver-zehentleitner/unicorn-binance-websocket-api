#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯
#
# File: unicorn_binance_websocket_api/exception.py
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


class MaximumSubscriptionsExceeded(Exception):
    """
    Exception if the maximum number of subscriptions per stream has been exceeded!
    """

    def __init__(self, exchange: str = None, max_subscriptions_per_stream: int = None):
        self.message = (
            f"The maximum number of {max_subscriptions_per_stream} subscriptions per stream for exchange "
            f"'{exchange}' has been exceeded! For detailed information please have a look at our wiki: "
            f"https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/Binance-"
            f"websocket-endpoint-configuration-overview"
        )
        super().__init__(self.message)


class Socks5ProxyConnectionError(Exception):
    """
    Exception if the manager class is not able to establish a connection to the socks5 proxy.
    """

    pass


class StreamIsCrashing(Exception):
    """
    Exception if the stream is crashing.
    """

    def __init__(self, stream_id=None, reason=None):
        self.message = (
            f"Stream with stream_id={stream_id} is crashing! Reason: {reason}"
        )
        super().__init__(self.message)


class StreamIsRestarting(Exception):
    """
    Exception if the stream is restarting.
    """

    def __init__(self, stream_id=None, reason=None):
        self.message = (
            f"Stream with stream_id={stream_id} is restarting! Reason: {reason}"
        )
        super().__init__(self.message)


class StreamIsStopping(Exception):
    """
    Exception if the stream is stopping.
    """

    def __init__(self, stream_id=None, reason=None):
        self.message = (
            f"Stream with stream_id={stream_id} is stopping! Reason: {reason}"
        )
        super().__init__(self.message)


class UnknownExchange(Exception):
    """
    Exception if the manager class is started with an unknown exchange.
    """

    def __init__(self, error_msg=None):
        super().__init__(error_msg)
