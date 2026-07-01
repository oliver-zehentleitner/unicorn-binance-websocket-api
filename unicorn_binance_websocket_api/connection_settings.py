#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: unicorn_binance_websocket_api/connection_settings.py
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

from enum import Enum
from typing import Type

MAX_SUBSCRIPTIONS_PER_STREAM: Type[int] = int
WEBSOCKET_BASE_URI: Type[str] = str
WEBSOCKET_API_BASE_URI: Type[str] = str


class Exchanges(str, Enum):
    BINANCE = "binance.com"
    BINANCE_TESTNET = "binance.com-testnet"
    BINANCE_MARGIN = "binance.com-margin"
    BINANCE_MARGIN_TESTNET = "binance.com-margin-testnet"
    BINANCE_ISOLATED_MARGIN = "binance.com-isolated_margin"
    BINANCE_ISOLATED_MARGIN_TESTNET = "binance.com-isolated_margin-testnet"
    BINANCE_FUTURES = "binance.com-futures"
    BINANCE_COIN_FUTURES = "binance.com-coin_futures"
    BINANCE_FUTURES_TESTNET = "binance.com-futures-testnet"
    BINANCE_VANILLA_OPTIONS = "binance.com-vanilla-options"
    BINANCE_VANILLA_OPTIONS_TESTNET = "binance.com-vanilla-options-testnet"
    BINANCE_PORTFOLIO_MARGIN = "binance.com-portfolio_margin"
    BINANCE_US = "binance.us"
    TRBINANCE = "trbinance.com"


CEX_EXCHANGES = [
    Exchanges.BINANCE,
    Exchanges.BINANCE_TESTNET,
    Exchanges.BINANCE_MARGIN,
    Exchanges.BINANCE_MARGIN_TESTNET,
    Exchanges.BINANCE_ISOLATED_MARGIN,
    Exchanges.BINANCE_ISOLATED_MARGIN_TESTNET,
    Exchanges.BINANCE_FUTURES,
    Exchanges.BINANCE_COIN_FUTURES,
    Exchanges.BINANCE_FUTURES_TESTNET,
    Exchanges.BINANCE_VANILLA_OPTIONS,
    Exchanges.BINANCE_VANILLA_OPTIONS_TESTNET,
    Exchanges.BINANCE_PORTFOLIO_MARGIN,
    Exchanges.BINANCE_US,
    Exchanges.TRBINANCE,
]

# only python 3.9+
# CONNECTION_SETTINGS: dict[str, Tuple[MAX_SUBSCRIPTIONS_PER_STREAM, WEBSOCKET_BASE_URI, WEBSOCKET_API_BASE_URI]] = {

CONNECTION_SETTINGS = {
    Exchanges.BINANCE: (1024, "wss://stream.binance.com:9443/", "wss://ws-api.binance.com/ws-api/v3"),
    Exchanges.BINANCE_TESTNET: (1024, "wss://testnet.binance.vision/", "wss://testnet.binance.vision/ws-api/v3"),
    Exchanges.BINANCE_MARGIN: (1024, "wss://stream.binance.com:9443/", "wss://ws-api.binance.com/ws-api/v3"),
    Exchanges.BINANCE_MARGIN_TESTNET: (1024, "wss://testnet.binance.vision/", "wss://testnet.binance.vision/ws-api/v3"),
    Exchanges.BINANCE_ISOLATED_MARGIN: (1024, "wss://stream.binance.com:9443/", "wss://ws-api.binance.com/ws-api/v3"),
    Exchanges.BINANCE_ISOLATED_MARGIN_TESTNET: (1024, "wss://testnet.binance.vision/", "wss://testnet.binance.vision/ws-api/v3"),
    Exchanges.BINANCE_FUTURES: (200, "wss://fstream.binance.com/", "wss://ws-fapi.binance.com/ws-fapi/v1"),
    Exchanges.BINANCE_FUTURES_TESTNET: (200, "wss://stream.binancefuture.com/", "wss://testnet.binancefuture.com/ws-fapi/v1"),
    Exchanges.BINANCE_COIN_FUTURES: (200, "wss://dstream.binance.com/", None),
    Exchanges.BINANCE_VANILLA_OPTIONS: (200, "wss://fstream.binance.com/public/", None),
    Exchanges.BINANCE_VANILLA_OPTIONS_TESTNET: (200, "wss://fstream.binancefuture.com/public/", None),
    Exchanges.BINANCE_PORTFOLIO_MARGIN: (200, "wss://fstream.binance.com/pm/", None),
    Exchanges.BINANCE_US: (1024, "wss://stream.binance.us:9443/", None),
    Exchanges.TRBINANCE: (1024, "wss://stream-cloud.trbinance.com/", None),
}

# Exchanges that use the new WS API userDataStream subscription flow (userDataStream.subscribe.signature)
# instead of the legacy REST listenKey approach (POST /api/v3/userDataStream).
# Binance deprecated and removed the listenKey REST endpoints for Spot and Margin in February 2026.
USERDATA_WS_API_EXCHANGES = frozenset([
    Exchanges.BINANCE,
    Exchanges.BINANCE_TESTNET,
    Exchanges.BINANCE_MARGIN,
    Exchanges.BINANCE_MARGIN_TESTNET,
    Exchanges.BINANCE_ISOLATED_MARGIN,
    Exchanges.BINANCE_ISOLATED_MARGIN_TESTNET,
])

# Exchanges that route USDT-M Futures WebSocket streams via the per-category base
# paths /public, /market, /private (Binance announcement effective 2026-04-23).
# BINANCE_PORTFOLIO_MARGIN is intentionally NOT part of this set: its user data
# stream keeps the legacy `/pm/ws/<listenKey>` path form (no /private sub-path,
# no `&events=` query filter), so it must fall through to the `else` branch in
# BinanceWebSocketApiManager.create_websocket_uri().
BINANCE_FUTURES_EXCHANGES = frozenset([
    Exchanges.BINANCE_FUTURES,
    Exchanges.BINANCE_FUTURES_TESTNET,
])

# UBWA-internal channel/market suffix markers — tokens that may appear in `channels`
# or `markets` but do not pin a Futures WS category (e.g. `arr` as in `!ticker@arr`,
# or `$all` for the full-symbol form). Used by Futures category resolution to skip
# these tokens when classifying.
FUTURES_SUFFIX_MARKERS = frozenset({"arr", "$all"})

# Default event subscription for the /private listenKey WebSocket on USDT-M Futures.
# Since 2026-04-23 the private endpoint requires an explicit `&events=` filter — if
# omitted, Binance production silently delivers nothing. To preserve the behaviour of
# the previous `/ws/<listenKey>` URL (which streamed every event), UBWA subscribes to
# all event types listed below by default. The user can override this with the
# `events` parameter on `create_stream()` / `replace_stream()`.
#
# The list mirrors the event types documented at
# https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams
# as of UBWA's release date. UBWA does not validate user-supplied event names against
# this list — Binance may add new events between releases and they can be passed
# through immediately without waiting for a UBWA bump.
BINANCE_FUTURES_USERDATA_EVENTS = (
    "ORDER_TRADE_UPDATE",
    "ACCOUNT_UPDATE",
    "MARGIN_CALL",
    "TRADE_LITE",
    "ACCOUNT_CONFIG_UPDATE",
    "STRATEGY_UPDATE",
    "GRID_UPDATE",
    "CONDITIONAL_ORDER_TRIGGER_REJECT",
    "ALGO_ORDER_UPDATE",
    "listenKeyExpired",
)
