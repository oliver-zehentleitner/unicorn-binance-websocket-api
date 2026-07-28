#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_binance_websocket_api import BinanceWebSocketApiManager

with BinanceWebSocketApiManager(warn_on_update=False) as ubwa:
    if ubwa.is_update_available():
        print(
            f"Please upgrade to {ubwa.get_latest_version()} you are on {ubwa.get_version()}"
        )
        latest_release_info = ubwa.get_latest_release_info()
        if latest_release_info:
            print(
                f"Please download the latest release or run `pip install unicorn-binance-websocket-api --upgrade`"
                f":\r\n\ttar: {latest_release_info['tarball_url']}\r\n\tzip: {latest_release_info['zipball_url']}\r\n"
                f"release info:\r\n{latest_release_info['body']}"
            )
    else:
        print(ubwa.get_version(), "is the latest version!")
