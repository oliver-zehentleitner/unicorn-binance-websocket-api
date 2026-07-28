#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯
#
# File: unicorn_binance_websocket_api/connection.py
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

from .exceptions import *
from urllib.parse import urlparse
import asyncio
import copy
import logging
import socks  # PySocks https://pypi.org/project/PySocks/
import sys
import websockets

__logger__: logging.getLogger = logging.getLogger("unicorn_binance_websocket_api")

logger = __logger__
connect: websockets.connect = websockets.connect


class BinanceWebSocketApiConnection(object):
    def __init__(self, manager, stream_id, channels, markets, symbols):
        self.manager = manager
        self.stream_id = copy.deepcopy(stream_id)
        self.api_key = copy.deepcopy(
            self.manager.stream_list[self.stream_id]["api_key"]
        )
        self.api_secret = copy.deepcopy(
            self.manager.stream_list[self.stream_id]["api_secret"]
        )
        self.ping_interval = copy.deepcopy(
            self.manager.stream_list[self.stream_id]["ping_interval"]
        )
        self.ping_timeout = copy.deepcopy(
            self.manager.stream_list[self.stream_id]["ping_timeout"]
        )
        self.close_timeout = copy.deepcopy(
            self.manager.stream_list[self.stream_id]["close_timeout"]
        )
        self.channels = copy.deepcopy(channels)
        self.markets = copy.deepcopy(markets)
        self.symbols = copy.deepcopy(symbols)
        self.websocket = None
        self.api = copy.deepcopy(self.manager.stream_list[self.stream_id]["api"])
        self.add_timeout = (
            True if "!userData" in f"{channels}{markets}" or self.api is True else False
        )
        self.timeout_disabled = False

    async def __aenter__(self):
        logger.debug(f"Entering with-context of BinanceWebSocketApiConnection() ...")
        self.raise_exceptions()
        uri = self.manager.create_websocket_uri(
            self.channels,
            self.markets,
            self.stream_id,
            symbols=self.symbols,
            api=self.manager.stream_list[self.stream_id]["api"],
        )
        if uri is None:
            # cant get a valid URI, so this stream has to crash
            error_msg = "Probably no internet connection?"
            logger.critical(
                f"BinanceWebSocketApiConnection.__aenter__(stream_id={self.stream_id}), channels="
                f"{self.channels}), markets={self.markets}) - error: 5 - {error_msg}"
            )
            self.manager.set_socket_is_ready(stream_id=self.stream_id)
            raise StreamIsRestarting(stream_id=self.stream_id, reason=error_msg)
        else:
            self.manager.stream_list[self.stream_id]["websocket_uri"] = uri
        try:
            if isinstance(uri, dict):
                # dict = error, string = valid url
                if (
                    uri["code"] == -1102
                    or uri["code"] == -2008
                    or uri["code"] == -2014
                    or uri["code"] == -2015
                    or uri["code"] == -11001
                ):
                    # -1102 = Mandatory parameter 'symbol' was not sent, was empty/null, or malformed.
                    # -2008 = Invalid Api-Key ID
                    # -2014 = API-key format invalid
                    # -2015 = Invalid API-key, IP, or permissions for action
                    # -11001 = Isolated margin account does not exist.
                    # Can not get a valid listen_key, so this stream has to crash:
                    logger.critical(
                        f"BinanceWebSocketApiConnection.__aenter__(stream_id={self.stream_id}), channels="
                        f"{self.channels}), markets={self.markets}) - error: 4 - Binance API: "
                        f"{str(uri['msg'])}"
                    )
                else:
                    logger.critical(
                        f"BinanceWebSocketApiConnection.__aenter__(stream_id={self.stream_id}), channels="
                        f"{self.channels}), markets={self.markets}) - error: 2 - Binance API: "
                        f"{str(uri['msg'])}"
                    )
                raise StreamIsCrashing(stream_id=self.stream_id, reason=uri["msg"])
        except KeyError as error_msg:
            logger.critical(
                f"BinanceWebSocketApiConnection.__aenter__(stream_id={self.stream_id}), "
                f"channels={self.channels}), markets={self.markets}) - error: 1 - "
                f"KeyError: {error_msg}"
            )
            print(f"KeyError: {error_msg}")
        if (
            self.manager.socks5_proxy_address is None
            or self.manager.socks5_proxy_port is None
        ):
            self._conn = connect(
                str(uri),
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                close_timeout=self.close_timeout,
                additional_headers={"User-Agent": str(self.manager.get_user_agent())},
            )
            logger.info(
                f"BinanceWebSocketApiConnection.__aenter__({self.stream_id}, {self.channels}"
                f", {self.markets}) - No proxy used!"
            )
        else:
            websocket_socks5_proxy = socks.socksocket()
            websocket_socks5_proxy.set_proxy(
                proxy_type=socks.SOCKS5,
                addr=self.manager.socks5_proxy_address,
                port=int(self.manager.socks5_proxy_port),
                username=self.manager.socks5_proxy_user,
                password=self.manager.socks5_proxy_pass,
            )
            netloc = urlparse(self.manager.websocket_base_uri).netloc
            try:
                host, port = netloc.split(":")
            except ValueError as error_msg:
                logger.debug(f"'netloc' split error: {netloc} - {error_msg}")
                host = netloc
                port = 443
            try:
                logger.info(
                    f"BinanceWebSocketApiConnection.__aenter__({self.stream_id}, {self.channels}"
                    f", {self.markets}) - Connect to socks5 proxy {host}:{port} (ssl_verification: "
                    f"{self.manager.socks5_proxy_ssl_verification})"
                )
                websocket_socks5_proxy.connect((host, int(port)))
                websocket_server_hostname = netloc
            except socks.ProxyConnectionError as error_msg:
                error_msg = f"{error_msg} ({host}:{port})"
                logger.critical(error_msg)
                raise Socks5ProxyConnectionError(error_msg)
            except socks.GeneralProxyError as error_msg:
                error_msg = f"{error_msg} ({host}:{port})"
                logger.critical(error_msg)
                raise Socks5ProxyConnectionError(error_msg)

            self._conn = connect(
                str(uri),
                ssl=self.manager.websocket_ssl_context,
                sock=websocket_socks5_proxy,
                server_hostname=websocket_server_hostname,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                close_timeout=self.close_timeout,
                additional_headers={"User-Agent": str(self.manager.get_user_agent())},
            )
            logger.info(
                f'BinanceWebSocketApiConnection.__aenter__("{self.stream_id}, {self.channels}'
                f', {self.markets}") - Using proxy: {self.manager.socks5_proxy_address} '
                f"{self.manager.socks5_proxy_port} SSL: {self.manager.socks5_proxy_ssl_verification}"
            )
        try:
            self.websocket = await self._conn.__aenter__()
        except asyncio.TimeoutError:
            self.manager.set_socket_is_ready(stream_id=self.stream_id)
            raise StreamIsRestarting(stream_id=self.stream_id, reason=f"timeout error")
        return self

    async def __aexit__(self, *args, **kwargs):
        logger.debug(
            f"Leaving asynchronous with-context of BinanceWebSocketApiConnection() ..."
        )
        self.manager.set_heartbeat(self.stream_id)
        await self._conn.__aexit__(*args, **kwargs)

    async def close(self):
        logger.info(f"BinanceWebSocketApiConnection.close({str(self.stream_id)})")
        self.manager.set_heartbeat(self.stream_id)
        return await self.websocket.close()

    async def receive(self):
        logger.debug(f"BinanceWebSocketApiConnection.receive({str(self.stream_id)})")
        self.raise_exceptions()
        if self.add_timeout:
            if self.api is True:
                timeout = 0.1
            else:
                timeout = 1
            received_data_json = await asyncio.wait_for(
                self.websocket.recv(), timeout=timeout
            )
        else:
            if (
                self.timeout_disabled is True
                and self.manager.stream_list[self.stream_id]["subscriptions"] != 0
            ):
                received_data_json = await self.websocket.recv()
            else:
                if (
                    self.manager.stream_list[self.stream_id]["processed_receives_total"]
                    > 10
                ):
                    self.timeout_disabled = True
                received_data_json = await asyncio.wait_for(
                    self.websocket.recv(), timeout=1
                )
        self.manager.set_heartbeat(self.stream_id)
        size = sys.getsizeof(str(received_data_json))
        self.manager.add_total_received_bytes(size)
        self.manager.increase_received_bytes_per_second(self.stream_id, size)
        self.manager.increase_processed_receives_statistic(self.stream_id)
        return received_data_json

    async def send(self, data):
        logger.debug(f"BinanceWebSocketApiConnection.send({str(self.stream_id)})")
        self.raise_exceptions()
        response = await self.websocket.send(data)
        self.manager.set_heartbeat(self.stream_id)
        self.manager.increase_transmitted_counter(self.stream_id)
        return response

    def raise_exceptions(self):
        if self.manager.is_stop_request(self.stream_id):
            raise StreamIsStopping(stream_id=self.stream_id, reason="stop request")
        if self.manager.is_crash_request(self.stream_id):
            raise StreamIsCrashing(stream_id=self.stream_id, reason="crash request")
