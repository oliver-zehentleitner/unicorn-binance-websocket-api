#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯
#
# File: unicorn_binance_websocket_api/restclient.py
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

from unicorn_binance_rest_api import BinanceRestApiManager
from unicorn_binance_rest_api.exceptions import UnknownExchange
from typing import Optional, Union, Tuple
import logging
import requests
import threading
import time

__logger__: logging.getLogger = logging.getLogger("unicorn_binance_websocket_api")

logger = __logger__


class BinanceWebSocketApiRestclient(object):
    def __init__(self,
                 debug: Optional[bool] = False,
                 disable_colorama: Optional[bool] = False,
                 exchange: Optional[str] = "binance.com",
                 restful_base_uri: Optional[str] = None,
                 show_secrets_in_logs: Optional[bool] = False,
                 socks5_proxy_server: Optional[str] = None,
                 socks5_proxy_user: Optional[str] = None,
                 socks5_proxy_pass: Optional[str] = None,
                 socks5_proxy_ssl_verification: Optional[bool] = True,
                 stream_list: dict = None,
                 ubra: BinanceRestApiManager = None,
                 warn_on_update: Optional[bool] = True):
        """
        Create a restclient instance!

        """
        self.threading_lock = threading.Lock()
        self.debug = debug
        self.disable_colorama = disable_colorama
        self.exchange = exchange
        self.restful_base_uri = restful_base_uri
        self.show_secrets_in_logs = show_secrets_in_logs
        self.socks5_proxy_server = socks5_proxy_server
        self.socks5_proxy_user = socks5_proxy_user
        self.socks5_proxy_pass = socks5_proxy_pass
        self.socks5_proxy_ssl_verification = socks5_proxy_ssl_verification
        self.stream_list = stream_list
        self.ubra = ubra
        self.warn_on_update = warn_on_update
        self.sigterm = False

    def _init_ubra(self) -> bool:
        """
        Init UBRA if necessary.

        :return: `True` if `self.ubra` is ready to use, `False` if the installed UBRA version
                 does not (yet) know `self.exchange` - callers must check this and bail out
                 gracefully instead of letting `UnknownExchange` propagate uncaught.
        """
        if self.ubra is None:
            logger.debug(f"Init UBRA for UBWA restclient.")
            try:
                self.ubra = BinanceRestApiManager(debug=self.debug,
                                                  disable_colorama=self.disable_colorama,
                                                  exchange=self.exchange,
                                                  socks5_proxy_server=self.socks5_proxy_server,
                                                  socks5_proxy_user=self.socks5_proxy_user,
                                                  socks5_proxy_pass=self.socks5_proxy_pass,
                                                  warn_on_update=self.warn_on_update)
            except UnknownExchange as error_msg:
                logger.critical(f"BinanceWebSocketApiRestclient._init_ubra() - The installed "
                                f"`unicorn-binance-rest-api` version does not support "
                                f"exchange='{self.exchange}' yet - please upgrade UBRA - "
                                f"error_msg: {error_msg}")
                return False
        return True

    def delete_listen_key(self, stream_id=None) -> Tuple[Union[str, None], Union[dict, None]]:
        """
        Delete a specific listen key

        :param stream_id: provide a stream_id
        :type stream_id: str

        :return: listen_key, binance_api_status
        :rtype: Tuple[Union[str, None], Union[dict, None]]
        """
        logger.info(f"BinanceWebSocketApiRestclient.delete_listen_key() stream_id='{str(stream_id)}')")

        if stream_id is None:
            return None, None

        try:
            with (self.threading_lock):
                if self._init_ubra() is False:
                    return None, None

                try:
                    kwargs = {'api_key': self.stream_list[stream_id]['api_key'],
                              'api_secret': self.stream_list[stream_id]['api_secret']}
                except TypeError:
                    logger.critical(f"delete_listen_key(stream_id='{str(stream_id)}') - No API key available!")
                    return None, None

                if self.exchange == "binance.com-margin" or \
                        self.exchange == "binance.com-margin-testnet":
                    if self.restful_base_uri is not None:
                        self.ubra.MARGIN_API_URL = self.restful_base_uri
                    result = self.ubra.margin_stream_close(listenKey=self.stream_list[stream_id]['listen_key'],
                                                           throw_exception=False,
                                                           **kwargs)
                elif self.exchange == "binance.com-isolated_margin" or \
                        self.exchange == "binance.com-isolated_margin-testnet":
                    if self.restful_base_uri is not None:
                        self.ubra.MARGIN_API_URL = self.restful_base_uri
                    result = self.ubra.isolated_margin_stream_close(symbol=self.stream_list[stream_id]['symbols'],
                                                                    listenKey=self.stream_list[stream_id]['listen_key'],
                                                                    throw_exception=False,
                                                                    **kwargs)
                elif self.exchange == "binance.com-futures" or self.exchange == "binance.com-futures-testnet":
                    if self.restful_base_uri is not None:
                        self.ubra.FUTURES_URL = self.restful_base_uri
                    result = self.ubra.futures_stream_close(listenKey=self.stream_list[stream_id]['listen_key'],
                                                            throw_exception=False,
                                                            **kwargs)
                elif self.exchange == "binance.com-coin_futures":
                    if self.restful_base_uri is not None:
                        self.ubra.FUTURES_COIN_URL = self.restful_base_uri
                    result = self.ubra.futures_coin_stream_close(listenKey=self.stream_list[stream_id]['listen_key'],
                                                                 throw_exception=False,
                                                                 **kwargs)
                elif self.exchange == "binance.com-portfolio_margin":
                    if self.restful_base_uri is not None:
                        self.ubra.PAPI_URL = self.restful_base_uri
                    result = self.ubra.portfolio_margin_stream_close(listenKey=self.stream_list[stream_id]['listen_key'],
                                                                     throw_exception=False,
                                                                     **kwargs)
                else:
                    if self.restful_base_uri is not None:
                        self.ubra.API_URL = self.restful_base_uri
                    result = self.ubra.stream_close(listenKey=self.stream_list[stream_id]['listen_key'],
                                                    throw_exception=False,
                                                    **kwargs)
        except requests.exceptions.ReadTimeout as error_msg:
            logger.debug(f"BinanceWebSocketApiManager.delete_listen_key_by_stream_id() - Not able to delete "
                         f"listen_key - requests.exceptions.ReadTimeout: {error_msg}")

        self.stream_list[stream_id]['listen_key'] = None

        return result, self.get_binance_api_status()

    def get_binance_api_status(self) -> dict:
        """
        Get the used weight of the last api request, with a current timestamp.

        :return: binance_api_status
        :rtype: dict
        """
        binance_api_status = self.ubra.get_used_weight()
        weight = binance_api_status['weight']
        binance_api_status['weight'] = 0 if weight is None else weight
        binance_api_status['timestamp'] = time.time()
        return binance_api_status

    def get_listen_key(self, stream_id=None) -> Tuple[Union[dict, None], Union[dict, None]]:
        """
        Request a valid listen_key from binance

        :param stream_id: provide a stream_id
        :type stream_id: str

        :return: response, binance_api_status
        :rtype: Tuple[Union[dict, None], Union[dict, None]]
        """
        logger.info(f"BinanceWebSocketApiRestclient.get_listen_key() symbol='{self.stream_list[stream_id]['symbols']}' "
                    f"stream_id='{str(stream_id)}')")

        if stream_id is None:
            return None, None

        with (self.threading_lock):
            if self._init_ubra() is False:
                return None, None

            try:
                kwargs = {'api_key': self.stream_list[stream_id]['api_key'],
                          'api_secret': self.stream_list[stream_id]['api_secret']}
            except TypeError as error_msg:
                logger.debug(f"delete_listen_key(stream_id='{str(stream_id)}') - TypeError: {error_msg}")
                return None, None

            if self.exchange == "binance.com-margin" or self.exchange == "binance.com-margin-testnet":
                try:
                    if self.restful_base_uri is not None:
                        self.ubra.MARGIN_API_URL = self.restful_base_uri
                    response = self.ubra.margin_stream_get_listen_key(output="raw_data",
                                                                      throw_exception=False,
                                                                      **kwargs)
                except AttributeError as error_msg:
                    logger.critical(f"BinanceWebSocketApiRestclient.get_listen_key() - error: 8 - "
                                    f"error_msg: {error_msg} - Can not acquire listen_key for margin!")
                    return None, None
            elif self.exchange == "binance.com-isolated_margin" or \
                    self.exchange == "binance.com-isolated_margin-testnet":
                if self.stream_list[stream_id]['symbols'] is None:
                    logger.critical("BinanceWebSocketApiRestclient.get_listen_key() - error_msg: Parameter "
                                    "`symbol` is missing!")
                    return None, None
                else:
                    try:
                        if self.restful_base_uri is not None:
                            self.ubra.MARGIN_API_URL = self.restful_base_uri
                        symbols = self.stream_list[stream_id]['symbols']
                        response = self.ubra.isolated_margin_stream_get_listen_key(symbol=symbols,
                                                                                   output="raw_data",
                                                                                   throw_exception=False,
                                                                                   **kwargs)
                    except AttributeError as error_msg:
                        logger.critical(f"BinanceWebSocketApiRestclient.get_listen_key() - error: 8 - "
                                        f"error_msg: {error_msg} - Can not acquire listen_key for isolated_margin!")
                        return None, None
            elif self.exchange == "binance.com-futures" or self.exchange == "binance.com-futures-testnet":
                try:
                    if self.restful_base_uri is not None:
                        self.ubra.FUTURES_URL = self.restful_base_uri
                    response = self.ubra.futures_stream_get_listen_key(output="raw_data",
                                                                       throw_exception=False,
                                                                       **kwargs)
                except AttributeError as error_msg:
                    logger.critical(f"BinanceWebSocketApiRestclient.get_listen_key() - error: 8 - "
                                    f"error_msg: {error_msg} - Can not acquire listen_key for futures!!")
                    return None, None
            elif self.exchange == "binance.com-coin_futures":
                try:
                    if self.restful_base_uri is not None:
                        self.ubra.FUTURES_COIN_URL = self.restful_base_uri
                    response = self.ubra.futures_coin_stream_get_listen_key(output="raw_data",
                                                                            throw_exception=False,
                                                                            **kwargs)
                except AttributeError as error_msg:
                    logger.critical(f"BinanceWebSocketApiRestclient.get_listen_key() - error: 8 - "
                                    f"error_msg: {error_msg} - Can not acquire listen_key for coin futures!!")
                    return None, None
            elif self.exchange == "binance.com-portfolio_margin":
                try:
                    if self.restful_base_uri is not None:
                        self.ubra.PAPI_URL = self.restful_base_uri
                    response = self.ubra.portfolio_margin_stream_get_listen_key(output="raw_data",
                                                                                throw_exception=False,
                                                                                **kwargs)
                except AttributeError as error_msg:
                    logger.critical(f"BinanceWebSocketApiRestclient.get_listen_key() - error: 8 - "
                                    f"error_msg: {error_msg} - Can not acquire listen_key for portfolio margin!!")
                    return None, None
            else:
                try:
                    if self.restful_base_uri is not None:
                        self.ubra.API_URL = self.restful_base_uri
                    response = self.ubra.stream_get_listen_key(output="raw_data",
                                                               throw_exception=False,
                                                               **kwargs)
                except AttributeError as error_msg:
                    logger.critical(f"BinanceWebSocketApiRestclient.get_listen_key() - error: 8 - "
                                    f"error_msg: {error_msg} - Can not acquire listen_key for exchange='"
                                    f"{self.exchange}'!")
                    return None, None

            try:
                self.stream_list[stream_id]['listen_key'] = response['listenKey']
                self.stream_list[stream_id]['last_static_ping_listen_key'] = time.time()
                return response, self.get_binance_api_status()
            except KeyError:
                return response, self.get_binance_api_status()
            except TypeError:
                return None, None

    def keepalive_listen_key(self, stream_id=None) -> Tuple[Union[str, None], Union[dict, None]]:
        """
        Ping a listenkey to keep it alive

        :param stream_id: provide a stream_id
        :type stream_id: str

        :return: listen_key, binance_api_status
        :rtype: Tuple[Union[str, None], Union[dict, None]]
        """
        logger.info(f"BinanceWebSocketApiRestclient.keepalive_listen_key() "
                    f"symbol='{self.stream_list[stream_id]['symbols']}' stream_id='{str(stream_id)}')")

        if stream_id is None:
            return None, None

        with (self.threading_lock):
            if self._init_ubra() is False:
                return None, None

            try:
                kwargs = {'api_key': self.stream_list[stream_id]['api_key'],
                          'api_secret': self.stream_list[stream_id]['api_secret']}
            except TypeError as error_msg:
                logger.debug(f"delete_listen_key(stream_id='{str(stream_id)}') - TypeError: {error_msg}")
                return None, None

            if self.exchange == "binance.com-margin" or \
                    self.exchange == "binance.com-margin-testnet":
                if self.restful_base_uri is not None:
                    self.ubra.MARGIN_API_URL = self.restful_base_uri
                result = self.ubra.margin_stream_keepalive(listenKey=self.stream_list[stream_id]['listen_key'],
                                                           throw_exception=False,
                                                           **kwargs)
            elif self.exchange == "binance.com-isolated_margin" or \
                    self.exchange == "binance.com-isolated_margin-testnet":
                if self.restful_base_uri is not None:
                    self.ubra.MARGIN_API_URL = self.restful_base_uri
                result = self.ubra.isolated_margin_stream_keepalive(symbol=self.stream_list[stream_id]['symbols'],
                                                                    listenKey=self.stream_list[stream_id]['listen_key'],
                                                                    throw_exception=False,
                                                                    **kwargs)
            elif self.exchange == "binance.com-futures" or self.exchange == "binance.com-futures-testnet":
                if self.restful_base_uri is not None:
                    self.ubra.FUTURES_URL = self.restful_base_uri
                result = self.ubra.futures_stream_keepalive(listenKey=self.stream_list[stream_id]['listen_key'],
                                                            throw_exception=False,
                                                            **kwargs)
            elif self.exchange == "binance.com-coin_futures":
                if self.restful_base_uri is not None:
                    self.ubra.FUTURES_URL = self.restful_base_uri
                result = self.ubra.futures_stream_keepalive(listenKey=self.stream_list[stream_id]['listen_key'],
                                                            throw_exception=False,
                                                            **kwargs)
            elif self.exchange == "binance.com-portfolio_margin":
                if self.restful_base_uri is not None:
                    self.ubra.PAPI_URL = self.restful_base_uri
                result = self.ubra.portfolio_margin_stream_keepalive(listenKey=self.stream_list[stream_id]['listen_key'],
                                                                     throw_exception=False,
                                                                     **kwargs)
            else:
                if self.restful_base_uri is not None:
                    self.ubra.API_URL = self.restful_base_uri
                result = self.ubra.stream_keepalive(listenKey=self.stream_list[stream_id]['listen_key'],
                                                    throw_exception=False,
                                                    **kwargs)

            self.stream_list[stream_id]['last_static_ping_listen_key'] = time.time()

            return result, self.get_binance_api_status()

    def stop(self) -> bool:
        """
        Stop this instance!r

        :rtype: bool
        """
        logger.debug(f"Stopping instance of `BinanceWebSocketApiRestclient()` ...")
        self.sigterm = True
        if self.ubra is not None:
            self.ubra.stop_manager()
        return True
