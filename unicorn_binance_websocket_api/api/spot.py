#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯
#
# File: unicorn_binance_websocket_api/api/spot.py
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

from decimal import Decimal
from typing import Optional, Union, Literal
import logging
import threading

__logger__: logging.getLogger = logging.getLogger("unicorn_binance_websocket_api")

logger = __logger__


class BinanceWebSocketApiApiSpot(object):
    """
    Connect to Binance Spot API via Websocket.

    Namespace: `ubwa.api.spot.*`:

    If no `stream_id` is provided, we try to find it via a provided `stream_label`, if also not available
    we use the `stream_id` of the one active websocket api stream if there is one. But if there is not exactly
    one valid websocket api stream, this will fail! It must be clear! The stream is also valid during a
    stream restart, the payload is submitted as soon the stream is online again.

    Todo:
        - https://binance-docs.github.io/apidocs/websocket_api/en/#24hr-ticker-price-change-statistics
        - https://binance-docs.github.io/apidocs/websocket_api/en/#trading-day-ticker
        - https://binance-docs.github.io/apidocs/websocket_api/en/#rolling-window-price-change-statistics
        - https://binance-docs.github.io/apidocs/websocket_api/en/#symbol-price-ticker
        - https://binance-docs.github.io/apidocs/websocket_api/en/#symbol-order-book-ticker
        - https://binance-docs.github.io/apidocs/websocket_api/en/#place-new-oco-deprecated-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#place-new-order-list-oco-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#place-new-order-list-oto-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#place-new-order-list-otoco-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#query-order-list-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#cancel-order-list-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#current-open-order-lists-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#place-new-order-using-sor-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#test-new-order-using-sor-trade
        - https://binance-docs.github.io/apidocs/websocket_api/en/#unfilled-order-count-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#account-order-history-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#account-order-list-history-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#account-trade-history-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#account-prevented-matches-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#account-allocations-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#account-commission-rates-user_data
        - https://binance-docs.github.io/apidocs/websocket_api/en/#ping-user-data-stream-user_stream
        - https://binance-docs.github.io/apidocs/websocket_api/en/#stop-user-data-stream-user_stream

    Read these instructions to get started:

        - https://technopathy.club/create-and-cancel-orders-via-websocket-on-binance-7f828831404

    Binance.com SPOT websocket API documentation:

        - https://binance-docs.github.io/apidocs/websocket_api/en/#trading-requests

    :param manager: Provide the initiated UNICORN Binance WebSocket API Manager instance.
    :type manager: BinanceWebsocketApiManager
    """

    def __init__(self, manager=None):
        self._manager = manager

    def cancel_and_replace_order(
        self,
        cancel_order_id: int = None,
        cancel_orig_client_order_id: str = None,
        cancel_new_client_order_id: str = None,
        cancel_replace_mode: Optional[
            Literal["STOP_ON_FAILURE", "ALLOW_FAILURE"]
        ] = "STOP_ON_FAILURE",
        cancel_restrictions: Optional[
            Literal["ONLY_NEW", "ONLY_PARTIALLY_FILLED"]
        ] = None,
        iceberg_qty: float = None,
        new_client_order_id: str = None,
        new_order_resp_type: Optional[Literal["ACK", "RESULT", "FULL"]] = None,
        order_rate_limit_exceeded_mode: Optional[
            Literal["DO_NOTHING", "CANCEL_ONLY"]
        ] = None,
        order_type: Optional[
            Literal[
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT",
                "TAKE_PROFIT_LIMIT",
            ]
        ] = None,
        price: float = 0.0,
        process_response=None,
        quantity: float = None,
        quote_order_qty: float = None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        self_trade_prevention_mode: Optional[
            Literal["EXPIRE_TAKER", "EXPIRE_MAKER", "EXPIRE_BOTH", "NONE"]
        ] = None,
        side: Optional[Literal["BUY", "SELL"]] = None,
        stop_price: float = None,
        strategy_id: int = None,
        strategy_type: int = None,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
        time_in_force: Optional[Literal["GTC", "IOC", "FOK"]] = None,
        trailing_delta: int = None,
    ) -> Union[str, dict, bool, tuple]:
        """
        Cancel and replace order (TRADE)

        Cancel an existing order and immediately place a new order instead of the canceled one.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#cancel-and-replace-order-trade

        :param cancel_order_id: Cancel order by orderId
        :type cancel_order_id: int
        :param cancel_orig_client_order_id: Cancel order by clientOrderId
        :type cancel_orig_client_order_id: str
        :param cancel_new_client_order_id: New ID for the canceled order. Automatically generated if not sent
        :type cancel_new_client_order_id: str
        :param cancel_replace_mode: Default is 'STOP_ON_FAILURE'!

                                        - STOP_ON_FAILURE – if cancellation request fails, new order placement will not be attempted.

                                        - ALLOW_FAILURE – new order placement will be attempted even if the cancel request fails.

        :type cancel_replace_mode: str
        :param cancel_restrictions: Supported values

                                      - ONLY_NEW: Cancel will succeed if the order status is `NEW`.

                                      - ONLY_PARTIALLY_FILLED: Cancel will succeed if order status is
                                        `PARTIALLY_FILLED`.

                                    If the cancelRestrictions value is not any of the supported values, the error will
                                    be: `{"code": -1145,"msg": "Invalid cancelRestrictions"}`

                                    If the order did not pass the conditions for cancelRestrictions, the error will be:
                                    `{"code": -2011,"msg": "Order was not canceled due to cancel restrictions."}`
        :type cancel_restrictions: str
        :param iceberg_qty: Any `LIMIT` or `LIMIT_MAKER` order can be made into an iceberg order by specifying the
                            `icebergQty`. An order with an `icebergQty` must have `timeInForce` set to `GTC`.
        :type iceberg_qty: float
        :param new_client_order_id: `newClientOrderId` specifies `clientOrderId` value for the order. A new order with
                                    the same 'clientOrderId' is accepted only when the previous one is filled or
                                    expired.
        :type new_client_order_id: str
        :param new_order_resp_type: Select response format: `ACK`, `RESULT`, `FULL`.
                                    'MARKET' and 'LIMIT' orders use `FULL` by default, other order types default to
                                    'ACK'
        :type new_order_resp_type: str
        :param order_rate_limit_exceeded_mode: Supported values

                                                   - DO_NOTHING (default)- will only attempt to cancel the order if
                                                     account has not exceeded the unfilled order rate limit

                                                   - CANCEL_ONLY - will always cancel the order.
        :type order_rate_limit_exceeded_mode: str
        :param order_type: 'LIMIT', 'LIMIT_MAKER', 'MARKET', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT',
                           'TAKE_PROFIT_LIMIT'

                           Mandatory parameters per `order_type`:

                             - LIMIT: 'timeInForce', 'price', 'quantity'

                             - LIMIT_MAKER: 'price', 'quantity'

                             - MARKET: 'quantity' or 'quoteOrderQty'

                             - STOP_LOSS: 'quantity', 'stopPrice' or 'trailingDelta'

                             - STOP_LOSS_LIMIT: 'timeInForce', 'price', 'quantity', 'stopPrice' or 'trailingDelta'

                             - TAKE_PROFIT: 'quantity', 'stopPrice' or 'trailingDelta'

                             - TAKE_PROFIT_LIMIT: 'timeInForce', 'price', 'quantity', 'stopPrice' or 'trailingDelta'
        :type order_type: str
        :param price: Price e.g. 10.223
        :type price: float
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param quantity: Amount e.g. 20.5
        :type quantity: float
        :param quote_order_qty: Amount e.g. 20.5
        :type quote_order_qty: float
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param self_trade_prevention_mode: The allowed enums for `selfTradePreventionMode` is dependent on what is
                                           configured on the symbol. The possible supported values are `EXPIRE_TAKER`,
                                           `EXPIRE_MAKER`, `EXPIRE_BOTH`, `NONE`.
        :type self_trade_prevention_mode: str
        :param side: `BUY` or `SELL`
        :type side: str
        :param strategy_id: Arbitrary numeric value identifying the order within an order strategy.
        :type strategy_id: int
        :param strategy_type: Arbitrary numeric value identifying the order strategy. Values smaller than 1000000 are
                              reserved and cannot be used.
        :type strategy_type: int
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param stop_price: Trigger order price rules for STOP_LOSS/TAKE_PROFIT orders:

                             - `stopPrice` must be above market price: STOP_LOSS BUY, TAKE_PROFIT SELL

                             - stopPrice must be below market price: STOP_LOSS SELL, TAKE_PROFIT BUY
        :type stop_price: float
        :param symbol: The symbol you want to trade
        :type symbol: str
        :param time_in_force: Available timeInForce options, setting how long the order should be active before
                              expiration:

                                - GTC: Good 'til Canceled – the order will remain on the book until you cancel it, or
                                  the order is completely filled.

                                - IOC: Immediate or Cancel – the order will be filled for as much as possible, the
                                  unfilled quantity immediately expires.

                                - FOK: Fill or Kill – the order will expire unless it cannot be immediately filled for
                                  the entire quantity.

                              `MARKET` orders using `quoteOrderQty` follow `LOT_SIZE` filter rules. The order will
                              execute a quantity that has notional value as close as possible to requested
                              `quoteOrderQty`.
        :type time_in_force: str
        :param trailing_delta: For more details on SPOT implementation on trailing stops, please refer to
                               `Trailing Stop FAQ <https://github.com/binance/binance-spot-api-docs/blob/master/faqs/trailing-stop-faq.md>`__
        :type trailing_delta: int

        :return: str (new_client_order_id), bool or tuple (str (new_client_order_id), str/dict (response_value))

        Message sent:

        .. code-block:: json

            {
              "id": "99de1036-b5e2-4e0f-9b5c-13d751c93a1a",
              "method": "order.cancelReplace",
              "params": {
                "symbol": "BTCUSDT",
                "cancelReplaceMode": "ALLOW_FAILURE",
                "cancelOrigClientOrderId": "4d96324ff9d44481926157",
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "price": "23416.10000000",
                "quantity": "0.00847000",
                "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                "signature": "7028fdc187868754d25e42c37ccfa5ba2bab1d180ad55d4c3a7e2de643943dc5",
                "timestamp": 1660813156900
              }
            }

        Response

        .. code-block:: json

            {
              "id": "99de1036-b5e2-4e0f-9b5c-13d751c93a1a",
              "status": 200,
              "result": {
                "cancelResult": "SUCCESS",
                "newOrderResult": "SUCCESS",
                // Format is identical to "order.cancel" format.
                // Some fields are optional and are included only for orders that set them.
                "cancelResponse": {
                  "symbol": "BTCUSDT",
                  "origClientOrderId": "4d96324ff9d44481926157",  // cancelOrigClientOrderId from request
                  "orderId": 125690984230,
                  "orderListId": -1,
                  "clientOrderId": "91fe37ce9e69c90d6358c0",      // cancelNewClientOrderId from request
                  "transactTime": 1684804350068,
                  "price": "23450.00000000",
                  "origQty": "0.00847000",
                  "executedQty": "0.00001000",
                  "cummulativeQuoteQty": "0.23450000",
                  "status": "CANCELED",
                  "timeInForce": "GTC",
                  "type": "LIMIT",
                  "side": "SELL",
                  "selfTradePreventionMode": "NONE"
                },
                // Format is identical to "order.place" format, affected by "newOrderRespType".
                // Some fields are optional and are included only for orders that set them.
                "newOrderResponse": {
                  "symbol": "BTCUSDT",
                  "orderId": 12569099453,
                  "orderListId": -1,
                  "clientOrderId": "bX5wROblo6YeDwa9iTLeyY",      // newClientOrderId from request
                  "transactTime": 1660813156959,
                  "price": "23416.10000000",
                  "origQty": "0.00847000",
                  "executedQty": "0.00000000",
                  "cummulativeQuoteQty": "0.00000000",
                  "status": "NEW",
                  "timeInForce": "GTC",
                  "type": "LIMIT",
                  "side": "SELL",
                  "workingTime": 1660813156959,
                  "fills": [],
                  "selfTradePreventionMode": "NONE"
                }
              },
              "rateLimits": [
                {
                  "rateLimitType": "ORDERS",
                  "interval": "SECOND",
                  "intervalNum": 10,
                  "limit": 50,
                  "count": 1
                },
                {
                  "rateLimitType": "ORDERS",
                  "interval": "DAY",
                  "intervalNum": 1,
                  "limit": 160000,
                  "count": 1
                },
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 1
                }
              ]
            }
        """
        if (
            cancel_replace_mode is None
            or quantity is None
            or side is None
            or symbol is None
            or order_type is None
            or (cancel_order_id is None and cancel_orig_client_order_id is None)
        ):
            raise ValueError(
                f"Missing mandatory parameter: cancel_replace_mode, quantity, side, symbol, order_type,"
                f"cancel_order_id/cancel_orig_client_order_id"
            )

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.cancel_and_replace_order() - error_msg: No `stream_id` "
                    f"provided or found!"
                )
                return False

        new_client_order_id = (
            new_client_order_id
            if new_client_order_id is not None
            else str(self._manager.get_request_id())
        )
        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "cancelReplaceMode": cancel_replace_mode,
            "side": side.upper(),
            "symbol": symbol.upper(),
            "timestamp": self._manager.get_timestamp(),
            "type": order_type,
        }

        if cancel_order_id is not None:
            params["cancelOrderId"] = int(cancel_order_id)
        if cancel_orig_client_order_id is not None:
            params["cancelOrigClientOrderId"] = str(cancel_orig_client_order_id)
        if cancel_new_client_order_id is not None:
            params["cancelNewClientOrderId"] = str(cancel_new_client_order_id)
        if cancel_restrictions is not None:
            params["cancelRestrictions"] = cancel_restrictions
        if iceberg_qty is not None:
            params["icebergQty"] = format(Decimal(repr(iceberg_qty)), "f")
        if new_client_order_id is not None:
            params["NewClientOrderId"] = new_client_order_id
        if new_order_resp_type is not None:
            params["newOrderRespType"] = new_order_resp_type
        if order_rate_limit_exceeded_mode is not None:
            params["orderRateLimitExceededMode"] = order_rate_limit_exceeded_mode
        if (
            order_type.upper() == "LIMIT"
            or order_type.upper() == "LIMIT_MAKER"
            or order_type.upper() == "STOP_LOSS_LIMIT"
            or order_type.upper() == "TAKE_PROFIT_LIMIT"
        ):
            params["price"] = format(Decimal(repr(price)), "f")
        if (
            order_type.upper() == "LIMIT"
            or order_type.upper() == "STOP_LOSS_LIMIT"
            or order_type.upper() == "TAKE_PROFIT_LIMIT"
        ):
            params["timeInForce"] = time_in_force
        if quantity is not None:
            params["quantity"] = format(Decimal(repr(quantity)), "f")
        if quote_order_qty is not None:
            params["quoteOrderQty"] = format(Decimal(repr(quote_order_qty)), "f")
            if quantity is not None:
                logger.warning(
                    f"BinanceWebSocketApiApiSpot.cancel_and_replace_order() - error_msg: By using the "
                    f"parameter `quoteOrderQty` the use of `quantity` is suppressed!"
                )
            del params["quantity"]
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)
        if self_trade_prevention_mode is not None:
            params["selfTradePreventionMode"] = self_trade_prevention_mode
        if stop_price is not None:
            params["stopPrice"] = format(Decimal(repr(stop_price)), "f")
            if trailing_delta is not None:
                logger.warning(
                    f"BinanceWebSocketApiApiSpot.cancel_and_replace_order() - error_msg: By using the "
                    f"parameter `stopPrice` the use of `trailingDelta` is suppressed!"
                )
        elif trailing_delta is not None:
            params["trailingDelta"] = str(trailing_delta)
        if strategy_id is not None:
            params["strategyId"] = str(strategy_id)
        if strategy_type is not None:
            params["strategyType"] = str(strategy_type)

        method = "order.cancelReplace"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.cancel_and_replace_order() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return new_client_order_id, response_value

        return new_client_order_id

    def cancel_open_orders(
        self,
        process_response=None,
        return_response: bool = False,
        symbol: str = None,
        recv_window: int = None,
        request_id: str = None,
        stream_id: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Cancel open orders (TRADE)

        Cancel all open orders on a symbol, including OCO orders.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#cancel-open-orders-trade

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param symbol: The symbol you want to trade
        :type symbol: int
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str

        :return: str, dict, bool

        Message Sent:

        .. code-block:: json

            {
                "id": "778f938f-9041-4b88-9914-efbf64eeacc8",
                "method": "openOrders.cancelAll"
                "params": {
                    "symbol": "BTCUSDT",
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "773f01b6e3c2c9e0c1d217bc043ce383c1ddd6f0e25f8d6070f2b66a6ceaf3a5",
                    "timestamp": 1660805557200
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "778f938f-9041-4b88-9914-efbf64eeacc8",
                "status": 200,
                "result": [
                    {
                        "symbol": "BTCUSDT",
                        "origClientOrderId": "4d96324ff9d44481926157",
                        "orderId": 12569099453,
                        "orderListId": -1,
                        "clientOrderId": "91fe37ce9e69c90d6358c0",
                        "price": "23416.10000000",
                        "origQty": "0.00847000",
                        "executedQty": "0.00001000",
                        "cummulativeQuoteQty": "0.23416100",
                        "status": "CANCELED",
                        "timeInForce": "GTC",
                        "type": "LIMIT",
                        "side": "SELL",
                        "stopPrice": "0.00000000",
                        "trailingDelta": 0,
                        "trailingTime": -1,
                        "icebergQty": "0.00000000",
                        "strategyId": 37463720,
                        "strategyType": 1000000,
                        "selfTradePreventionMode": "NONE"
                    },
                    {
                        "orderListId": 19431,
                        "contingencyType": "OCO",
                        "listStatusType": "ALL_DONE",
                        "listOrderStatus": "ALL_DONE",
                        "listClientOrderId": "iuVNVJYYrByz6C4yGOPPK0",
                        "transactionTime": 1660803702431,
                        "symbol": "BTCUSDT",
                        "orders": [
                            {
                            "symbol": "BTCUSDT",
                            "orderId": 12569099453,
                            "clientOrderId": "bX5wROblo6YeDwa9iTLeyY"
                            },
                            {
                            "symbol": "BTCUSDT",
                            "orderId": 12569099454,
                            "clientOrderId": "Tnu2IP0J5Y4mxw3IATBfmW"
                            }
                        ],
                        "orderReports": [
                            {
                                "symbol": "BTCUSDT",
                                "origClientOrderId": "bX5wROblo6YeDwa9iTLeyY",
                                "orderId": 12569099453,
                                "orderListId": 19431,
                                "clientOrderId": "OFFXQtxVFZ6Nbcg4PgE2DA",
                                "price": "23450.50000000",
                                "origQty": "0.00850000",
                                "executedQty": "0.00000000",
                                "cummulativeQuoteQty": "0.00000000",
                                "status": "CANCELED",
                                "timeInForce": "GTC",
                                "type": "STOP_LOSS_LIMIT",
                                "side": "BUY",
                                "stopPrice": "23430.00000000",
                                "selfTradePreventionMode": "NONE"
                            },
                            {
                                "symbol": "BTCUSDT",
                                "origClientOrderId": "Tnu2IP0J5Y4mxw3IATBfmW",
                                "orderId": 12569099454,
                                "orderListId": 19431,
                                "clientOrderId": "OFFXQtxVFZ6Nbcg4PgE2DA",
                                "price": "23400.00000000",
                                "origQty": "0.00850000",
                                "executedQty": "0.00000000",
                                "cummulativeQuoteQty": "0.00000000",
                                "status": "CANCELED",
                                "timeInForce": "GTC",
                                "type": "LIMIT_MAKER",
                                "side": "BUY",
                                "selfTradePreventionMode": "NONE"
                            }
                        ]
                    }
                ],
                "rateLimits": [
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 1
                    }
                ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.cancel_open_orders() - error_msg: No `stream_id` provided"
                    f" or found!"
                )
                return False

        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "symbol": symbol.upper(),
            "timestamp": self._manager.get_timestamp(),
        }

        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        method = "openOrders.cancelAll"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.cancel_open_orders() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def cancel_order(
        self,
        cancel_restrictions: Optional[
            Literal["ONLY_NEW", "ONLY_PARTIALLY_FILLED"]
        ] = None,
        new_client_order_id: str = None,
        order_id: int = None,
        orig_client_order_id: str = None,
        process_response=None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        symbol: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Cancel order (TRADE)

        Cancel an active order.

        Either order_id or orig_client_order_id must be sent.

        If you cancel an order that is a part of an OCO pair, the entire OCO is canceled.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#cancel-order-trade

        :param cancel_restrictions: Supported values:

                                      - ONLY_NEW: Cancel will succeed if the order status is `NEW`.

                                      - ONLY_PARTIALLY_FILLED: Cancel will succeed if order status is
                                        `PARTIALLY_FILLED`.

                                    If the cancelRestrictions value is not any of the supported values, the error will
                                    be: `{"code": -1145,"msg": "Invalid cancelRestrictions"}`

                                    If the order did not pass the conditions for cancelRestrictions, the error will be:
                                    `{"code": -2011,"msg": "Order was not canceled due to cancel restrictions."}`
        :type cancel_restrictions: str
        :param new_client_order_id: New ID for the canceled order. Automatically generated if not sent.
                                    `newClientOrderId` will replace `clientOrderId` of the canceled order, freeing it
                                    up for new orders.
        :type new_client_order_id: str
        :param order_id: Cancel by `order_id`. If both `orderId` and `origClientOrderId` parameters are specified, only
                         `orderId` is used and `origClientOrderId` is ignored.
        :type order_id: str
        :param orig_client_order_id: Cancel by `origClientOrderId`. If both `orderId` and `origClientOrderId` parameters
                                     are specified, only `orderId` is used and `origClientOrderId` is ignored.
        :type orig_client_order_id: str
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The symbol of the order you want to cancel
        :type symbol: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "5633b6a2-90a9-4192-83e7-925c90b6a2fd",
                "method": "order.cancel",
                "params": {
                    "symbol": "BTCUSDT",
                    "origClientOrderId": "4d96324ff9d44481926157",
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "33d5b721f278ae17a52f004a82a6f68a70c68e7dd6776ed0be77a455ab855282",
                    "timestamp": 1660801715830
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "5633b6a2-90a9-4192-83e7-925c90b6a2fd",
                "status": 200,
                "result": {
                    "symbol": "BTCUSDT",
                    "origClientOrderId": "4d96324ff9d44481926157",
                    "orderId": 12569099453,
                    "orderListId": -1,
                    "clientOrderId": "91fe37ce9e69c90d6358c0",
                    "price": "23416.10000000",
                    "origQty": "0.00847000",
                    "executedQty": "0.00001000",
                    "cummulativeQuoteQty": "0.23416100",
                    "status": "CANCELED",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "SELL",
                    "stopPrice": "0.00000000",
                    "trailingDelta": 0,
                    "trailingTime": -1,
                    "icebergQty": "0.00000000",
                    "strategyId": 37463720,
                    "strategyType": 1000000,
                    "selfTradePreventionMode": "NONE"
                },
                "rateLimits": [
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 1
                    }
                ]
            }
        """
        if symbol is None or (order_id is None and orig_client_order_id is None):
            raise ValueError(
                f"Missing mandatory parameter: symbol, order_id/orig_client_order_id"
            )

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.cancel_order() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "symbol": symbol.upper(),
            "timestamp": self._manager.get_timestamp(),
        }

        if cancel_restrictions is not None:
            params["cancelRestrictions"] = cancel_restrictions
        if new_client_order_id is not None:
            params["newClientOrderId"] = new_client_order_id
        if order_id is not None:
            params["orderId"] = int(order_id)
        if orig_client_order_id is not None:
            params["origClientOrderId"] = str(orig_client_order_id)
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        method = "order.cancel"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.cancel_order() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def create_order(
        self,
        iceberg_qty: float = None,
        new_client_order_id: str = None,
        new_order_resp_type: Optional[Literal["ACK", "RESULT", "FULL"]] = None,
        order_type: Optional[
            Literal[
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT",
                "TAKE_PROFIT_LIMIT",
            ]
        ] = None,
        price: float = 0.0,
        process_response=None,
        quantity: float = None,
        quote_order_qty: float = None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        self_trade_prevention_mode: Optional[
            Literal["EXPIRE_TAKER", "EXPIRE_MAKER", "EXPIRE_BOTH", "NONE"]
        ] = None,
        side: Optional[Literal["BUY", "SELL"]] = None,
        stop_price: float = None,
        strategy_id: int = None,
        strategy_type: int = None,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
        time_in_force: Optional[Literal["GTC", "IOC", "FOK"]] = None,
        test: bool = False,
        trailing_delta: int = None,
    ) -> Union[str, dict, bool, tuple]:
        """
        Place new order (TRADE)

        Create a new order.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#place-new-order-trade

        :param iceberg_qty: Any `LIMIT` or `LIMIT_MAKER` order can be made into an iceberg order by specifying the
                            `icebergQty`. An order with an `icebergQty` must have `timeInForce` set to `GTC`.
        :type iceberg_qty: float
        :param new_client_order_id: `newClientOrderId` specifies `clientOrderId` value for the order. A new order with
                                    the same 'clientOrderId' is accepted only when the previous one is filled or
                                    expired.
        :type new_client_order_id: str
        :param new_order_resp_type: Select response format: `ACK`, `RESULT`, `FULL`.
                                    'MARKET' and 'LIMIT' orders use `FULL` by default, other order types default to
                                    'ACK'
        :type new_order_resp_type: str
        :param order_type: 'LIMIT', 'LIMIT_MAKER', 'MARKET', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT',
                           'TAKE_PROFIT_LIMIT'

                           Mandatory parameters per `order_type`:

                             - LIMIT: 'timeInForce', 'price', 'quantity'

                             - LIMIT_MAKER: 'price', 'quantity'

                             - MARKET: 'quantity' or 'quoteOrderQty'

                             - STOP_LOSS: 'quantity', 'stopPrice' or 'trailingDelta'

                             - STOP_LOSS_LIMIT: 'timeInForce', 'price', 'quantity', 'stopPrice' or 'trailingDelta'

                             - TAKE_PROFIT: 'quantity', 'stopPrice' or 'trailingDelta'

                             - TAKE_PROFIT_LIMIT: 'timeInForce', 'price', 'quantity', 'stopPrice' or 'trailingDelta'
        :type order_type: str
        :param price: Price e.g. 10.223
        :type price: float
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param quantity: Amount e.g. 20.5
        :type quantity: float
        :param quote_order_qty: Amount e.g. 20.5
        :type quote_order_qty: float
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param self_trade_prevention_mode: The allowed enums for `selfTradePreventionMode` is dependent on what is
                                           configured on the symbol. The possible supported values are `EXPIRE_TAKER`,
                                           `EXPIRE_MAKER`, `EXPIRE_BOTH`, `NONE`.
        :type self_trade_prevention_mode: str
        :param side: `BUY` or `SELL`
        :type side: str
        :param strategy_id: Arbitrary numeric value identifying the order within an order strategy.
        :type strategy_id: int
        :param strategy_type: Arbitrary numeric value identifying the order strategy. Values smaller than 1000000 are
                              reserved and cannot be used.
        :type strategy_type: int
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param stop_price: Trigger order price rules for STOP_LOSS/TAKE_PROFIT orders:

                             - `stopPrice` must be above market price: STOP_LOSS BUY, TAKE_PROFIT SELL

                             - stopPrice must be below market price: STOP_LOSS SELL, TAKE_PROFIT BUY
        :type stop_price: float
        :param symbol: The symbol you want to trade
        :type symbol: str
        :param test: Test order placement. Validates new order parameters and verifies your signature but does not
                     send the order into the matching engine.
        :type test: bool
        :param time_in_force: Available timeInForce options, setting how long the order should be active before
                              expiration:

                                - GTC: Good 'til Canceled – the order will remain on the book until you cancel it, or
                                  the order is completely filled.

                                - IOC: Immediate or Cancel – the order will be filled for as much as possible, the
                                  unfilled quantity immediately expires.

                                - FOK: Fill or Kill – the order will expire unless it cannot be immediately filled for
                                  the entire quantity.

                              `MARKET` orders using `quoteOrderQty` follow `LOT_SIZE` filter rules. The order will
                              execute a quantity that has notional value as close as possible to requested
                              `quoteOrderQty`.
        :type time_in_force: str
        :param trailing_delta: For more details on SPOT implementation on trailing stops, please refer to
                               `Trailing Stop FAQ <https://github.com/binance/binance-spot-api-docs/blob/master/faqs/trailing-stop-faq.md>`__
        :type trailing_delta: int

        :return: str (new_client_order_id), bool or tuple (str (new_client_order_id), str/dict (response_value))

        Message sent:

        .. code-block:: json

            {
                "id": "56374a46-3061-486b-a311-99ee972eb648",
                "method": "order.place",
                "params": {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "price": "23416.10000000",
                    "quantity": "0.00847000",
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "15af09e41c36f3cc61378c2fbe2c33719a03dd5eba8d0f9206fbda44de717c88",
                    "timestamp": 1660801715431
                }
            }

        Response

        .. code-block:: json

            {
                "id": "56374a46-3061-486b-a311-99ee972eb648",
                "status": 200,
                "result": {
                    "symbol": "BTCUSDT",
                    "orderId": 12569099453,
                    "orderListId": -1,
                    "clientOrderId": "4d96324ff9d44481926157ec08158a40",
                    "transactTime": 1660801715639
                },
                "rateLimits": [
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "SECOND",
                        "intervalNum": 10,
                        "limit": 50,
                        "count": 1
                    },
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "DAY",
                        "intervalNum": 1,
                        "limit": 160000,
                        "count": 1
                    },
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 1
                    }
                ]
            }
        """
        if side is None or symbol is None or order_type is None:
            raise ValueError(f"Missing mandatory parameter: order_type, side, symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.create_order() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        new_client_order_id = (
            new_client_order_id
            if new_client_order_id is not None
            else str(self._manager.get_request_id())
        )
        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "newClientOrderId": new_client_order_id,
            "side": side.upper(),
            "symbol": symbol.upper(),
            "timestamp": self._manager.get_timestamp(),
            "type": order_type,
        }

        if iceberg_qty is not None:
            params["icebergQty"] = format(Decimal(repr(iceberg_qty)), "f")
        if new_order_resp_type is not None:
            params["newOrderRespType"] = new_order_resp_type
        if (
            order_type.upper() == "LIMIT"
            or order_type.upper() == "LIMIT_MAKER"
            or order_type.upper() == "STOP_LOSS_LIMIT"
            or order_type.upper() == "TAKE_PROFIT_LIMIT"
        ):
            params["price"] = format(Decimal(repr(price)), "f")
        if (
            order_type.upper() == "LIMIT"
            or order_type.upper() == "STOP_LOSS_LIMIT"
            or order_type.upper() == "TAKE_PROFIT_LIMIT"
        ):
            params["timeInForce"] = time_in_force
        if quantity is not None:
            params["quantity"] = format(Decimal(repr(quantity)), "f")
        if quote_order_qty is not None:
            params["quoteOrderQty"] = format(Decimal(repr(quote_order_qty)), "f")
            if quantity is not None:
                logger.warning(
                    f"BinanceWebSocketApiApiSpot.create_order() - error_msg: By using the parameter "
                    f"`quoteOrderQty` the use of `quantity` is suppressed!"
                )
            del params["quantity"]
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)
        if self_trade_prevention_mode is not None:
            params["selfTradePreventionMode"] = self_trade_prevention_mode
        if stop_price is not None:
            params["stopPrice"] = format(Decimal(repr(stop_price)), "f")
            if trailing_delta is not None:
                logger.warning(
                    f"BinanceWebSocketApiApiSpot.create_order() - error_msg: By using the parameter "
                    f"`stopPrice` the use of `trailingDelta` is suppressed!"
                )
        elif trailing_delta is not None:
            params["trailingDelta"] = str(trailing_delta)
        if strategy_id is not None:
            params["strategyId"] = str(strategy_id)
        if strategy_type is not None:
            params["strategyType"] = str(strategy_type)

        method = "order.test" if test is True else "order.place"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.create_order() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return new_client_order_id, response_value

        return new_client_order_id

    def create_test_order(
        self,
        iceberg_qty: float = None,
        new_client_order_id: str = None,
        new_order_resp_type: Optional[Literal["ACK", "RESULT", "FULL"]] = None,
        order_type: Optional[
            Literal[
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT",
                "TAKE_PROFIT_LIMIT",
            ]
        ] = None,
        price: float = 0.0,
        process_response=None,
        quantity: float = None,
        quote_order_qty: float = None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        self_trade_prevention_mode: Optional[
            Literal["EXPIRE_TAKER", "EXPIRE_MAKER", "EXPIRE_BOTH", "NONE"]
        ] = None,
        side: Optional[Literal["BUY", "SELL"]] = None,
        stop_price: float = None,
        strategy_id: int = None,
        strategy_type: int = None,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
        time_in_force: Optional[Literal["GTC", "IOC", "FOK"]] = None,
        trailing_delta: int = None,
    ) -> Union[str, dict, bool, tuple]:
        """
        Test new order (TRADE)

        Test order placement.

        Validates new order parameters and verifies your signature but does not send the order into the matching engine.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#test-new-order-trade

        :param iceberg_qty: Any `LIMIT` or `LIMIT_MAKER` order can be made into an iceberg order by specifying the
                            `icebergQty`. An order with an `icebergQty` must have `timeInForce` set to `GTC`.
        :type iceberg_qty: float
        :param new_client_order_id: `newClientOrderId` specifies `clientOrderId` value for the order. A new order with
                                    the same 'clientOrderId' is accepted only when the previous one is filled or
                                    expired.
        :type new_client_order_id: str
        :param new_order_resp_type: Select response format: `ACK`, `RESULT`, `FULL`.
                                    'MARKET' and 'LIMIT' orders use `FULL` by default, other order types default to
                                    'ACK'
        :type new_order_resp_type: str
        :param order_type: 'LIMIT', 'LIMIT_MAKER', 'MARKET', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT',
                           'TAKE_PROFIT_LIMIT'

                           Mandatory parameters per `order_type`:

                             - LIMIT: 'timeInForce', 'price', 'quantity'

                             - LIMIT_MAKER: 'price', 'quantity'

                             - MARKET: 'quantity' or 'quoteOrderQty'

                             - STOP_LOSS: 'quantity', 'stopPrice' or 'trailingDelta'

                             - STOP_LOSS_LIMIT: 'timeInForce', 'price', 'quantity', 'stopPrice' or 'trailingDelta'

                             - TAKE_PROFIT: 'quantity', 'stopPrice' or 'trailingDelta'

                             - TAKE_PROFIT_LIMIT: 'timeInForce', 'price', 'quantity', 'stopPrice' or 'trailingDelta'
        :type order_type: str
        :param price: Price e.g. 10.223
        :type price: float
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param quantity: Amount e.g. 20.5
        :type quantity: float
        :param quote_order_qty: Amount e.g. 20.5
        :type quote_order_qty: float
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param self_trade_prevention_mode: The allowed enums for `selfTradePreventionMode` is dependent on what is
                                           configured on the symbol. The possible supported values are `EXPIRE_TAKER`,
                                           `EXPIRE_MAKER`, `EXPIRE_BOTH`, `NONE`.
        :type self_trade_prevention_mode: str
        :param side: `BUY` or `SELL`
        :type side: str
        :param strategy_id: Arbitrary numeric value identifying the order within an order strategy.
        :type strategy_id: int
        :param strategy_type: Arbitrary numeric value identifying the order strategy. Values smaller than 1000000 are
                              reserved and cannot be used.
        :type strategy_type: int
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param stop_price: Trigger order price rules for STOP_LOSS/TAKE_PROFIT orders:

                             - `stopPrice` must be above market price: STOP_LOSS BUY, TAKE_PROFIT SELL

                             - stopPrice must be below market price: STOP_LOSS SELL, TAKE_PROFIT BUY
        :type stop_price: float
        :param symbol: The symbol you want to trade
        :type symbol: str
        :param time_in_force: Available timeInForce options, setting how long the order should be active before
                              expiration:

                                - GTC: Good 'til Canceled – the order will remain on the book until you cancel it, or
                                  the order is completely filled.

                                - IOC: Immediate or Cancel – the order will be filled for as much as possible, the
                                  unfilled quantity immediately expires.

                                - FOK: Fill or Kill – the order will expire unless it cannot be immediately filled for
                                  the entire quantity.

                              `MARKET` orders using `quoteOrderQty` follow `LOT_SIZE` filter rules. The order will
                              execute a quantity that has notional value as close as possible to requested
                              `quoteOrderQty`.
        :type time_in_force: str
        :param trailing_delta: For more details on SPOT implementation on trailing stops, please refer to
                               `Trailing Stop FAQ <https://github.com/binance/binance-spot-api-docs/blob/master/faqs/trailing-stop-faq.md>`__
        :type trailing_delta: int

        :return: str (new_client_order_id), bool or tuple (str (new_client_order_id), str/dict (response_value))

        Message sent:

        .. code-block:: json

            {
                "id": "56374a46-3061-486b-a311-99ee972eb648",
                "method": "order.test",
                "params": {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "price": "23416.10000000",
                    "quantity": "0.00847000",
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "15af09e41c36f3cc61378c2fbe2c33719a03dd5eba8d0f9206fbda44de717c88",
                    "timestamp": 1660801715431
                }
            }

        Response

        .. code-block:: json

            {
                "id": "56374a46-3061-486b-a311-99ee972eb648",
                "status": 200,
                "result": {
                    "symbol": "BTCUSDT",
                    "orderId": 12569099453,
                    "orderListId": -1,
                    "clientOrderId": "4d96324ff9d44481926157ec08158a40",
                    "transactTime": 1660801715639
                },
                "rateLimits": [
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "SECOND",
                        "intervalNum": 10,
                        "limit": 50,
                        "count": 1
                    },
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "DAY",
                        "intervalNum": 1,
                        "limit": 160000,
                        "count": 1
                    },
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 1
                    }
                ]
            }
        """
        return self.create_order(
            iceberg_qty=iceberg_qty,
            new_client_order_id=new_client_order_id,
            new_order_resp_type=new_order_resp_type,
            price=price,
            order_type=order_type,
            process_response=process_response,
            quantity=quantity,
            quote_order_qty=quote_order_qty,
            recv_window=recv_window,
            request_id=request_id,
            return_response=return_response,
            self_trade_prevention_mode=self_trade_prevention_mode,
            side=side,
            stop_price=stop_price,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            stream_id=stream_id,
            stream_label=stream_label,
            symbol=symbol,
            time_in_force=time_in_force,
            test=True,
            trailing_delta=trailing_delta,
        )

    def get_account_status(
        self,
        process_response=None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Account information (USER_DATA)

        Query information about your account.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#account-information-user_data

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "605a6d20-6588-4cb9-afa0-b0ab087507ba",
                "method": "account.status",
                "params": {
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "83303b4a136ac1371795f465808367242685a9e3a42b22edb4d977d0696eb45c",
                    "timestamp": 1660801839480
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "605a6d20-6588-4cb9-afa0-b0ab087507ba",
                "status": 200,
                "result": {
                    "makerCommission": 15,
                    "takerCommission": 15,
                    "buyerCommission": 0,
                    "sellerCommission": 0,
                    "canTrade": true,
                    "canWithdraw": true,
                    "canDeposit": true,
                    "commissionRates": {
                        "maker": "0.00150000",
                        "taker": "0.00150000",
                        "buyer": "0.00000000",
                        "seller":"0.00000000"
                    },
                    "brokered": false,
                    "requireSelfTradePrevention": false,
                    "updateTime": 1660801833000,
                    "accountType": "SPOT",
                    "balances": [
                        {
                            "asset": "BNB",
                            "free": "0.00000000",
                            "locked": "0.00000000"
                        },
                        {
                            "asset": "BTC",
                            "free": "1.3447112",
                            "locked": "0.08600000"
                        },
                        {
                            "asset": "USDT",
                            "free": "1021.21000000",
                            "locked": "0.00000000"
                        }
                    ],
                    "permissions": [
                        "SPOT"
                    ]
                },
                "rateLimits": [
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 10
                    }
                ]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_account_status() - error_msg: No `stream_id` provided"
                    f" or found!"
                )
                return False

        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "timestamp": self._manager.get_timestamp(),
        }

        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        method = "account.status"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_account_status() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_aggregate_trades(
        self,
        process_response=None,
        end_time: int = None,
        from_id: int = None,
        limit: int = None,
        request_id: str = None,
        return_response: bool = False,
        start_time: int = None,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Aggregate trades

        Get aggregate trades.

        Use from_id and limit to page through all aggtrades.

        If you need access to real-time trading activity, please consider using
        'WebSocket Streams <https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream>'_

          - <symbol>@aggTrade

        If you need historical aggregate trade data, please consider using data.binance.vision:
        https://github.com/binance/binance-public-data/#aggtrades

        Weight(IP): 2

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#aggregate-trades

        :param end_time: The end time of the selection.
        :type end_time: int
        :param from_id: If from_id is specified, return aggtrades with aggregate trade ID >= from_id.
        :type from_id: int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param start_time: The start time of the selection.
        :type start_time: int
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The selected symbol
        :type symbol: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "189da436-d4bd-48ca-9f95-9f613d621717",
              "method": "trades.aggregate",
              "params": {
                "symbol": "BNBBTC",
                "fromId": 50000000,
                "limit": 1
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "189da436-d4bd-48ca-9f95-9f613d621717",
              "status": 200,
              "result": [
                {
                  "a": 50000000,        // Aggregate trade ID
                  "p": "0.00274100",    // Price
                  "q": "57.19000000",   // Quantity
                  "f": 59120167,        // First trade ID
                  "l": 59120170,        // Last trade ID
                  "T": 1565877971222,   // Timestamp
                  "m": true,            // Was the buyer the maker?
                  "M": true             // Was the trade the best price match?
                 }
              ],
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 2
                }
              ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_aggregate_trades() - error_msg: No `stream_id` "
                    f"provided or found!"
                )
                return False

        params = {"symbol": symbol.upper()}

        if limit is not None:
            params["limit"] = str(limit)
        if end_time is not None:
            params["endTime"] = str(end_time)
        if from_id is not None:
            params["fromId"] = str(from_id)
        if start_time is not None:
            params["startTime"] = str(start_time)

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "trades.aggregate", "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_aggregate_trades() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_current_average_price(
        self,
        process_response=None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Current average price

        Get current average price for a symbol.

        Weight(IP): 2

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#current-average-price

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: Specifiy the symbol!
        :type symbol: str

        :return: str, dict, bool

        Message Sent:

        .. code-block:: json

            {
              "id": "ddbfb65f-9ebf-42ec-8240-8f0f91de0867",
              "method": "avgPrice",
              "params": {
                "symbol": "BNBBTC"
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "ddbfb65f-9ebf-42ec-8240-8f0f91de0867",
              "status": 200,
              "result": {
                "mins": 5,                 // Average price interval (in minutes)
                "price": "0.01378135",     // Average price
                "closeTime": 1694061154503 // Last trade time
              },
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 2
                }
              ]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_current_average_price() - error_msg: No `stream_id` "
                    f"provided or found!"
                )
                return False

        params = {}
        if symbol is not None:
            params["symbol"] = str(symbol.upper())

        method = "avgPrice"
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_current_average_price() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_exchange_info(
        self,
        permissions: list = None,
        process_response=None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
        symbols: list = None,
    ) -> Union[str, dict, bool]:
        """
        Exchange information

        Get the Exchange Information.

        Only one of `symbol`, `symbols`, `permissions` parameters can be specified.

        Without parameters, `exchangeInfo` displays all symbols with ["SPOT, "MARGIN", "LEVERAGED"] permissions. In
        order to list all active symbols on the exchange, you need to explicitly request all permissions

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#exchange-information

        :param permissions: Filter symbols by permissions. `permissions` accepts either a list of permissions, or a
                            single permission name: "SPOT".
                            `Available Permissions <https://binance-docs.github.io/apidocs/websocket_api/en/#exchange-information>`__
        :type permissions: list
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: Describe a single symbol
        :type symbol: str
        :param symbols: Describe multiple symbols.
        :type symbols: list

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "5494febb-d167-46a2-996d-70533eb4d976",
                "method": "exchangeInfo",
                "params": {
                    "symbols": [
                        "BNBBTC"
                    ]
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "5494febb-d167-46a2-996d-70533eb4d976",
                "status": 200,
                "result": {
                "timezone": "UTC",
                "serverTime": 1655969291181,
                "rateLimits": [{
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200
                },
                {
                    "rateLimitType": "ORDERS",
                    "interval": "SECOND",
                    "intervalNum": 10,
                    "limit": 50
                },
                {
                    "rateLimitType": "ORDERS",
                    "interval": "DAY",
                    "intervalNum": 1,
                    "limit": 160000
                },
                {
                    "rateLimitType": "RAW_REQUESTS",
                    "interval": "MINUTE",
                    "intervalNum": 5,
                    "limit": 6100
                }],
                "exchangeFilters": [],
                "symbols": [{
                    "symbol": "BNBBTC",
                    "status": "TRADING",
                    "baseAsset": "BNB",
                    "baseAssetPrecision": 8,
                    "quoteAsset": "BTC",
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "baseCommissionPrecision": 8,
                    "quoteCommissionPrecision": 8,
                    "orderTypes": [
                        "LIMIT",
                        "LIMIT_MAKER",
                        "MARKET",
                        "STOP_LOSS_LIMIT",
                        "TAKE_PROFIT_LIMIT"
                    ],
                    "icebergAllowed": true,
                    "ocoAllowed": true,
                    "quoteOrderQtyMarketAllowed": true,
                    "allowTrailingStop": true,
                    "cancelReplaceAllowed": true,
                    "isSpotTradingAllowed": true,
                    "isMarginTradingAllowed": true,
                    "filters": [{
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.00000100",
                        "maxPrice": "100000.00000000",
                        "tickSize": "0.00000100"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00100000",
                        "maxQty": "100000.00000000",
                        "stepSize": "0.00100000"
                    }],
                    "permissions": [
                        "SPOT",
                        "MARGIN",
                        "TRD_GRP_004"
                    ],
                    "defaultSelfTradePreventionMode": "NONE",
                    "allowedSelfTradePreventionModes": [
                        "NONE"
                    ]
                }]},
                "rateLimits": [{
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200,
                    "count": 10
                }]
            }

        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_exchange_info() - error_msg: No `stream_id` provided"
                    f" or found!"
                )
                return False
        params = {}
        if symbol is not None:
            params = {"symbol": symbol}
        if symbols is not None:
            symbols = [symbol.upper() for symbol in symbols]
            params = {"symbols": symbols}
        if permissions is not None:
            params = {"permissions": permissions}
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        method = "exchangeInfo"
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_exchange_info() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_historical_trades(
        self,
        process_response=None,
        from_id: int = None,
        limit: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Historical trades

        Get historical trades.

        Weight(IP): 25

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#historical-trades

        :param from_id: Trade ID to begin at. If from_id is not specified, the most recent trades are returned.
        :type from_id: int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The selected symbol
        :type symbol: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "cffc9c7d-4efc-4ce0-b587-6b87448f052a",
              "method": "trades.historical",
              "params": {
                "symbol": "BNBBTC",
                "fromId": 0,
                "limit": 1
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "cffc9c7d-4efc-4ce0-b587-6b87448f052a",
              "status": 200,
              "result": [
                {
                  "id": 0,
                  "price": "0.00005000",
                  "qty": "40.00000000",
                  "quoteQty": "0.00200000",
                  "time": 1500004800376,
                  "isBuyerMaker": true,
                  "isBestMatch": true
                }
              ],
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 10
                }
              ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_historical_trades() - error_msg: No `stream_id` "
                    f"provided or found!"
                )
                return False

        params = {"symbol": symbol.upper()}

        if limit is not None:
            params["limit"] = str(limit)
        if from_id is not None:
            params["fromId"] = str(from_id)

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        payload = {"id": request_id, "method": "trades.historical", "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_historical_trades() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_klines(
        self,
        process_response=None,
        end_time: int = None,
        interval: Optional[
            Literal[
                "1s",
                "1m",
                "3m",
                "5m",
                "15m",
                "30m",
                "1h",
                "2h",
                "4h",
                "6h",
                "8h",
                "12h",
                "1d",
                "3d",
                "1w",
                "1M",
            ]
        ] = None,
        limit: int = None,
        request_id: str = None,
        return_response: bool = False,
        start_time: int = None,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
        time_zone: str = None,
    ) -> Union[str, dict, bool]:
        """
        Klines

        Get klines (candlestick bars).

        Klines are uniquely identified by their open & close time.

        If you need access to real-time trading activity, please consider using
        'WebSocket Streams <https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream>'_

          - <symbol>@kline_<interval>

        If you need historical kline data, please consider using data.binance.vision:
        https://github.com/binance/binance-public-data/#klines

        Weight(IP): 2

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#klines

        :param end_time: If start_time, end_time are not specified, the most recent klines are returned.
        :type end_time: int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param interval: Interval of the klines: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        :type interval: str
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param start_time: If start_time, end_time are not specified, the most recent klines are returned.
        :type start_time: int
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The selected symbol
        :type symbol: str
        :param time_zone: Default: 0 (UTC)
                          Supported values for time_zone: Hours and minutes (e.g. -1:00, 05:45),
                          Only hours (e.g. 0, 8, 4), Accepted range is strictly [-12:00 to +14:00] inclusive

                          If time_zone provided, kline intervals are interpreted in that timezone instead of UTC.

                          Note: start_time and end_time are always interpreted in UTC, regardless of time_zone.
        :type time_zone: int

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "1dbbeb56-8eea-466a-8f6e-86bdcfa2fc0b",
              "method": "klines",
              "params": {
                "symbol": "BNBBTC",
                "interval": "1h",
                "startTime": 1655969280000,
                "limit": 1
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "1dbbeb56-8eea-466a-8f6e-86bdcfa2fc0b",
              "status": 200,
              "result": [
                [
                  1655971200000,      // Kline open time
                  "0.01086000",       // Open price
                  "0.01086600",       // High price
                  "0.01083600",       // Low price
                  "0.01083800",       // Close price
                  "2290.53800000",    // Volume
                  1655974799999,      // Kline close time
                  "24.85074442",      // Quote asset volume
                  2283,               // Number of trades
                  "1171.64000000",    // Taker buy base asset volume
                  "12.71225884",      // Taker buy quote asset volume
                  "0"                 // Unused field, ignore
                ]
              ],
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 2
                }
              ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_klines() - error_msg: No `stream_id` provided "
                    f"or found!"
                )
                return False

        params = {"symbol": symbol.upper()}

        if interval is not None:
            params["interval"] = str(interval)
        if limit is not None:
            params["limit"] = str(limit)
        if end_time is not None:
            params["endTime"] = str(end_time)
        if start_time is not None:
            params["startTime"] = str(start_time)
        if time_zone is not None:
            params["timeZone"] = str(time_zone)

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "klines", "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_klines() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_listen_key(
        self,
        process_response=None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Start user data stream (USER_STREAM)

        Get a listenKey to start a UserDataStream.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#start-user-data-stream-user_stream

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "d3df8a61-98ea-4fe0-8f4e-0fcea5d418b0",
              "method": "userDataStream.start",
              "params": {
                "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "d3df8a61-98ea-4fe0-8f4e-0fcea5d418b0",
              "status": 200,
              "result": {
                "listenKey": "xs0mRXdAKlIPDRFrlPcw0qI41Eh3ixNntmymGyhrhgqo7L6FuLaWArTD7RLP"
              },
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 2
                }
              ]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_listen_key() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        method = "userDataStream.start"
        params = {"apiKey": self._manager.stream_list[stream_id]["api_key"]}

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_listen_key() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_open_orders(
        self,
        process_response=None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Current open orders (USER_DATA)

        Query execution status of all open orders.

        Open orders are always returned as a flat list. If all symbols are requested, use the symbol field to tell
        which symbol the orders belong to.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#current-open-orders-user_data

        If you need to continuously monitor order status updates, please consider using
        `WebSocket Streams <https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream>`__

          - `userData`

          - `executionReport` user data stream event

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: If omitted, open orders for all symbols are returned.
        :type symbol: str

        :return: str, dict, bool

        Message Sent:

        .. code-block:: json

            {
                "id": "55f07876-4f6f-4c47-87dc-43e5fff3f2e7",
                "method": "openOrders.status",
                "params": {
                    "symbol": "BTCUSDT",
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "d632b3fdb8a81dd44f82c7c901833309dd714fe508772a89b0a35b0ee0c48b89",
                    "timestamp": 1660813156812
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "55f07876-4f6f-4c47-87dc-43e5fff3f2e7",
                "status": 200,
                "result": [
                    {
                        "symbol": "BTCUSDT",
                        "orderId": 12569099453,
                        "orderListId": -1,
                        "clientOrderId": "4d96324ff9d44481926157",
                        "price": "23416.10000000",
                        "origQty": "0.00847000",
                        "executedQty": "0.00720000",
                        "cummulativeQuoteQty": "172.43931000",
                        "status": "PARTIALLY_FILLED",
                        "timeInForce": "GTC",
                        "type": "LIMIT",
                        "side": "SELL",
                        "stopPrice": "0.00000000",
                        "icebergQty": "0.00000000",
                        "time": 1660801715639,
                        "updateTime": 1660801717945,
                        "isWorking": true,
                        "workingTime": 1660801715639,
                        "origQuoteOrderQty": "0.00000000",
                        "selfTradePreventionMode": "NONE"
                    }
                ],
                "rateLimits": [
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 3
                    }
                ]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_open_orders() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "timestamp": self._manager.get_timestamp(),
        }

        if symbol is not None:
            params["symbol"] = str(symbol.upper())

        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        method = "openOrders.status"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_open_orders() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_order(
        self,
        order_id: int = None,
        orig_client_order_id: str = None,
        process_response=None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Query order (USER_DATA)

        Check execution status of an order.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#query-order-user_data

        If both `orderId` and `origClientOrderId` parameters are specified, only `orderId` is used and
        `origClientOrderId` is ignored.

        For some historical orders the `cummulativeQuoteQty` response field may be negative, meaning the data is not
        available at this time.

        :param order_id: Lookup order by `orderId`.
        :type order_id: int
        :param orig_client_order_id: Lookup order by `clientOrderId`.
        :type orig_client_order_id: str
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The symbol you want to trade
        :type symbol: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "aa62318a-5a97-4f3b-bdc7-640bbe33b291",
                "method": "order.status",
                "params": {
                    "symbol": "BTCUSDT",
                    "orderId": 12569099453,
                    "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                    "signature": "2c3aab5a078ee4ea465ecd95523b77289f61476c2f238ec10c55ea6cb11a6f35",
                    "timestamp": 1660801720951
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "aa62318a-5a97-4f3b-bdc7-640bbe33b291",
                "status": 200,
                "result": {
                    "symbol": "BTCUSDT",
                    "orderId": 12569099453,
                    "orderListId": -1,
                    "clientOrderId": "4d96324ff9d44481926157",
                    "price": "23416.10000000",
                    "origQty": "0.00847000",
                    "executedQty": "0.00847000",
                    "cummulativeQuoteQty": "198.33521500",
                    "status": "FILLED",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "SELL",
                    "stopPrice": "0.00000000",
                    "trailingDelta": 10,
                    "trailingTime": -1,
                    "icebergQty": "0.00000000",
                    "time": 1660801715639,
                    "updateTime": 1660801717945,
                    "isWorking": true,
                    "workingTime": 1660801715639,
                    "origQuoteOrderQty": "0.00000000",
                    "strategyId": 37463720,
                    "strategyType": 1000000,
                    "selfTradePreventionMode": "NONE",
                    "preventedMatchId": 0,
                    "preventedQuantity": "1.200000"
                },
                "rateLimits": [
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 2
                    }
                ]
            }
        """
        if symbol is None or (order_id is None and orig_client_order_id is None):
            raise ValueError(
                f"Missing mandatory parameter: symbol, order_id/orig_client_order_id"
            )

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_order() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        params = {
            "apiKey": self._manager.stream_list[stream_id]["api_key"],
            "symbol": symbol.upper(),
            "timestamp": self._manager.get_timestamp(),
        }

        if order_id is not None:
            params["orderId"] = int(order_id)
        if orig_client_order_id is not None:
            params["origClientOrderId"] = str(orig_client_order_id)
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        method = "order.status"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_order() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_order_book(
        self,
        process_response=None,
        limit: int = None,
        recv_window: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Order book

        Get current order book.

        Note that this request returns limited market depth.

        If you need to continuously monitor order book updates, please consider using
        'WebSocket Streams <https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream>'_

          - <symbol>@depth<levels>

          - <symbol>@depth

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#order-book

        :param limit: Default 100; max 5000.
        :type limit: int
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param recv_window: An additional parameter, `recvWindow`, may be sent to specify the number of milliseconds
                            after timestamp the request is valid for. If `recvWindow` is not sent, it defaults to 5000.
                            The value cannot be greater than 60000.
        :type recv_window: int
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The selected symbol
        :type symbol: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "5494febb-d167-46a2-996d-70533eb4d976",
                "method": "depth",
                "params": {
                    "symbol": "BNBBTC",
                    "limit": 5
                }
            }

        Response:

        .. code-block:: json

            {
                "id": "5494febb-d167-46a2-996d-70533eb4d976",
                "status": 200,
                "result": {
                    "lastUpdateId": 2731179239,
                    "bids": [
                        [
                            "0.01379900",
                            "3.43200000"
                        ],
                        [
                            "0.01379800",
                            "3.24300000"
                        ],
                        [
                            "0.01379700",
                            "10.45500000"
                        ],
                        [
                            "0.01379600",
                            "3.82100000"
                        ],
                        [
                            "0.01379500",
                            "10.26200000"
                        ]
                    ],
                    "asks": [
                        [
                            "0.01380000",
                            "5.91700000"
                        ],
                        [
                            "0.01380100",
                            "6.01400000"
                        ],
                        [
                            "0.01380200",
                            "0.26800000"
                        ],
                        [
                            "0.01380300",
                            "0.33800000"
                        ],
                        [
                            "0.01380400",
                            "0.26800000"
                        ]
                    ]
                },
                "rateLimits": [
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1200,
                        "count": 1
                    }
                ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_order_book() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        params = {"symbol": symbol.upper()}

        if limit is not None:
            params["limit"] = str(limit)
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "depth", "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_order_book() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_recent_trades(
        self,
        process_response=None,
        limit: int = None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
    ) -> Union[str, dict, bool]:
        """
        Recent trades

        Get recent trades.

        If you need access to real-time trading activity, please consider using
        'WebSocket Streams <https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream>'_

          - <symbol>@trade

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#recent-trades

        :param limit: Default 500; max 1000.
        :type limit: int
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The selected symbol
        :type symbol: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "409a20bd-253d-41db-a6dd-687862a5882f",
              "method": "trades.recent",
              "params": {
                "symbol": "BNBBTC",
                "limit": 1
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "409a20bd-253d-41db-a6dd-687862a5882f",
              "status": 200,
              "result": [
                {
                  "id": 194686783,
                  "price": "0.01361000",
                  "qty": "0.01400000",
                  "quoteQty": "0.00019054",
                  "time": 1660009530807,
                  "isBuyerMaker": true,
                  "isBestMatch": true
                }
              ],
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 2
                }
              ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_recent_trades() - error_msg: No `stream_id` provided "
                    f"or found!"
                )
                return False

        params = {"symbol": symbol.upper()}

        if limit is not None:
            params["limit"] = str(limit)

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "trades.recent", "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_recent_trades() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_server_time(
        self,
        process_response=None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Check server time

        Test connectivity to the WebSocket API and get the current server time.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#check-server-time

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "187d3cb2-942d-484c-8271-4e2141bbadb1",
                "method": "time"
            }


        Response:

        .. code-block:: json

            {
                "id": "187d3cb2-942d-484c-8271-4e2141bbadb1",
                "status": 200,
                "result": {
                    "serverTime": 1656400526260
                },
                "rateLimits": [{
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200,
                    "count": 1
                }]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_server_time() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "time"}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_server_time() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def ping(
        self,
        process_response=None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Test connectivity

        Test connectivity to the WebSocket API.

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#test-connectivity

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
                "id": "4e72973031d8-bff9-8481-c95b-c42414df",
                "method": "ping"
            }

        Response:

        .. code-block:: json

            {
                "id": "4e72973031d8-bff9-8481-c95b-c42414df",
                "status": 200,
                "result": {},
                "rateLimits": [{
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200,
                    "count": 1
                }]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.ping() - error_msg: No `stream_id` provided or "
                    f"found!"
                )
                return False

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "ping"}

        logger.debug(f"BinanceWebSocketApiApiSpot.ping() - Created payload: {payload}")

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_ui_klines(
        self,
        process_response=None,
        end_time: int = None,
        interval: Optional[
            Literal[
                "1s",
                "1m",
                "3m",
                "5m",
                "15m",
                "30m",
                "1h",
                "2h",
                "4h",
                "6h",
                "8h",
                "12h",
                "1d",
                "3d",
                "1w",
                "1M",
            ]
        ] = None,
        limit: int = None,
        request_id: str = None,
        return_response: bool = False,
        start_time: int = None,
        stream_id: str = None,
        stream_label: str = None,
        symbol: str = None,
        time_zone: str = None,
    ) -> Union[str, dict, bool]:
        """
        UI Klines

        Get klines (candlestick bars) optimized for presentation.

        This request is similar to klines, having the same parameters and response. uiKlines return modified kline data, optimized for presentation of candlestick charts.

        Weight(IP): 2

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#ui-klines

        :param end_time: If start_time, end_time are not specified, the most recent klines are returned.
        :type end_time: int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param interval: Interval of the klines: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        :type interval: str
        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param start_time: If start_time, end_time are not specified, the most recent klines are returned.
        :type start_time: int
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str
        :param symbol: The selected symbol
        :type symbol: str
        :param time_zone: Default: 0 (UTC)
                          Supported values for time_zone: Hours and minutes (e.g. -1:00, 05:45),
                          Only hours (e.g. 0, 8, 4), Accepted range is strictly [-12:00 to +14:00] inclusive

                          If time_zone provided, kline intervals are interpreted in that timezone instead of UTC.

                          Note: start_time and end_time are always interpreted in UTC, regardless of time_zone.
        :type time_zone: int

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "b137468a-fb20-4c06-bd6b-625148eec958",
              "method": "uiKlines",
              "params": {
                "symbol": "BNBBTC",
                "interval": "1h",
                "startTime": 1655969280000,
                "limit": 1
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "b137468a-fb20-4c06-bd6b-625148eec958",
              "status": 200,
              "result": [
                [
                  1655971200000,      // Kline open time
                  "0.01086000",       // Open price
                  "0.01086600",       // High price
                  "0.01083600",       // Low price
                  "0.01083800",       // Close price
                  "2290.53800000",    // Volume
                  1655974799999,      // Kline close time
                  "24.85074442",      // Quote asset volume
                  2283,               // Number of trades
                  "1171.64000000",    // Taker buy base asset volume
                  "12.71225884",      // Taker buy quote asset volume
                  "0"                 // Unused field, ignore
                ]
              ],
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 2
                }
              ]
            }
        """
        if symbol is None:
            raise ValueError(f"Missing mandatory parameter: symbol")

        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_ui_klines() - error_msg: No `stream_id` provided "
                    f"or found!"
                )
                return False

        params = {"symbol": symbol.upper()}

        if interval is not None:
            params["interval"] = str(interval)
        if limit is not None:
            params["limit"] = str(limit)
        if end_time is not None:
            params["endTime"] = str(end_time)
        if start_time is not None:
            params["startTime"] = str(start_time)
        if time_zone is not None:
            params["timeZone"] = str(time_zone)

        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )

        payload = {"id": request_id, "method": "uiKlines", "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_ui_klines() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True

    def get_unfilled_order_count(
        self,
        process_response=None,
        request_id: str = None,
        return_response: bool = False,
        stream_id: str = None,
        stream_label: str = None,
    ) -> Union[str, dict, bool]:
        """
        Unfilled Order Count (USER_DATA)

        Query your current unfilled order count for all intervals.

        Weight(IP): 40

        Official documentation:

            - https://binance-docs.github.io/apidocs/websocket_api/en/#unfilled-order-count-user_data

        :param process_response: Provide a function/method to process the received webstream data (callback)
                                 of this specific request.
        :type process_response: function
        :param request_id: Provide a custom id for the request
        :type request_id: str
        :param return_response: If `True` the response of the API request is waited for and returned directly.
                                However, this increases the execution time of the function by the duration until the
                                response is received from the Binance API.
        :type return_response: bool
        :param stream_id: ID of a stream to send the request
        :type stream_id: str
        :param stream_label: Label of a stream to send the request. Only used if `stream_id` is not provided!
        :type stream_label: str

        :return: str, dict, bool

        Message sent:

        .. code-block:: json

            {
              "id": "d3783d8d-f8d1-4d2c-b8a0-b7596af5a664",
              "method": "account.rateLimits.orders",
              "params": {
                "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
                "signature": "76289424d6e288f4dc47d167ac824e859dabf78736f4348abbbac848d719eb94",
                "timestamp": 1660801839500
              }
            }

        Response:

        .. code-block:: json

            {
              "id": "d3783d8d-f8d1-4d2c-b8a0-b7596af5a664",
              "status": 200,
              "result": [
                {
                  "rateLimitType": "ORDERS",
                  "interval": "SECOND",
                  "intervalNum": 10,
                  "limit": 50,
                  "count": 0
                },
                {
                  "rateLimitType": "ORDERS",
                  "interval": "DAY",
                  "intervalNum": 1,
                  "limit": 160000,
                  "count": 0
                }
              ],
              "rateLimits": [
                {
                  "rateLimitType": "REQUEST_WEIGHT",
                  "interval": "MINUTE",
                  "intervalNum": 1,
                  "limit": 6000,
                  "count": 40
                }
              ]
            }
        """
        if stream_id is None:
            if stream_label is not None:
                stream_id = self._manager.get_stream_id_by_label(
                    stream_label=stream_label
                )
            else:
                stream_id = self._manager.get_the_one_active_websocket_api()
            if stream_id is None:
                logger.critical(
                    f"BinanceWebSocketApiApiSpot.get_unfilled_order_count() - error_msg: No `stream_id` "
                    f"provided or found!"
                )
                return False

        params = {"apiKey": self._manager.stream_list[stream_id]["api_key"]}
        method = "account.rateLimits.orders"
        api_secret = self._manager.stream_list[stream_id]["api_secret"]
        request_id = (
            self._manager.get_new_uuid_id() if request_id is None else request_id
        )
        params["signature"] = self._manager.generate_signature(
            api_secret=api_secret, data=params
        )

        payload = {"id": request_id, "method": method, "params": params}

        logger.debug(
            f"BinanceWebSocketApiApiSpot.get_unfilled_order_count() - Created payload: {payload}"
        )

        if (
            self._manager.send_with_stream(stream_id=stream_id, payload=payload)
            is False
        ):
            self._manager.add_payload_to_stream(stream_id=stream_id, payload=payload)

        if process_response is not None:
            with self._manager.process_response_lock:
                entry = {"callback_function": process_response}
                self._manager.process_response[request_id] = entry

        if return_response is True:
            with self._manager.return_response_lock:
                entry = {"event_return_response": threading.Event()}
                self._manager.return_response[request_id] = entry
            self._manager.return_response[request_id]["event_return_response"].wait()
            with self._manager.return_response_lock:
                response_value = self._manager.return_response[request_id][
                    "response_value"
                ]
                del self._manager.return_response[request_id]
            return response_value

        return True
