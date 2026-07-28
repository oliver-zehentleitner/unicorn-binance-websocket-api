#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from dotenv import load_dotenv
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os

market = "BTCUSDT"


async def binance_api(ubwa):
    async def handle_socket_message(stream_id=None):
        while ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await ubwa.get_stream_data_from_asyncio_queue(stream_id=stream_id)
            print(f"Received data:\r\n{data}\r\n")

    print(f"Starting the stream:")
    api_stream = ubwa.create_stream(
        api=True,
        api_key=os.getenv("BINANCE_API_KEY"),
        api_secret=os.getenv("BINANCE_API_SECRET"),
        stream_label="Bobs Spot Websocket API",
        process_asyncio_queue=handle_socket_message,
    )

    print(f"Executing API requests on Binance Spot:")

    current_average_price = ubwa.api.spot.get_current_average_price(
        stream_id=api_stream, symbol=market, return_response=True
    )
    print(f"current_average_price: {current_average_price}\r\n")

    ubwa.api.spot.get_listen_key(stream_id=api_stream)

    ubwa.api.spot.get_server_time(stream_id=api_stream)

    server_time = ubwa.api.spot.get_server_time(
        stream_id=api_stream, return_response=True
    )
    print(f"Server Time: {server_time['result']['serverTime']}\r\n")

    account_status = ubwa.api.spot.get_account_status(
        stream_id=api_stream, return_response=True
    )
    print(f"Status of account_status request: {account_status['status']}\r\n")

    orig_client_order_id = ubwa.api.spot.create_order(
        stream_id=api_stream,
        price=1.0,
        order_type="LIMIT",
        quantity=15.0,
        side="SELL",
        symbol=market,
    )

    ubwa.api.spot.create_test_order(
        stream_id=api_stream,
        price=1.2,
        order_type="LIMIT",
        quantity=12.0,
        side="SELL",
        symbol=market,
    )

    ubwa.api.spot.ping(stream_id=api_stream)

    exchange_info = ubwa.api.spot.get_exchange_info(
        stream_id=api_stream,
        symbols=[
            market,
        ],
        return_response=True,
    )
    print(f"Status of exchange_info request: {exchange_info['status']}\r\n")

    order_book = ubwa.api.spot.get_order_book(
        stream_id=api_stream, symbol=market, limit=10, return_response=True
    )
    print(
        f"Orderbook, lastUpdateId={order_book['result']['lastUpdateId']}: {order_book['result']['asks']}, "
        f"{order_book['result']['bids']}\r\n"
    )

    klines = ubwa.api.spot.get_klines(
        stream_id=api_stream, symbol=market, interval="1m", return_response=True
    )
    print(f"A few klines: {klines['result'][:5]}\r\n")

    ui_klines = ubwa.api.spot.get_ui_klines(
        stream_id=api_stream, symbol=market, interval="1d", return_response=True
    )
    print(f"A few ui_klines: {ui_klines['result'][:5]}\r\n")

    ubwa.api.spot.get_open_orders(stream_id=api_stream, symbol=market)

    ubwa.api.spot.get_open_orders(stream_id=api_stream)

    replaced_client_order_id = ubwa.api.spot.cancel_and_replace_order(
        stream_id=api_stream,
        price=1.1,
        order_type="LIMIT",
        quantity=15.0,
        side="SELL",
        symbol=market,
        cancel_orig_client_order_id=orig_client_order_id,
    )

    ubwa.api.spot.cancel_order(
        stream_id=api_stream,
        symbol=market,
        orig_client_order_id=replaced_client_order_id,
    )

    ubwa.api.spot.cancel_open_orders(stream_id=api_stream, symbol=market)

    ubwa.api.spot.get_order(
        stream_id=api_stream,
        symbol=market,
        orig_client_order_id=replaced_client_order_id,
    )

    aggregate_trades = ubwa.api.spot.get_aggregate_trades(
        stream_id=api_stream, symbol=market, return_response=True
    )
    print(f"aggregate_trades: {aggregate_trades['result'][:5]}\r\n")

    historical_trades = ubwa.api.spot.get_historical_trades(
        stream_id=api_stream, symbol=market, return_response=True
    )
    print(f"historical_trades: {historical_trades['result'][:5]}\r\n")

    recent_trades = ubwa.api.spot.get_recent_trades(
        stream_id=api_stream, symbol=market, return_response=True
    )
    print(f"recent_trades: {recent_trades['result'][:5]}\r\n")
    print(f"Stopping!\r\n")


def process_stream_signal(
    signal_type=None, stream_id=None, data_record=None, error_msg=None
):
    # More info about `stream_signals`:
    # https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signals%60
    print(
        f"Received stream_signal for stream '{ubwa_manager.get_stream_label(stream_id=stream_id)}': "
        f"{signal_type} - {stream_id} - {data_record} - {error_msg}\r\n"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )

    # Loading os.getenv() vars from .env
    load_dotenv()

    with BinanceWebSocketApiManager(
        exchange="binance.com",
        output_default="dict",
        process_stream_signals=process_stream_signal,
    ) as ubwa_manager:
        try:
            asyncio.run(binance_api(ubwa_manager))
        except KeyboardInterrupt:
            print("\r\nGracefully stopping the websocket manager...")
