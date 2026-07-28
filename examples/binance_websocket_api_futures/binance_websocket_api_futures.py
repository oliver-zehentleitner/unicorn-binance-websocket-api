#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from dotenv import load_dotenv
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os

market = "BTCUSDC"


async def binance_api(ubwa):
    async def handle_socket_message(stream_id=None):
        while ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await ubwa.get_stream_data_from_asyncio_queue(stream_id=stream_id)
            print(f"received data:\r\n{data}\r\n")

    print(f"Starting the stream ...\r\n")
    api_stream = ubwa.create_stream(
        api=True,
        api_key=os.getenv("BINANCE_API_KEY"),
        api_secret=os.getenv("BINANCE_API_SECRET"),
        stream_label="Bobs Future Websocket API",
        process_asyncio_queue=handle_socket_message,
    )

    print(f"Executing API requests on Binance Futures:\r\n")

    account_status = ubwa.api.futures.get_account_status(
        stream_id=api_stream, return_response=True
    )
    print(f"Status of account_status request: {account_status['status']}\r\n")

    ubwa.api.futures.get_listen_key(stream_id=api_stream)

    ubwa.api.futures.get_server_time(stream_id=api_stream)

    print(
        f"Account balance: "
        f"{str(ubwa.api.futures.get_account_balance(stream_id=api_stream, return_response=True))[:100]} ...\r\n"
    )

    print(
        f"Account position: "
        f"{str(ubwa.api.futures.get_account_position(stream_id=api_stream, return_response=True))[:100]} ...\r\n"
    )

    print(
        f"Account status: "
        f"{str(ubwa.api.futures.get_account_status(stream_id=api_stream, return_response=True))[:100]} ...\r\n"
    )

    print(
        f"Ticker order book: "
        f"{str(ubwa.api.futures.get_ticker_order_book(stream_id=api_stream, 
                                                        symbol=market, 
                                                        return_response=True))[:100]} ...\r\n"
    )

    print(
        f"Ticker price: "
        f"{str(ubwa.api.futures.get_ticker_price(stream_id=api_stream, 
                                                   symbol=market,
                                                   return_response=True))[:100]} ...\r\n"
    )

    orig_client_order_id = ubwa.api.futures.create_order(
        stream_id=api_stream,
        price=1.0,
        order_type="LIMIT",
        quantity=15.0,
        side="SELL",
        symbol=market,
        time_in_force="GTC",
    )

    order_book = ubwa.api.futures.get_order_book(
        stream_id=api_stream, symbol=market, limit=10, return_response=True
    )
    print(
        f"Orderbook, lastUpdateId={order_book['result']['lastUpdateId']}: \r\n{order_book['result']}\r\n"
    )

    ubwa.api.futures.get_order(
        stream_id=api_stream, symbol=market, orig_client_order_id=orig_client_order_id
    )

    new_orig_client_order_id = ubwa.api.futures.modify_order(
        stream_id=api_stream,
        symbol=market,
        quantity=15.0,
        side="SELL",
        orig_client_order_id=orig_client_order_id,
    )

    ubwa.api.futures.cancel_order(
        stream_id=api_stream,
        symbol=market,
        orig_client_order_id=new_orig_client_order_id,
    )

    ubwa.api.futures.get_order(
        stream_id=api_stream,
        symbol=market,
        orig_client_order_id=new_orig_client_order_id,
    )

    print(
        f"PING: {ubwa.api.futures.ping(stream_id=api_stream, return_response=True)}\r\n"
    )

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
    load_dotenv()

    with BinanceWebSocketApiManager(
        exchange="binance.com-futures-testnet",
        output_default="dict",
        process_stream_signals=process_stream_signal,
    ) as ubwa_manager:
        try:
            asyncio.run(binance_api(ubwa_manager))
        except KeyboardInterrupt:
            print("\r\nGracefully stopping the websocket manager...")
