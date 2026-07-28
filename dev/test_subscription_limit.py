#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_binance_websocket_api import (
    BinanceWebSocketApiManager,
    MaximumSubscriptionsExceeded,
)
from unicorn_binance_rest_api import BinanceRestApiManager
import asyncio
import logging
import os
import traceback

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


class BinanceDataProcessor:
    def __init__(self):
        self.ubwa = BinanceWebSocketApiManager(
            enable_stream_signal_buffer=True,
            process_stream_signals=self.receive_stream_signal,
        )

    async def main(self):
        with BinanceRestApiManager() as ubra:
            markets: list = [item["symbol"] for item in ubra.get_all_tickers()]
            stream_id = self.ubwa.create_stream(
                channels="trade",
                markets=markets[:500],
                process_asyncio_queue=self.process_data,
            )
        try:
            self.ubwa.subscribe_to_stream(
                stream_id=stream_id, channels=["kline_1m", "depth20"]
            )
        except MaximumSubscriptionsExceeded as error_msg:
            print(f"ERROR: {error_msg}")
        while self.ubwa.is_manager_stopping() is False:
            await asyncio.sleep(1)

    async def process_data(self, stream_id=None):
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            self.ubwa.asyncio_queue_task_done(stream_id)

    def receive_stream_signal(
        self, signal_type=None, stream_id=None, data_record=None, error_msg=None
    ):
        # More info about `stream_signals`:
        # https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signals%60
        print(
            f"Received stream_signal for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record} - {error_msg}"
        )


if __name__ == "__main__":
    bdp = BinanceDataProcessor()
    try:
        asyncio.run(bdp.main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
    except Exception as e:
        print(f"\r\nError: {e}")
        print("Gracefully stopping ...")
    bdp.ubwa.stop_manager()
