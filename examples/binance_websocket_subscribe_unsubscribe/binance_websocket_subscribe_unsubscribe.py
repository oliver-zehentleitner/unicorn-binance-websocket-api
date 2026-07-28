#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os
import time

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.INFO,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


class BinanceDataProcessor:
    def __init__(self):
        self.ubwa = BinanceWebSocketApiManager(
            enable_stream_signal_buffer=True,
            process_stream_signals=self.receive_stream_signal,
            output_default="UnicornFy",
        )
        self.show_time_diff = None

    async def main(self):
        print(f"Creating a stream and subscribing to trades of market 'ethusdt' ...")
        stream_id = self.ubwa.create_stream(
            channels="trade",
            markets="ethusdt",
            stream_label="MyStream",
            process_asyncio_queue=self.process_data,
        )
        time.sleep(1)
        time_start = time.time()
        self.show_time_diff = time_start
        print(f"Subscribing to market 'btcusdt' ...", end="")
        self.ubwa.subscribe_to_stream(stream_id=stream_id, markets="btcusdt")
        print(f" ({(time.time() - time_start)})")
        time_start = time.time()
        print(f"Unsubscribing from market 'ethusdt' ...", end="")
        self.ubwa.unsubscribe_from_stream(stream_id=stream_id, markets="ethusdt")
        print(f" ({(time.time() - time_start)})")
        time.sleep(3)
        self.ubwa.stop_manager()

    async def process_data(self, stream_id=None):
        print(
            f"Processing data of '{self.ubwa.get_stream_label(stream_id=stream_id)}' ..."
        )
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            if self.show_time_diff is not None:
                if data.get("symbol") == "BTCUSDT":
                    print(
                        f"Time between subscription and first BTCUSDT receive: ({(time.time() - self.show_time_diff)})"
                    )
                    self.show_time_diff = None
                try:
                    if data["result"] is None:
                        print(f"Result: {data}")
                except KeyError:
                    pass
                try:
                    if data["error"] is None:
                        print(f"Error: {data}")
                except KeyError:
                    pass
                if data.get("event_type") == "trade":
                    print(f"Data: {data}")
            self.ubwa.asyncio_queue_task_done(stream_id)

    def receive_stream_signal(
        self, signal_type=None, stream_id=None, data_record=None, error_msg=None
    ):
        print(
            f"Received stream_signal for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record} - {error_msg}"
        )


if __name__ == "__main__":
    bdp = BinanceDataProcessor()
    try:
        asyncio.run(bdp.main())
    except KeyboardInterrupt:
        print("\r\n")
    print("Gracefully stopping ...")
    bdp.ubwa.stop_manager()
