#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os

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
            process_stream_signals=self.receive_stream_signal
        )

    async def main(self):
        print(
            f"In the Wiki you will find detailed information about the respective stream signal types: "
            f"https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signals%60"
        )
        print(f"\r\nExample of invalid API credentials:")
        self.ubwa.create_stream(
            channels="arr",
            markets="!userData",
            stream_label="INVALID_CREDENTIALS",
            api_key="something",
            api_secret="wrong",
        )
        await asyncio.sleep(3)
        print(f"\r\nExample of a healthy stream:")
        stream_id = self.ubwa.create_stream(
            channels="trade", markets="btcusdt", stream_label="HEALTHY"
        )
        print(f"Waiting 10 seconds and then stop the stream ...")
        await asyncio.sleep(10)
        self.ubwa.stop_stream(stream_id=stream_id)
        await asyncio.sleep(5)

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
        print("\r\nGracefully stopping ...")
    except Exception as e:
        print(f"\r\nError: {e}\r\nGracefully stopping ...")
    bdp.ubwa.stop_manager()
