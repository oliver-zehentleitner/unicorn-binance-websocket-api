#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from dotenv import load_dotenv
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
from unicorn_binance_rest_api import BinanceRestApiManager
import asyncio
import logging
import os

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.ERROR,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)
load_dotenv()


class BinanceDataProcessor:
    def __init__(self, ubwa_manager: BinanceWebSocketApiManager = None):
        self.footer: str = ""
        self.markets_limit = 10
        self.title: str = "UBWA Demo"
        self.ubwa: BinanceWebSocketApiManager = ubwa_manager

    async def main(self):
        self.ubwa.create_stream(
            channels=["!userData"],
            markets=["arr"],
            stream_label="!userData",
            api_key=os.getenv("BINANCE_API_KEY"),
            api_secret=os.getenv("BINANCE_API_SECRET"),
            process_asyncio_queue=self.process_data,
        )

        self.ubwa.create_stream(
            channels="!miniTicker",
            markets="arr",
            stream_label="!miniTicker",
            process_asyncio_queue=self.process_data,
        )

        with BinanceRestApiManager() as ubra:
            markets: list = [
                item["symbol"]
                for item in ubra.get_all_tickers()
                if item["symbol"].endswith("USDT")
            ]
        channels: list = ["aggTrade", "kline_1m", "depth5"]
        for channel in channels:
            self.ubwa.create_stream(
                channels=channel,
                markets=markets[: self.markets_limit],
                process_asyncio_queue=self.process_data,
                stream_label=channel,
            )

        while self.ubwa.is_manager_stopping() is False:
            if os.getenv("EXPORT_TO_PNG") is None:
                self.ubwa.print_summary(footer=self.footer, title=self.title)
                await asyncio.sleep(1)
            else:
                self.ubwa.print_summary_to_png(
                    height_per_row=13.5,
                    print_summary_export_path="/var/www/html/",
                    footer=self.footer,
                    title=self.title,
                )
                await asyncio.sleep(10)

    async def process_data(self, stream_id=None):
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            self.ubwa.asyncio_queue_task_done(stream_id)


if __name__ == "__main__":
    with BinanceWebSocketApiManager() as ubwa:
        bdp = BinanceDataProcessor(ubwa_manager=ubwa)
        try:
            asyncio.run(bdp.main())
        except KeyboardInterrupt:
            print("\r\nGracefully stopping ...")
        except Exception as e:
            print(f"\r\nERROR: {e}")
            print("Gracefully stopping ...")
