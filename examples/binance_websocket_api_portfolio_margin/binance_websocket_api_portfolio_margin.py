#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os

api_key = ""
api_secret = ""
exchange = "binance.com-portfolio_margin"

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(level=logging.INFO,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")


# Binance Portfolio Margin user data streams live at
# `wss://fstream.binance.com/pm/ws/<listenKey>` — the listenKey is acquired,
# kept alive and closed via the Portfolio Margin REST API at
# `https://papi.binance.com/papi/v1/listenKey` (POST/PUT/DELETE).
#
# UBWA/UBRA currently only cover this listenKey lifecycle for Portfolio
# Margin, not the full REST surface (account, positions, orders, etc.).
#
# https://developers.binance.com/docs/derivatives/portfolio-margin/user-data-streams


class BinancePortfolioMarginUserDataProcessor:
    def __init__(self):
        self.ubwa = BinanceWebSocketApiManager(exchange=exchange,
                                               enable_stream_signal_buffer=True,
                                               process_stream_signals=self.receive_stream_signal,
                                               output_default="dict")

    async def main(self):
        self.ubwa.create_stream('arr', '!userData',
                                api_key=api_key, api_secret=api_secret,
                                process_asyncio_queue=self.process_userdata,
                                stream_label="PM_UserData")

        await asyncio.sleep(5)
        while self.ubwa.is_manager_stopping() is False:
            self.ubwa.print_summary()
            await asyncio.sleep(600)

    async def process_userdata(self, stream_id=None):
        print(f"Processing data of {self.ubwa.get_stream_label(stream_id=stream_id)} ...")
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            print(f"data: {data}")
            self.ubwa.asyncio_queue_task_done(stream_id)

    def receive_stream_signal(self, signal_type=None, stream_id=None, data_record=None, error_msg=None):
        print(f"Received stream_signal for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
              f"{signal_type} - {stream_id} - {data_record} - {error_msg}")


if __name__ == "__main__":
    bdp = BinancePortfolioMarginUserDataProcessor()
    try:
        asyncio.run(bdp.main())
    except KeyboardInterrupt:
        print("\r\n")
    except Exception as e:
        print(f"\r\nError: {e}")
    print("Gracefully stopping ...")
    bdp.ubwa.stop_manager()
