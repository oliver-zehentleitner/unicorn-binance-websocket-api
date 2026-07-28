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
    def __init__(
        self,
        print_new_data=False,
        start_mark_price=True,
        start_multiplex=True,
        start_userdata=True,
    ):
        self.api_key = "YOUR_BINANCE_FUTURE_API_KEY"
        self.api_secret = "YOUR_BINANCE_FUTURE_API_SECRET"
        self.example_database = []
        self.print_new_data = print_new_data
        self.start_mark_price = start_mark_price
        self.start_multiplex = start_multiplex
        self.start_userdata = start_userdata
        self.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com-futures",
            auto_data_cleanup_stopped_streams=True,
            enable_stream_signal_buffer=True,
            output_default="dict",
            process_stream_signals=self.receive_stream_signal,
        )

    async def main(self):
        if self.start_mark_price is True:
            self.ubwa.create_stream(
                channels="!markPrice",
                markets="arr@1s",
                process_asyncio_queue=self.processing_of_new_data,
                stream_label="mark_price",
            )

        if self.start_multiplex is True:
            self.ubwa.create_stream(
                channels=["kline_1m", "depth5", "markPrice", "bookTicker", "ticker"],
                markets=["btcusdt", "ethusdt"],
                process_asyncio_queue=self.processing_of_new_data,
                stream_label="multiplex",
            )

        if self.start_userdata is True:
            self.ubwa.create_stream(
                api_key=self.api_key,
                api_secret=self.api_secret,
                channels=["arr"],
                markets=["!userData"],
                process_asyncio_queue=self.processing_of_new_data,
                stream_label="userData",
            )

        while self.ubwa.is_manager_stopping() is False:
            await asyncio.sleep(1)
            stream_info = {
                "mark_price": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("mark_price")
                ),
                "multiplex": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("multiplex")
                ),
                "userData": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("userData")
                ),
            }
            status_text = ""
            if self.start_mark_price is True:
                status_text += (
                    f"\tStream 'mark_price' is {stream_info['mark_price']['status']} "
                    f"(last_stream_signal={stream_info['mark_price']['last_stream_signal']})\r\n"
                )
            if self.start_multiplex is True:
                status_text += (
                    f"\tStream 'multiplex' is {stream_info['multiplex']['status']} "
                    f"(last_stream_signal={stream_info['multiplex']['last_stream_signal']})\r\n"
                )
            if self.start_userdata is True:
                status_text += (
                    f"\tStream 'userData' is {stream_info['userData']['status']} "
                    f"(last_stream_signal={stream_info['userData']['last_stream_signal']})\r\n"
                )
            print(
                f"Status:\r\n\tStored {len(self.example_database)} data records in `self.example_database`\r\n"
                f"{status_text}"
            )

    async def processing_of_new_data(self, stream_id=None):
        print(
            f"Saving the data from webstream {self.ubwa.get_stream_label(stream_id=stream_id)} to the database ..."
        )
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            self.example_database.append(data)
            if self.print_new_data is True:
                print(
                    f"Received data by stream `{self.ubwa.get_stream_label(stream_id=stream_id)}`: {data}"
                )
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
    bdp = BinanceDataProcessor(
        print_new_data=True,
        start_mark_price=True,
        start_multiplex=True,
        start_userdata=True,
    )
    try:
        asyncio.run(bdp.main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
    except Exception as e:
        print(f"\r\nERROR: {e}")
        print("Gracefully stopping ...")
    bdp.ubwa.stop_manager()
