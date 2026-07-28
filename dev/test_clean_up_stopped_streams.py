from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os
import time

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


class BinanceDataProcessor:
    def __init__(self, print_new_data=False):
        self.example_database = []
        self.print_new_data = print_new_data
        self.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com",
            output_default="UnicornFy",
            enable_stream_signal_buffer=True,
            process_stream_signals=self.processing_of_stream_signals,
            auto_data_cleanup_stopped_streams=True,
        )

    async def processing_of_new_data_async(self, data):
        self.example_database.append(data)
        if self.print_new_data is True:
            print(f"Data record received and added to the database: {data}")
        await asyncio.sleep(1)

    def processing_of_stream_signals(
        self, signal_type=None, stream_id=None, data_record=None, error_msg=None
    ):
        # More info about `stream_signals`:
        # https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signal_buffer%60
        print(
            f"Received STREAM SIGNAL for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record} - {error_msg}"
        )

    async def start(self):
        stream_id0 = self.ubwa.create_stream(
            channels=["trade", "kline_1m", "depth5"],
            markets=["btcusdt", "ethusdt", "bnbusdt"],
            process_stream_data_async=self.processing_of_new_data_async,
            stream_label="multiplex0",
        )
        stream_id1 = self.ubwa.create_stream(
            channels=["trade", "kline_1m", "depth5"],
            markets=["btcusdt", "ethusdt", "bnbusdt"],
            process_stream_data_async=self.processing_of_new_data_async,
            stream_label="multiplex1",
        )
        stream_id2 = self.ubwa.create_stream(
            channels=["trade", "kline_1m", "depth5"],
            markets=["btcusdt", "ethusdt", "bnbusdt"],
            process_stream_data_async=self.processing_of_new_data_async,
            stream_label="multiplex2",
        )
        stream_id3 = self.ubwa.create_stream(
            channels=["trade", "kline_1m", "depth5"],
            markets=["btcusdt", "ethusdt", "bnbusdt"],
            process_stream_data_async=self.processing_of_new_data_async,
            stream_label="multiplex3",
        )
        time.sleep(5)
        self.ubwa.stop_stream(stream_id1)
        self.ubwa.stop_stream(stream_id2)

        if self.ubwa.wait_till_stream_has_stopped(stream_id1):
            print(f"Stream {stream_id1} has stopped!")
        else:
            print(f"Stream {stream_id1} did not stop within timeout frame.")

        while self.ubwa.is_manager_stopping() is False:
            self.ubwa.print_summary()
            await asyncio.sleep(1)


if __name__ == "__main__":
    bdp = BinanceDataProcessor(print_new_data=False)
    try:
        asyncio.run(bdp.start())
    except KeyboardInterrupt:
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
    except Exception as error_msg:
        print(f"\r\nERROR: {error_msg}")
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
