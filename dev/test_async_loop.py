from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


class BinanceDataProcessor:
    def __init__(
        self, print_new_data=False, start_multiplex=True, start_userdata_a=True
    ):
        self.api_key = "YOUR_BINANCE_API_KEY"
        self.api_secret = "YOUR_BINANCE_API_SECRET"
        self.example_database = []
        self.print_new_data = print_new_data
        self.start_multiplex = start_multiplex
        self.start_userdata_a = start_userdata_a
        self.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com",
            output_default="UnicornFy",
            enable_stream_signal_buffer=True,
            process_stream_signals=self.processing_of_stream_signals,
        )

    def processing_of_new_data(self, data):
        self.example_database.append(data)
        if self.print_new_data is True:
            print(f"Data record received and added to the database: {data}")

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
        if self.start_multiplex is True:
            self.ubwa.create_stream(
                channels=["trade", "kline_1m", "depth5"],
                markets=["btcusdt", "ethusdt", "bnbusdt"],
                process_stream_data=self.processing_of_new_data,
                stream_label="multiplex",
            )

        if self.start_userdata_a is True:
            self.ubwa.create_stream(
                api_key=self.api_key,
                api_secret=self.api_secret,
                channels=["arr"],
                markets=["!userData"],
                process_stream_data=self.processing_of_new_data,
                stream_label="userData_A",
            )

        while self.ubwa.is_manager_stopping() is False:
            await asyncio.sleep(1)
            stream_info = {
                "multiplex": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("multiplex")
                ),
                "userData_A": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("userData_A")
                ),
            }

            status_text = ""
            if self.start_multiplex is True:
                status_text += (
                    f"\tStream 'multiplex' is {stream_info['multiplex']['status']} "
                    f"(last_stream_signal={stream_info['multiplex']['last_stream_signal']})\r\n"
                )
            if self.start_userdata_a is True:
                status_text += (
                    f"\tStream 'userData_A' is {stream_info['userData_A']['status']} "
                    f"(last_stream_signal={stream_info['userData_A']['last_stream_signal']})\r\n"
                )

            print(
                f"Status:\r\n\tStored {len(self.example_database)} data records in `self.example_database`\r\n"
                f"{status_text}"
            )


if __name__ == "__main__":
    bdp = BinanceDataProcessor(
        print_new_data=True, start_multiplex=False, start_userdata_a=True
    )
    try:
        asyncio.run(bdp.start())
    except KeyboardInterrupt:
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
    except Exception as error_msg:
        print(f"\r\nERROR: {error_msg}")
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
