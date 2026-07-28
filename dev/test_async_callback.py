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
    def __init__(self, print_new_data=False):
        self.example_database = []
        self.print_new_data = print_new_data
        self.ubwa = BinanceWebSocketApiManager(
            exchange="binance.com",
            output_default="UnicornFy",
            enable_stream_signal_buffer=True,
            process_stream_signals=self.processing_of_stream_signals,
        )

    async def processing_of_new_data_async(self, data):
        self.example_database.append(data)
        if self.print_new_data is True:
            print(
                f"Data record received in async function and added to the database: {data}"
            )
        await asyncio.sleep(1)

    def processing_of_stream_signals(
        self, signal_type=False, stream_id=False, data_record=False
    ):
        # More info about `stream_signals`:
        # https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signal_buffer%60
        print(
            f"Received STREAM SIGNAL for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record}"
        )

    async def start(self):
        self.ubwa.create_stream(
            channels=["trade", "kline_1m", "depth5"],
            markets=["btcusdt", "ethusdt", "bnbusdt"],
            process_stream_data_async=self.processing_of_new_data_async,
            stream_label="multiplex",
        )

        while self.ubwa.is_manager_stopping() is False:
            await asyncio.sleep(10)
            stream_info = {
                "multiplex": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("multiplex")
                ),
                "userData_A": self.ubwa.get_stream_info(
                    stream_id=self.ubwa.get_stream_id_by_label("userData_A")
                ),
            }

            status_text = ""
            status_text += (
                f"\tStream is {stream_info['multiplex']['status']} "
                f"(last_stream_signal={stream_info['multiplex']['last_stream_signal']})\r\n"
            )

            print(
                f"Status:\r\n\tStored {len(self.example_database)} data records in `self.example_database`\r\n"
                f"{status_text}"
            )


if __name__ == "__main__":
    bdp = BinanceDataProcessor(print_new_data=True)
    try:
        asyncio.run(bdp.start())
    except KeyboardInterrupt:
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
    except Exception as error_msg:
        print(f"\r\nERROR: {error_msg}")
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
