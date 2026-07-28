from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import logging
import os
import tracemalloc

tracemalloc.start(25)

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
            exchange="binance.com",
            process_asyncio_queue=self.process_asyncio_queue_global,
            enable_stream_signal_buffer=True,
            process_stream_signals=self.processing_of_stream_signals,
            output_default="dict",
            high_performance=False,
            auto_data_cleanup_stopped_streams=True,
        )
        self.stream_id1 = None
        self.stream_id2 = None

    async def process_asyncio_queue_global(self, stream_id=None):
        print(f"Start processing data of {stream_id} ...")
        last_update_id = {}
        current_update_id = {}
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            if data.get("data"):
                market = str(data.get("stream").split("@")[0]).lower()
                current_update_id[market] = data.get("data").get("lastUpdateId")
                if last_update_id.get(market) is None:
                    last_update_id[market] = 0
                print(
                    f"{market} - {last_update_id.get(market)} - {current_update_id.get(market)} - "
                    f"{('True' if current_update_id.get(market) > last_update_id.get(market) else 'False')}"
                )
                last_update_id[market] = current_update_id.get(market)
            self.ubwa.asyncio_queue_task_done(stream_id)

    async def process_asyncio_queue_specific(self, stream_id=None):
        print(f"Start processing data of {stream_id} ...")
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            # print(data)
            self.ubwa.asyncio_queue_task_done(stream_id)

    def processing_of_stream_signals(
        self, signal_type=None, stream_id=None, data_record=None, error_msg=None
    ):
        print(
            f"Received STREAM SIGNAL for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record} - {error_msg}"
        )

    async def start(self):
        self.stream_id1 = self.ubwa.create_stream(
            stream_label="stream_1",
            channels=["depth5@100ms"],
            markets=["ethbtc", "btcusdt"],
        )
        self.stream_id2 = self.ubwa.create_stream(
            stream_label="stream_2",
            channels=["trade"],
            markets=["ethbtc", "btcusdt"],
            process_asyncio_queue=self.process_asyncio_queue_specific,
        )
        self.ubwa.create_stream(
            markets="arr",
            channels="!userData",
            api_key="api_key",
            api_secret="api_secret",
        )
        while self.ubwa.is_manager_stopping() is False:
            # self.ubwa.print_summary()
            await asyncio.sleep(5)


if __name__ == "__main__":
    bdp = BinanceDataProcessor()
    try:
        asyncio.run(bdp.start())
    except KeyboardInterrupt:
        print("Gracefully stopping ...")
        bdp.shutdown = True
        bdp.ubwa.stop_manager()
    except Exception as error_msg:
        print(f"\r\nERROR: {error_msg}")
        print("Gracefully stopping ...")
        bdp.shutdown = True
        bdp.ubwa.stop_manager()
