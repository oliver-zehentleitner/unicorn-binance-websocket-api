#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_binance_websocket_api import BinanceWebSocketApiManager
from unicorn_binance_rest_api import BinanceRestApiManager
import aiosqlite
import asyncio
import logging
import os

exchange = "binance.com"
sqlite_db_file = "ohlcv.db"

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.basename(__file__) + ".log",
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
    style="{",
)


class BinanceDataProcessor:
    def __init__(self):
        self.db = None
        self.ubwa = BinanceWebSocketApiManager(
            exchange=exchange,
            enable_stream_signal_buffer=True,
            process_stream_signals=self.receive_stream_signal,
            output_default="UnicornFy",
        )

    async def main(self):
        self.db = AsyncDatabase(sqlite_db_file)
        await self.db.create_connection()
        await self.db.create_table()
        with BinanceRestApiManager(exchange=exchange) as ubra:
            markets = [
                item["symbol"]
                for item in ubra.get_all_tickers()
                if item["symbol"].endswith("USDT")
            ]
        self.ubwa.create_stream(
            channels="kline_1m",
            markets=markets[: self.ubwa.get_limit_of_subscriptions_per_stream()],
            process_asyncio_queue=self.process_ohlcv_datasets,
            stream_label="OHLCV",
        )
        while self.ubwa.is_manager_stopping() is False:
            await asyncio.sleep(1)
            saved_datasets = await self.db.count_ohlcv_records()
            self.ubwa.print_summary(add_string=f"saved datasets: {saved_datasets}")

    async def process_ohlcv_datasets(self, stream_id=None):
        print(
            f"Saving the data from webstream {self.ubwa.get_stream_label(stream_id=stream_id)} to the database ..."
        )
        while self.ubwa.is_stop_request(stream_id=stream_id) is False:
            kline = await self.ubwa.get_stream_data_from_asyncio_queue(stream_id)
            if kline.get("event_type") == "kline":
                if (
                    kline["kline"]["is_closed"] is True
                    or kline["event_time"] >= kline["kline"]["kline_close_time"]
                ):
                    await self.db.insert_ohlcv_data(kline)
            self.ubwa.asyncio_queue_task_done(stream_id)

    def receive_stream_signal(
        self, signal_type=None, stream_id=None, data_record=None, error_msg=None
    ):
        print(
            f"Received stream_signal for stream '{self.ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record} - {error_msg}"
        )


class AsyncDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None

    async def close(self):
        await self.conn.close()

    async def count_ohlcv_records(self):
        async with self.conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM ohlcv")
            result = await cur.fetchone()
            return result[0] if result else 0

    async def create_connection(self):
        self.conn = await aiosqlite.connect(self.db_file)

    async def create_table(self):
        try:
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv (
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL
                )
            """)
            await self.conn.commit()
        except Exception as error_msg:
            print(f"Not able to create SQLite table: {error_msg}")

    async def insert_ohlcv_data(self, data):
        sql = """
        INSERT INTO ohlcv(date, symbol, open, high, low, close, volume)
        VALUES(?,?,?,?,?,?,?)
        """
        ohlcv_data = (
            data["kline"]["kline_start_time"],
            data["kline"]["symbol"],
            float(data["kline"]["open_price"]),
            float(data["kline"]["high_price"]),
            float(data["kline"]["low_price"]),
            float(data["kline"]["close_price"]),
            float(data["kline"]["base_volume"]),
        )
        async with self.conn.cursor() as cur:
            await cur.execute(sql, ohlcv_data)
        await self.conn.commit()
        return cur.lastrowid


if __name__ == "__main__":
    bdp = BinanceDataProcessor()
    try:
        asyncio.run(bdp.main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
        bdp.ubwa.stop_manager()
        asyncio.run(bdp.db.close())
    except Exception as e:
        print(f"\r\nError: {e}")
        print("Gracefully stopping ...")
        bdp.ubwa.stop_manager()
        asyncio.run(bdp.db.close())
