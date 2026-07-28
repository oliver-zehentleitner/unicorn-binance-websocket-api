#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from kafka.errors import KafkaConnectionError
from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import asyncio
import json
import logging
import os


class BinanceToKafkaProcessor:
    def __init__(self):
        self.kafka_producer = None

    async def main(self):
        self.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=kafka["server"],
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=kafka["user"],
            sasl_plain_password=kafka["pass"],
            ssl_context=create_ssl_context(),
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        await self.kafka_producer.start()
        ubwa.create_stream(
            channels="trade",
            markets=markets,
            process_asyncio_queue=self.import_last_price_to_kafka,
            stream_label="TRADE",
        )
        while ubwa.is_manager_stopping() is False:
            await asyncio.sleep(1)

    async def import_last_price_to_kafka(self, stream_id=None):
        print(
            f"Start processing data from webstream {ubwa.get_stream_label(stream_id=stream_id)} to Kafka ..."
        )
        while ubwa.is_stop_request(stream_id=stream_id) is False:
            data = await ubwa.get_stream_data_from_asyncio_queue(stream_id)
            if data.get("event_type") == "trade":
                topic = f"{kafka['user']}-{str(data['symbol']).lower()}_binance_spot_last_trade_price"
                message = {
                    "symbol": data["symbol"],
                    "time": data["trade_time"],
                    "price": data["price"],
                }
                print(f"Adding to Kafka: {topic}, {message}")
                await self.kafka_producer.send_and_wait(topic, message)
            ubwa.asyncio_queue_task_done(stream_id)

    def receive_stream_signal(
        self, signal_type=None, stream_id=None, data_record=None, error_msg=None
    ):
        print(
            f"Received stream_signal for stream '{ubwa.get_stream_label(stream_id=stream_id)}': "
            f"{signal_type} - {stream_id} - {data_record} - {error_msg}"
        )


if __name__ == "__main__":
    # Config
    exchange = "binance.com"
    markets = ["ethusdt", "btcusdt", "ltcusdt"]
    kafka = {
        "server": "rocket.srvs.cloudkafka.com:9094",
        "user": "YOUR_KAFKA_USER",
        "pass": "YOUR_KAFKA_PASSWORD",
    }

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )

    btkp = BinanceToKafkaProcessor()
    with BinanceWebSocketApiManager(
        exchange=exchange,
        enable_stream_signal_buffer=True,
        process_stream_signals=btkp.receive_stream_signal,
        output_default="UnicornFy",
    ) as ubwa:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(btkp.main())
        except KeyboardInterrupt:
            print(f"\r\n")
        except KafkaConnectionError as e:
            print(f"\r\nKafkaConnectionError: {e}")
        except Exception as e:
            print(f"\r\nError: {e}")
        print("Gracefully stopping ...")
        loop.create_task(btkp.kafka_producer.stop())
