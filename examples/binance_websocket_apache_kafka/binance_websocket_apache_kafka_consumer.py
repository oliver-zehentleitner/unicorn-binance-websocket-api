#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
from kafka.errors import KafkaConnectionError
import asyncio
import logging
import json
import os


async def consume_from_kafka(kafka_config: dict = None):
    kafka_consumer = AIOKafkaConsumer(
        kafka_config["topic"],
        bootstrap_servers=kafka_config["server"],
        group_id=f"{kafka_config['user']}-{kafka_config['topic']}",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        auto_offset_reset="earliest",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=kafka_config["user"],
        sasl_plain_password=kafka_config["pass"],
        ssl_context=create_ssl_context(),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    await kafka_consumer.start()
    try:
        async for msg in kafka_consumer:
            print(f"{msg.topic}:{msg.partition}:{msg.offset}: value={msg.value}")
    finally:
        await kafka_consumer.stop()


if __name__ == "__main__":
    # Logging
    logging.basicConfig(
        level=logging.INFO,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )

    # Config
    kafka = {
        "server": "rocket.srvs.cloudkafka.com:9094",
        "user": "YOUR_KAFKA_USER",
        "pass": "YOUR_KAFKA_PASSWORD",
        "topic": "YOUR_PREFIX-btcusdt_binance_spot_last_trade_price",
    }

    try:
        asyncio.run(consume_from_kafka(kafka_config=kafka))
    except KeyboardInterrupt:
        pass
    except KafkaConnectionError as e:
        print(f"KafkaConnectionError: {e}")
    except Exception as e:
        print(f"\r\nError: {e}")
    print("Gracefully stopping ...")
