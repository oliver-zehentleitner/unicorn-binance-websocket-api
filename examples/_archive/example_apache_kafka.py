#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: example_apache_kafka.py
#
# Part of ‘UNICORN Binance WebSocket API’
# Project website: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Github: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Documentation: https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api
# PyPI: https://pypi.org/project/unicorn-binance-websocket-api
#
# Author: Oliver Zehentleitner
#
# Copyright (c) 2019-2024, Oliver Zehentleitner (https://about.me/oliver-zehentleitner)
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

# How to:
# https://technopathy.club/passing-binance-market-data-to-apache-kafka-in-python-with-aiokafka-570541574655

# Info:
# depends on: https://pypi.org/project/kafka-python/
# must not be installed: https://pypi.org/project/kafka (https://github.com/dpkp/kafka-python/issues/1566)

from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from kafka.errors import KafkaConnectionError
from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import asyncio
import json
import logging
import os
import time


async def import_last_price_to_kafka(
    kafka_config: dict = None,
    ubwa_manager: BinanceWebSocketApiManager = False,
    markets: list = None,
):

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_config["server"],
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=kafka_config["user"],
        sasl_plain_password=kafka_config["pass"],
        ssl_context=create_ssl_context(),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    await kafka_producer.start()

    ubwa_manager.create_stream(channels="trade", markets=markets, output="UnicornFy")

    try:
        while True:
            data = ubwa_manager.pop_stream_data_from_stream_buffer()
            if data is not False:
                try:
                    event_type = data["event_type"]
                    symbol = data["symbol"]
                    trade_time = data["trade_time"]
                    price = data["price"]
                except KeyError:
                    continue
                if event_type == "trade":
                    topic = f"{kafka_config['user']}-{str(symbol).lower()}_binance_spot_last_trade_price"
                    message = {"symbol": symbol, "time": trade_time, "price": price}
                    print(topic, message)
                    await kafka_producer.send_and_wait(topic, message)
            else:
                time.sleep(0.01)
    finally:
        await kafka_producer.stop()
        ubwa_manager.stop_manager_with_all_streams()


if __name__ == "__main__":
    # Logging
    logging.basicConfig(
        level=logging.DEBUG,
        filename=os.path.basename(__file__) + ".log",
        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
        style="{",
    )

    # Config
    exchange = "binance.com"
    symbols = ["ethusdt", "btcusdt", "ltcusdt"]
    kafka = {
        "server": "rocket.srvs.cloudkafka.com:9094",
        "user": "pfyrfgiv",
        "pass": "JUTrtwrJFYdMsYbEZGoL0Zt5m1WElPzS",
    }

    ubwa = BinanceWebSocketApiManager(exchange=exchange)

    try:
        asyncio.run(
            import_last_price_to_kafka(
                kafka_config=kafka, ubwa_manager=ubwa, markets=symbols
            )
        )
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
    except KafkaConnectionError as error_msg:
        print(f"{error_msg}")

    ubwa.stop_manager_with_all_streams()
