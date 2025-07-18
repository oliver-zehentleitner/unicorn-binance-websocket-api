# Binance Chain WebSocket Best Practice
## Overview
A best practice example for websockets to Binance Chain (binance.org) in Python.

## Prerequisites
Ensure you have Python 3.7+ installed on your system. 

Before running the provided script, install the required Python packages:
```bash
pip install -r requirements.txt
```
## Get a UNICORN Binance Suite License
To run modules of the *UNICORN Binance Suite* you need a [valid license](https://shop.lucit.services)!

## Usage
### Running the Script:
```bash
python binance_chain_websocket_best_practice.py
```

### Graceful Shutdown:
The script is designed to handle a graceful shutdown upon receiving a KeyboardInterrupt (e.g., Ctrl+C) or encountering 
an unexpected exception.

## Logging
The script employs logging to provide insights into its operation and to assist in troubleshooting. Logs are saved to a 
file named after the script with a .log extension.

For further assistance or to report issues, please [contact our support team](https://www.lucit.tech/get-support.html) 
or [visit our GitHub repository](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api).