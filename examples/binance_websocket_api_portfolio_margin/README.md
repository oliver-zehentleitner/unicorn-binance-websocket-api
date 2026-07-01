# Binance Portfolio Margin User Data Stream
## Overview
This example opens a Portfolio Margin user data stream against `binance.com-portfolio_margin`.

Portfolio Margin user data streams live at `wss://fstream.binance.com/pm/ws/<listenKey>`. The `listenKey` is
acquired, kept alive and closed via the Portfolio Margin REST API at `https://papi.binance.com/papi/v1/listenKey`
(`POST`/`PUT`/`DELETE`) — UBWA/UBRA handle that lifecycle automatically, just like for the other exchanges.

Support for `binance.com-portfolio_margin` is currently scoped to this listenKey/user-data-stream lifecycle. The
full Portfolio Margin REST surface (account, positions, orders, etc.) is not implemented in
[UBRA](https://github.com/oliver-zehentleitner/unicorn-binance-rest-api) yet — see
[issue #452](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/452).

Reference: [Binance Portfolio Margin user data streams docs](https://developers.binance.com/docs/derivatives/portfolio-margin/user-data-streams).

## Prerequisites
Ensure you have Python 3.9+ installed on your system.

Before running the provided script, install the required Python packages:
```bash
pip install -r requirements.txt
```

Edit the script and set `api_key` / `api_secret` to a Binance API key with Portfolio Margin permissions.

## Usage
### Running the Script:
```bash
python binance_websocket_api_portfolio_margin.py
```

Trigger an order or a balance change on the Portfolio Margin account (e.g. via the REST API or the Binance UI) and
you should see the corresponding user data events arrive on the stream.

### Graceful Shutdown:
The script is designed to handle a graceful shutdown upon receiving a KeyboardInterrupt (e.g., Ctrl+C) or
encountering an unexpected exception.

## Logging
The script employs logging to provide insights into its operation and to assist in troubleshooting. Logs are saved
to a file named after the script with a .log extension.

For further assistance or to report issues, please
[visit the GitHub repository](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api).
