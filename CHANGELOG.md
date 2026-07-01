# unicorn-binance-websocket-api Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to 
[Semantic Versioning](http://semver.org/).

[Discussions about unicorn-binance-websocket-api releases!](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/discussions/categories/releases)

  [How to upgrade to the latest version!](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/readme.html#installation-and-upgrade)

## 2.14.0.dev (development stage/unreleased/unstable)
### Added
- Portfolio Margin support (`binance.com-portfolio_margin` exchange), scoped
  to the `!userData` stream for now:
  - `connection_settings.py`: new `Exchanges.BINANCE_PORTFOLIO_MARGIN` entry
    in `CONNECTION_SETTINGS` (`wss://fstream.binance.com/pm/`, no WS API base
    URI yet). Deliberately excluded from `BINANCE_FUTURES_EXCHANGES` — it
    keeps the legacy `/ws/<listenKey>` path form, not the `/private/ws?
    listenKey=...&events=...` form used by USDⓈ-M Futures.
  - `restclient.py`: listenKey get/keepalive/close routing to the new UBRA
    `portfolio_margin_stream_*()` methods.
  - New example:
    [`binance_websocket_api_portfolio_margin`](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/tree/master/examples/binance_websocket_api_portfolio_margin).
  - Requires an unreleased UBRA version — see
    [issue #452](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/452).
    The full Portfolio Margin REST surface (account, positions, orders,
    etc.) is out of scope for now; deferred to the planned UBRA rewrite.
  - `restclient.py`: `_init_ubra()` now catches UBRA's `UnknownExchange` and
    returns `False` instead of letting it propagate — before this, using an
    exchange the installed UBRA doesn't recognize yet (as is currently the
    case for `binance.com-portfolio_margin` until UBRA ships it) crashed the
    calling stream thread with an uncaught exception instead of the intended
    graceful `(None, None)` degradation. `get_listen_key()`,
    `keepalive_listen_key()` and `delete_listen_key()` now check this return
    value and bail out cleanly.

## 2.14.0
### Added
- `create_stream()` and `replace_stream()`: new optional `events` /
  `new_events` parameter selecting which event types Binance pushes
  on a USDT-M Futures `!userData` stream
  (`ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`, `MARGIN_CALL`,
  `TRADE_LITE`, `ACCOUNT_CONFIG_UPDATE`, `STRATEGY_UPDATE`,
  `GRID_UPDATE`, `CONDITIONAL_ORDER_TRIGGER_REJECT`,
  `ALGO_ORDER_UPDATE`, `listenKeyExpired`). Accepts a `str`, an
  iterable of event names or `None`. `None` (the default) subscribes
  to the full set, preserving the behaviour of the pre-2026-04-23
  `/ws/<listenKey>` URL where every event arrived unfiltered. User-
  supplied event names are not validated against an allow-list so
  that Binance-side additions can be passed through immediately
  without waiting for a UBWA release. Ignored for non-Futures
  exchanges and non-userData streams.
- `connection_settings.BINANCE_FUTURES_USERDATA_EVENTS` tuple
  containing the default event subscription set, mirroring the
  Binance docs for the USDⓈ-M Futures user data stream.
### Changed
- Internal cleanup: moved the UBWA-internal Futures-category resolution
  helpers (`_classify_futures_token`, `_resolve_futures_category`) from
  module scope into `BinanceWebSocketApiManager` as private
  `@staticmethod` / `@classmethod`. The routing-token constant
  `FUTURES_SUFFIX_MARKERS` moves from `manager.py` into
  `connection_settings.py`, next to the other Futures routing
  constants. No behaviour change.
  ([PR #449](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/449))
### Fixed
- USDT-M Futures `!userData` streams (`BINANCE_FUTURES`,
  `BINANCE_FUTURES_TESTNET`): the `listenKey` is now passed as a
  query parameter (`/private/ws?listenKey=...`) instead of as a path
  segment (`/private/ws/<listenKey>`). The path form has been
  decommissioned by Binance on the mainnet `fstream` host; testnet
  was still accepting both forms which masked the issue during the
  2.13.0 verification round. Reported by
  [@andykarpov](https://github.com/andykarpov) on
  [issue #437](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/437#issuecomment-4492250065)
  and originally fixed in [PR #445](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/445).
- `replace_stream()`: forwarded its arguments to `create_stream()`
  positionally, but the 12th slot of `create_stream()` had become
  `listen_key` (introduced with the WS API rework), so
  `new_stream_buffer_maxlen` was silently bound to `listen_key`.
  All arguments are now passed by keyword, eliminating the silent
  parameter shift and future-proofing against signature additions.

## 2.13.0
### Added
- `connection_settings.BINANCE_FUTURES_EXCHANGES` frozenset
  containing the exchanges that route through the new per-category
  Futures base paths.
- `create_stream()` now raises `ValueError` on
  `BINANCE_FUTURES`/`BINANCE_FUTURES_TESTNET` when
  `channels`/`markets` resolve to more than one Futures category
  (`public`/`market`/`private`). A single WebSocket connection can
  no longer carry multiple categories; UBWA does not silently split
  a stream into several connections — open one `create_stream()`
  per category instead. Mirrors the existing `!userData`-in-
  combined-stream rejection.
### Changed
- USDT-M Futures (`BINANCE_FUTURES`, `BINANCE_FUTURES_TESTNET`):
  WebSocket streams are now routed via the per-category base paths
  `/public`, `/market` and `/private` introduced by Binance on
  2026-04-23. Channels are auto-classified at connection time and
  the matching path segment is prepended to `ws/` and
  `stream?streams=`. The legacy `/ws` and `/stream` paths have been
  decommissioned by Binance.
  See [issue #437](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/437).

## 2.12.2
### Changed
- Bumped minimum `unicorn-fy` dependency from `>=0.15.0` to `>=0.17.2`
  in `setup.py`, `requirements.txt`, `pyproject.toml`,
  `environment.yml` and `meta.yaml`. 0.17.2 is the first release of
  the suite-wide LUCIT-cleanup round.
- Bumped minimum `unicorn-binance-rest-api` dependency from `>=2.7.0`
  to `>=2.11.0` in `setup.py`, `requirements.txt`, `pyproject.toml`,
  `environment.yml` and `meta.yaml`. 2.11.0 is the cleanup-round
  release.
- `CONTRIBUTING.md` and `.github/pull_request_template.md`: removed the
  "submitted code becomes LUCIT IT-Management GmbH's property"
  clause and the LSOSL reference. Contributions are MIT-licensed now.
- `SECURITY.md`: replaced the lucit.tech contact form URL with the
  GitHub Security Advisories private-reporting URL.
- `dev/test_*.py`: updated the copyright header from
  "LUCIT Systems and Development" to "Oliver Zehentleitner" and
  dropped the `# https://shop.lucit.services` license-shop references.
- `unittest_binance_websocket_api.py`: changed the
  `print_summary_to_png()` footer from "By LUCIT" to "UBWA".
### Removed
- `dev/test_license.py`: tested the old LUCIT licensing manager which
  has been removed from the codebase; the test file is obsolete.
- README: switched all conda references from the legacy `lucit` channel
  to `conda-forge`. Added conda-forge version / downloads / feedstock
  build badges. Removed the "There is no conda support until migration"
  placeholders. Install section is now a single
  `conda install -c conda-forge unicorn-binance-websocket-api`.
- Aligned dependency lists across `requirements.txt`, `setup.py`,
  `pyproject.toml`, `environment.yml` and `meta.yaml`
  (`typing_extensions` was missing from `requirements.txt` and
  `pyproject.toml`; versions pinned consistently).
- `meta.yaml`: removed the leftover `channels:` and `dependencies:`
  blocks (they are `environment.yml` keys, not valid in `meta.yaml`).
  Dropped the `lucit::` channel prefixes from the suite deps.
  Refreshed `about.description` by re-embedding the current `README.md`.
- `environment.yml`: dropped the `lucit` channel and the `lucit::`
  prefixes on suite dependencies — conda-forge provides them now.
- `.github/workflows/build_conda.yml`: the conda-forge feedstock
  (`conda-forge/unicorn-binance-websocket-api-feedstock`) now builds
  and publishes the conda package; no in-repo build is needed anymore.

## 2.12.1
### Fixed
- `MANIFEST.in`: the previous pattern `include unicorn_binance_websocket_api/*.*`
  only matched top-level files and excluded the `api/` subpackage, so the
  source tarball on PyPI shipped `api/*.c` but no `api/*.py` or `api/*.pyi`.
  Building from the sdist (e.g. conda-forge) therefore failed with
  `ValueError: 'unicorn_binance_websocket_api/api/*.py' doesn't match any files`.
  Switched to `recursive-include` so all Python, stub, Cython and binary
  files in every subpackage are packaged.

## 2.12.0
### Added
- Support for Binance European Options (Vanilla Options) WebSocket streams via `exchange="binance.com-vanilla-options"`
- New exchange strings: `binance.com-vanilla-options`, `binance.com-vanilla-options-testnet`
- Connection settings: `wss://fstream.binance.com/public/` (200 max streams per connection)

## 2.11.0
### Added
- Python 3.14 support (GIL build)
- `stop_manager()` and `stop_manager_with_all_streams()` now accept a `delete_listen_key` parameter
### Changed
- build_wheels.yml: Upgraded `cibuildwheel` from `v3.0.0` to `v3.4.1`
- `!userData` streams on `binance.com`, `binance.com-testnet`, `binance.com-margin`,
  `binance.com-margin-testnet`, `binance.com-isolated_margin` and
  `binance.com-isolated_margin-testnet` now use the new WS API subscription flow
  (`userDataStream.subscribe.signature`) instead of the legacy REST listenKey approach.
  Binance removed the Spot/Margin listenKey REST endpoints (`POST /api/v3/userDataStream` etc.)
  in February 2026. The public interface is unchanged:
  `create_stream('arr', '!userData', api_key=..., api_secret=...)` continues to work as before.
  Futures exchanges are unaffected and keep their existing listenKey flow. (issue #399)
- `connection_settings.py`: Added `USERDATA_WS_API_EXCHANGES` constant; added WS API base URIs
  for `binance.com-margin`, `binance.com-margin-testnet`, `binance.com-isolated_margin` and
  `binance.com-isolated_margin-testnet`
- Minimum Python version raised from 3.8 to 3.9 (Python 3.8 reached EOL in October 2024)
- Upgraded `websockets` dependency from `==11.0.3` to `>=14.0`
- Updated websockets exception handling: `websockets.InvalidStatusCode` → `websockets.exceptions.InvalidStatus`
  with `.response.status_code` attribute access (websockets 14 API change)
- Simplified `connection_settings.py`: removed Python 3.8 version guard, now uses `typing.Type` unconditionally
### Fixed
- `get_latest_release_info()`: corrected GitHub API URL from the old `LUCIT-Systems-and-Development`
  organization to `oliver-zehentleitner`
- `_ping_listen_key()`: IP ban detection now reads the `Retry-After` response header first
  (provided by Binance on HTTP 418/429), with fallback to parsing `"banned until"` in the
  error message. The previous check for `"IP banned"` never matched Binance's actual error
  text and silently skipped the backoff entirely. (issue #398)
- `api/spot.py` and `api/futures.py`: float parameters (`price`, `quantity`, `stop_price`, etc.) were
  converted to string using `str()`, which produces scientific notation (e.g. `1.9e-07`) for very small
  values. Binance rejects these with error -1100. Now using `format(Decimal(repr(value)), 'f')` which
  always produces decimal notation. (issue #397)
  (default `True`). Set to `False` if multiple processes share the same `listen_key` and stopping
  one process should not invalidate it for the others. (issue #396)
- On reconnect, the payload queue is now cleared and the full current subscription state is
  re-queued as a fresh SUBSCRIBE message. Previously, stale queued payloads from before the
  disconnect could cause duplicate or inconsistent subscriptions on the new connection.
  UserData streams (listen-key URI) and WebSocket-API streams are unaffected. (issue #374 follow-up)
- `_frequent_checks()`: Fixed `KeyError: 'receives_statistic_last_second'` race condition. When a
  stream restarts, its `stream_list` entry can be briefly absent or incomplete while
  `_frequent_checks()` is still iterating the active stream list snapshot. The delete-cleanup
  blocks for `receives_statistic_last_second` and `transfer_rate_per_second` were not wrapped in
  `try/except KeyError`, unlike the surrounding stat-read blocks. Added the missing guard.
### Removed
- `simplejson` dependency — unused in UBWA code; `orjson` is the sole JSON library
- Python 3.8 support and CI job
- DEX support (`binance.org`, `binance.org-testnet`): Binance Chain has been discontinued. Removed
  `Exchanges.BINANCE_ORG` and `Exchanges.BINANCE_ORG_TESTNET` from `connection_settings.py`, all
  DEX-specific code paths in `manager.py` and `sockets.py`, the `set_private_dex_config()` method,
  the `dex_user_address` stream state field, and all related tests and examples.
- Icinga/Nagios monitoring REST server: removed `start_monitoring_api()`, `stop_monitoring_api()`,
  `get_monitoring_status_icinga()`, `_start_monitoring_api_thread()`, `get_latest_release_info_check_command()`,
  `get_latest_version_check_command()`, `is_update_available_check_command()`, and `restserver.py`.
  The `get_monitoring_status_plain()` method is retained for programmatic status access.
  Dependencies `flask`, `flask_restful`, and `cheroot` have been removed.

## 2.10.2
### Fixed
- restclient.py - Removed lucit licensing

## 2.10.1
### Added
- build_wheels.yml:  
  CIBW_ARCHS_LINUX: "x86_64 aarch64"
  CIBW_ARCHS_MACOS: "x86_64 arm64 universal2"
  CIBW_ARCHS_WINDOWS: "AMD64"
  CIBW_MUSLLINUX_X86_64_IMAGE: "musllinux_1_1"

## 2.10.0
### Changed
- Moved from https://github.com/LUCIT-Systems-and-Development/ to https://github.com/oliver-zehentleitner
### Removed
- LUCIT Licensing Manager

## 2.9.0
### Added
- New `Websocket API Spot` functions:
  - `manager.api.spot.cancel_and_replace_order()`
  - `manager.api.spot.get_aggregate_trades()`
  - `manager.api.spot.get_current_average_price()`
  - `manager.api.spot.get_historical_trades()`
  - `manager.api.spot.get_klines()`
  - `manager.api.spot.get_ui_klines()`  
  - `manager.api.spot.get_recent_trades()`
  - `manager.api.spot.get_unfilled_order_count()`
- Full Support for `Websocket API Futures`:
  - `manager.api.futures.cancel_order()`
  - `manager.api.futures.create_order()`
  - `manager.api.futures.get_account_balance()` (v1+v2)
  - `manager.api.futures.get_account_position()` (v1+v2)
  - `manager.api.futures.get_account_status()` (v1+v2)
  - `manager.api.futures.get_listen_key()`
  - `manager.api.futures.get_order()` 
  - `manager.api.futures.get_order_book()`
  - `manager.api.futures.get_ticker_order_book()`
  - `manager.api.futures.get_ticker_price()`
  - `manager.api.futures.get_server_time()`
  - `manager.api.futures.modify_order()`
  - `manager.api.futures.ping()`
- Error handling if someone tries to use the WebSocket API Feature with an unsupported exchange.
### Changed
- ujson has been replaced by orjson (faster!)
- Websocket API functions are no longer available under `manager.api` but under `manager.api.spot`. In addition, there 
  is now also `manager.api.futures`.
- No more use of deepcopy in ws api (faster!)
- Enhanced logging in WS API

## 2.8.1
### Changed
- Log levels in `send_with_stream()` to be less verbose.
- Dropping support for Python 3.7
### Fixed
- `AttributeError: 'NoneType' object has no attribute 'get'` in `get_latest_version()`.

## 2.8.0
### Added
- `clear_asyncio_queue()` 
### Changed
- Made the logic of `is_exchange_type()` better readable.
- Type of parameter `stream_buffer_name` in all methods.
### Fixed
- `get_latest_version()` KeyError.
### Removed
- Obsolete import `from __future__ import print_function` in `sockets.py`.

## 2.7.2
### Fixed
- Stream Data is always returned in `raw_data` format. [issue#380](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/380)

## 2.7.1
### Added
- Parameter `footer` to `print_summary()`, `print_summary_to_png()` and `print_stream_info()`
### Changed
- Improved text of `MaximumSubscriptionsExceeded` exception.
- Updated description text in all files.
### Fixed
- Import in `licensing_manager.py`.
- Type of global `logger` and `connect` variable.
### Security
- Set higher minimum version `2.31.0` for `requests`, as vulnerabilities were found in earlier versions:
  - CVE-2023-32681, Score: 6.1 (Medium)
    - Requests is a HTTP library. Requests has been leaking Proxy-Authorization headers to destination servers when 
      redirected to an HTTPS endpoint. This is a product of how we use `rebuild_proxies` to reattach the 
      `Proxy-Authorization` header to requests. For HTTP connections sent through the tunnel, the proxy will identify 
      the header in the request itself and remove it prior to forwarding to the destination server. However when sent 
      over HTTPS, the `Proxy-Authorization` header must be sent in the CONNECT request as the proxy has no visibility 
      into the tunneled request. This results in Requests forwarding proxy credentials to the destination server 
      unintentionally, allowing a malicious actor to potentially exfiltrate sensitive information. This issue affects 
      versions 2.3.0 through 2.30.0.
    - https://devhub.checkmarx.com/cve-details/CVE-2023-32681/
- Set higher minimum version `2.5.1` for `unicorn-binance-rest-api` are affected by vulnerabilities in used dependencies! 
  - Dependency `certifi`:
    - CVE-2023-37920, Score: 9.8 (High)
      - Certifi is a curated collection of Root Certificates for validating the trustworthiness of SSL certificates while 
        verifying the identity of TLS hosts. Certifi 1.0.1 through 2023.5.7 recognizes "e-Tugra" root certificates. 
        e-Tugra's root certificates were subject to an investigation prompted by reporting of security issues in their 
        systems. Certifi 2023.07.22 removes root certificates from "e-Tugra" from the root store.
      - https://devhub.checkmarx.com/cve-details/CVE-2023-37920/
  - Dependency `cryptography`:
    - CVE-2023-38325, Score: 7.5 (High)
      - The cryptography package versions prior to 41.0.2 for Python mishandles SSH certificates that have critical 
        options.
      - https://devhub.checkmarx.com/cve-details/CVE-2023-38325/
    - CVE-2023-49083, Score: 7.5 (High)
      - Cryptography is a package designed to expose cryptographic primitives and recipes to Python developers. Calling 
        `load_pem_pkcs7_certificates` or `load_der_pkcs7_certificates` could lead to a NULL-pointer dereference and 
        segfault. Exploitation of this vulnerability poses a serious risk of Denial of Service (DoS) for any application 
        attempting to deserialize a PKCS7 blob/certificate. The consequences extend to potential disruptions in system 
        availability and stability. This issue affects versions 3.1 through 41.0.5.
      - https://devhub.checkmarx.com/cve-details/CVE-2023-49083/
    - CVE-2023-50782, Score: 7.5 (High)
      - A flaw was found in the python cryptography package versions prior to 42.0.0. This issue may allow a remote 
        attacker to decrypt captured messages in TLS servers that use RSA key exchanges, which may lead to exposure of 
        confidential or sensitive data. This issue is an incomplete fix of CVE-2020-25659.
      - https://devhub.checkmarx.com/cve-details/CVE-2023-50782/
    - CVE-2024-26130, Score: 7.5 (High)
      - cryptography is a package designed to expose cryptographic primitives and recipes to Python developers. Starting 
        in version 38.0.0 and prior to version 42.0.4, if `pkcs12.serialize_key_and_certificates` is called with both a 
        certificate whose public key did not match the provided private key and an `encryption_algorithm` with `hmac_hash` 
        set (via `PrivateFormat.PKCS12.encryption_builder().hmac_hash(...)`, then a NULL pointer dereference would occur, 
        crashing the Python process. This has been resolved in version 42.0.4, the first version in which a `ValueError` 
        is properly raised.
      - https://devhub.checkmarx.com/cve-details/CVE-2024-26130/
  - Dependency  `requests`:
    - CVE-2023-32681, Score: 6.1 (Medium)
      - Requests is a HTTP library. Requests has been leaking Proxy-Authorization headers to destination servers when 
        redirected to an HTTPS endpoint. This is a product of how we use `rebuild_proxies` to reattach the 
        `Proxy-Authorization` header to requests. For HTTP connections sent through the tunnel, the proxy will identify 
        the header in the request itself and remove it prior to forwarding to the destination server. However when sent 
        over HTTPS, the `Proxy-Authorization` header must be sent in the CONNECT request as the proxy has no visibility 
        into the tunneled request. This results in Requests forwarding proxy credentials to the destination server 
        unintentionally, allowing a malicious actor to potentially exfiltrate sensitive information. This issue affects 
        versions 2.3.0 through 2.30.0.
      - https://devhub.checkmarx.com/cve-details/CVE-2023-32681/

## 2.7.0
### Added
- Exception `MaximumSubscriptionsExceeded` thrown by `subscribe_to_stream()`.
- `print_summary()` and `print_stream_info()` now display `binance_api_status_code` in color! Green at 200 otherwise red.
### Changed
- `subscribe_to_stream()` now throws the exception `MaximumSubscriptionsExceeded` instand of returning `False` if  
  the number of allowed subscriptions per stream is exceeded.
### Fixed
- Typing of `create_stream()` parameters.
- Type of parameter `stream_label` in `get_stream_id_by_label()`.
- Type of first return variable in `restclient.get_listen_key()`.
- Typo in `is_update_availabe_unicorn_fy()` to `is_update_available_unicorn_fy()`.
- Typo in `is_update_availabe_check_command()` to `is_update_available_check_command()`.
- A couple of small text typos.

## 2.6.0
### Added
- Better Logging to investigate [issue#374](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/374)
- `send_with_stream()` - Send a payload with a specific stream.
- Since UBWA is delivered as a compiled C extension, IDEs such as Pycharm and Visual Code cannot use information about 
  available methods, parameters and their types for autocomplete and other intellisense functions. As a solution, from 
  now on stub files (PYI) will be created in the build process and attached to the packages. The IDEs can automatically 
  obtain the required information from these.
### Changed
- Replaced all calls of `add_payload_to_stream()` in `manager.py`, `api.py` with `send_with_stream()`.
- Calling `set_socket_is_not_ready()` in `sockets.__aexit__()`.
- Consistent use of `stream_list_lock` and replacement of `stream_threading_lock` by `stream_list_lock` in `manager.py`.

## 2.5.0
Functionally, nothing changes with this update. However, there are now sensible error messages if errors occur in the 
code of the `process_asyncio_queue`-coroutine. 

### Added
- Solved `Tracebacks of process_asyncio_queue-coroutines` [issue#375](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/375)

## 2.4.0
Optimization of the performance and revision of the life support of the ListenKey.

### Changed
- The management methods are no longer started individually in threads but together in an event loop in 'run()'. By 
  switching from threads to AsyncIO tasks, some `time.sleep()` could be replaced by `asyncio.sleep()`
### Fixed
- In `_run_sockets()` `time.sleep()` was mistakenly used instead of `asyncio.sleep()`.
- Keeping the ListenKey alive has been revised and now runs as an asyncIO task in the event loop of the stream. In the 
  event of an exception due to an IP ban, the time window until the ban is lifted is extracted from the Binance response 
  and now waits until then to continue. [issue#330](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/330)
- Added exception handling for `TypeError` to `subscribe_to_stream()` and `unsubscribe_from_stream()`.

## 2.3.0
Redesign and rewrite of `connection.py`, `sockets.py` as well as the loop and exception handling in `manager.py` and 
support for `await get_stream_data_from_asyncio_queue(stream_id)`. 

Runs perfectly on Python 3.7 to 3.12 on Windows, Linux and Mac!

### Added
- `ubwa.api.get_listen_key()`
- Unit tests for python 3.7 to 3.12, extension of the tests.
- Support and use of an `asyncio.queue()`. The parameter "process_asyncio_queue" for `BinanceWebSocketApiManager()` and 
  `create_stream()` can be passed an async function, which is automatically added to the event loop of the websocket as 
  an async task. Received websocket data can be conveniently awaited with 
  `await get_stream_data_from_asyncio_queue(stream_id)` as an AsyncIO alternative to 
  `pop_stream_data_from_stream_buffer()`. This is probably the smartest way to process data from the websocket and 
  should be preferred to the stream_buffer and the callback function.
- `is_socket_ready()`
### Changed
- Updated from websockets 10.4 to 11.0.3.
- `pop_stream_data_from_stream_buffer()` returns `None` instead of `False`
- The parameter `throw_exception_if_unrepairable` was removed by the UBWA Manager and replaced by the stream_signal 
  `STREAM_UNREPAIRABLE`.  
  Info: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/wiki/%60stream_signals%60
- Many `False` values have been changed to `None` values in accordance with Python conventions.
- `shutdown_asyncgens()` to `_shutdown_asyncgens()`
- `run_socket()` to `_run_socket()`
- `_auto_data_cleanup_stopped_streams()` now performs a check every 60 seconds and deletes the data from streams that 
  have been stopped for more than 900 seconds.
- `datetime.utcfromtimestamp(stream_info['start_time']).strftime('%Y-%m-%d, %H:%M:%S UTC'))` is obsolete and has been 
  replaced by `datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d, %H:%M:%S UTC')`.
- Renamed `stop_stream_as_crash()` with `_crash_stream()`
- Renamed `is_stop_as_crash_request()` with `is_crash_request()`
- Renamed `stream_is_crashing()` with `_stream_is_crashing()`
- Renamed `stream_is_stopping()` with `_stream_is_stopping()`
### Fixed
- In Websocket API wrong method names were used in logging.
- Logging info in `connection.py` revised.
- `ubwa.api.get_open_orders()` can now be used without the `symbol` parameter to query all open orders.
- Completely revised the error handling.
### Removed
- Parameter `throw_exception_if_unrepairable` of `BinanceWebSocketApiManager()`.
- Exception `StreamRecoveryError`.
- `set_restart_request()`
- `_restart_stream()`
- `_keepalive_streams()`
- `kill_stream()`

## 2.2.0
This update is primarily aimed at stabilization. The loop management has been improved and runs absolutely fine in 
tests!

`unicorn-binance-websocket-api` can now also be installed on all architectures on which there are no precompiled 
packages from LUCIT. PIP now automatically recognises whether there is a suitable precompiled package and if not, 
the source is automatically compiled on the target system during the installation process with Cython. Even if you 
don't have to do anything special, please note that this process takes some time!

### Added
- Parameter `process_stream_data_async` in `manager.py` to `BinanceWebSocketApiManager()` as global setting parameter
  and to `create_stream()` as stream specific parameter. This means it is possible to provide a global and a stream 
  specific asyncio function as "process_stream_data" function.
- Support for `process_stream_data_async` and `specific_process_stream_data_async[stream_id]` to `socket.py`. 
- Support for `with-context` to `BinanceWebSocketApiSocket()`.
- `run_socket()` to `manager.py`.
- `shutdown_asyncgens()` to `manager.py`.
- `timeout` parameter (float) to `delete_stream_from_stream_list()`, `wait_till_stream_has_stopped()` and 
  `wait_till_stream_has_started()` with default value 10.0 seconds.
- Automatic data cleanup of stopped web streams. [issue#307](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/307)
  The UBWA Manager can be informed via the parameter `auto_data_cleanup_stopped_streams=True` that all remaining data 
  within the UBWA of a stopped stream should be automatically and completely deleted (duration 60 sec). 
  Default value is `False`. Also added the function `remove_all_data_of_stream_id()` and 
  `_auto_data_cleanup_stopped_streams()` which are doing the cleaning up job.
### Changed
- Parameter `process_stream_data` in `manager.py` from `False` to `None`.
- Parameter `process_stream_signals` in `manager.py` from `False` to `None`.
- Usage of `BinanceWebSocketApiSocket()` in `manager.py` by using the new `with-context` and `ubwa.run_socket()` to 
  close every websocket connection when leaving.
- Now using `wait_till_stream_has_stopped()` in `delete_stream_from_stream_list()`. This avoids many errors ;)
- Rewrite of the `try-except` and the `finally` code part for the loop handling in `_create_stream_thread()`. 
### Fixed
- Missing `await` of `websocket.close()` in `socket.py`.
- Event loops were not closed in rare situations. Adjustments to `_create_stream_thread()`, `create_stream()` 
  and `_restart_stream()`
- In `connection.py` `sys.exit()` statements were used within an asyncio loop, this prevented the correct closing of 
  the asyncio loops in rare cases and was replaced by `return False`.
### Removed
- `import sys` in `sockets.py`

## 2.1.4
### Added
- Websocket API: `create_order()` and `create_test_order()` - Support of parameter `quoteOrderQty`, Behavior: If 
  activated, it replaces the `quantity` parameter.
### Fixed
- Websocket API: `create_order()` and `create_test_order()` -  [issue#353](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/353) 
  and [issue#352](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/352)
- Websocket API: `create_order()` and `create_test_order()` -  `trailingDelta` is only used if `stopPrice` was not 
  specified.
- Binance futures testnet userData stream in `restclient.py` - [issue#347](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/347)

## 2.1.3
### Fixed
- Troubleshooting restart problems
- `manager.delete_listen_key_by_stream_id()` used `False` instead of `None`

## 2.1.2
### Fixed
- `RuntimeError: dictionary changed size during iteration` in `manager.stop_manager()`.
- Stopping `manager._restart_stream()` if manager is stopping.

## 2.1.1
### Changed
- Rewrite of `restclient.py` and implementing usage of `**params` with `UBRA`.
- `False` and `None` was used incorrectly in some cases, this has been unified.
### Fixed
- `manager.wait_till_stream_has_stopped()`

## 2.1.0
### Added
- Handling of not retrieved error messages in AsyncIO loops
- Debug mode for AsyncIO tasks
- `manager.stop_manager()` alias for `manager.stop_manager_with_all_streams()` 
- Support for `with`-context
### Fixed
- Exceptions in `socket.start_stocket() coroutine and replaced `sys.exit()` with `return False`
- AsyncIO Loop handling and closing
- Graceful shutdown in exception `UnknownExchange`
- `manager.wait_till_stream_has_started()`

## 2.0.0
### Added
- Support for Python 3.11 and 3.12
- Integration of the `lucit-licensing-python` library for verifying the UNICORN Binance Suite license. A license can be 
  purchased in the LUCIT Online Shop: https://shop.lucit.services/software/unicorn-binance-suite
- License change from MIT to LSOSL - LUCIT Synergetic Open Source License:
  https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/LICENSE
- Conversion to a C++ compiled Cython package with precompiled as well as PyPy and source code wheels.
- Setup of a "Trusted Publisher" deployment chain. The source code is transparently packaged into wheels directly from
  the GitHub repository by a GitHub action for all possible platforms and published directly as a new release on GitHub
  and PyPi. A second process from Conda-Forge then uploads it to Anaconda. Thus, the entire deployment process is
  transparent and the user can be sure that the compilation of a version fully corresponds to the source code.
### Changed
- Added `@staticmethod` to many static mehtods.
### Fixed
- Typos and formatting
- Shadow of a Python built-in in `manager.get_human_bytesize()`: `bytes` to `amount_bytes`
- `manager.get_new_uuid_id()` returned not str()


## 1.46.2
### Fixed
- Issue in `api.get_exchange_info()`: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/346


## 1.46.1
### Fixed
- Issue with separation of spot and margin accounts: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/331

## 1.46.0
### Added
- `get_the_one_active_websocket_api()` to manger.py
- Logging to `get_stream_id_by_label()`
- `process_response` parameter to `ubwa.api` methods to provide specific callback functions for specific 
  responses
- `return_response` parameter to `ubwa.api` methods to let the used method wait till the requested data is received via
  websocket, and then it returns it.
### Changed
- Use UnicornFy only for non api requests in sockets.py - if `output="UnicornFy"` is used for api then its just 
  converted to a python dict.
- `ubwa.api` methods do not need a `stream_id`/`stream_label` if there is only one valid websocket api. (Self-discovery 
  of uniquely identifiable websocket api streams if no `stream_id` or `stream_label` was specified for identification.)
- Renamed `ubwa.api.test_create_order()` to `ubwa.api.create_test_order()`
- Entire WS API implementation reworked
### Fixed
- Support for `new_client_order_id` in `create_test_order()`
- Get listenKey from Binance API for futures and coin futures userData stream
### Removed
- jex.com support

## 1.45.2
### Fixed 
- Python 3.7+ Support
### Renamed
- ws_api.py to api.py

## 1.45.1
### Changed
- Revised ws_api.py .... Websocket API is still BETA and not fully ready!! Please share your experience and ideas to 
  improve the implementation: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/319

## 1.45.0
### Added
- This is the first code part to support the new [Binance Websocket API](https://developers.binance.com/docs/binance-trading-api/websocket_api)
  [issue#319](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/319):
  - `WEBSOCKET_API_BASE_URI` to connection_settings.py and added URI for spot and testnet: `wss://ws-api.binance.com/ws-api/v3` and `wss://testnet.binance.vision/ws-api/v3`
  - Upgraded `print_stream_info()` to show if a userData stream is a Websocket API stream or not.
  - New methods to manager.py `get_timestamp()`, `generate_signature()`, `order_params()`, `add_payload_to_stream`
  - ws_api.py to provide Binance websocket api functions in `ubwa.api.method(stream_id=stream_id)`.
    - `ubwa.api.cancel_open_orders()`
    - `ubwa.api.cancel_order()`
    - `ubwa.api.create_order()`
    - `ubwa.api.get_account_status()`
    - `ubwa.api.get_exchange_info()`
    - `ubwa.api.get_open_orders()`
    - `ubwa.api.get_order()`
    - `ubwa.api.get_order_book()`
    - `ubwa.api.get_server_time()`
    - `ubwa.api.ping()`
### Changed
- `create_stream(channels=[], markets=[])` initiated as lists and are not mandatory anymore to enable the use of 
  parameter `api` to create a Websocket API stream. 

## 1.44.1
### Added
- Passing the variable `warn_on_update` to UBRA
### Fixing 
- Saving `binance_api_status` update in `delete_listen_key()` and `keepalive_listenkey()` did not work after the integration of UBRA

## 1.44.0
### Added 
- [`BinanceRestApiManager()`](https://unicorn-binance-rest-api.docs.lucit.tech/unicorn_binance_rest_api.html#unicorn_binance_rest_api.manager.BinanceRestApiManager) - 
  New parameter: `socks5_proxy_user` and `socks5_proxy_pass`
- Dependency `unicorn-binance-rest-api` to `setup.py`, `requirements.txt`, `environment.yml` and conda feedstock recipe
- `socks5_proxy_user` and `socks5_proxy_pass` to [`BinanceWebSocketApiManager`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager)
- Full SOCKS5 Proxy support to REST API in `get_listen_key()`, `keepalive_listen_key()` and `delete_listen_key()` 
- Show proxy info in `print_summary()` and `print_stream_info()`
### Changed
- `restclient.py` now relies on `unicorn-binance-rest-api>=1.8.0`. REST config removed in `connection_settings.py`
- Structure of `CONNECTION_SETTINGS`
### Fixed
- RuntimeError: dictionary changed size during iteration in manger.py line 788, https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/commit/f189b9a8420133ffe34e9c9948e461a06d92f0a2
- `keepalive_listen_key()` and `delete_listen_key()` for isolated margin
- Activated functionality of `restful_base_uri` in rest client
- SOCKS5 proxy support only worked with one stream. Now it works with multiple streams.
### Removed
- `restful_path_userdata` from [`BinanceWebSocketApiManager`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager)

## 1.43.3
### Fixed
- Restore backward compatibility Python >= 3.7 [issue#311](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/311)

## 1.43.2
### Changed
- Remove "Assignment Expressions in f-strings" to restore Python Support 3.7+.
### Fixed
- Added whitespace between left "=" and title in first line of `manager.print_summary()` output.

## 1.43.1
### Fixed
- Fix typo "bug" in dependencies of setup.py

## 1.43.0
### Added
- Official support for Python 3.11
- Socks5 proxy support for [websocket](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager) only: `socks5_proxy_server` and `socks5_proxy_ssl_verification`  and exception 
`Socks5ProxyConnectionError` - Currently, no REST support for `listenKey` (needed for userData streams), this will be added later after the 
next UBRA update!
- Logging to `manager.wait_till_stream_has_started()`
- Logging to `manager.wait_till_stream_has_stoped()`
- Added [PR#305](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/305) of 
[issue#304](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/304) thx to 
[@dima-dmytruk23](https://github.com/dima-dmytruk23):
New parameters to override connection settings to 
[`BinanceWebSocketApiManager`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager): `websocket_base_uri`, 
`max_subscriptions_per_stream`, `restful_base_uri`, `restful_path_userdata`, `exchange_type`
### Changed
- "binance.com-coin-futures" to choose coin futures is deprecated. Use "binance.com-coin_futures" instead!
### Fixed
- Better "stop" measuring in `manager.wait_till_stream_has_stoped()` [discussion#279](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/discussions/279#discussioncomment-2917403)


## 1.42.0
### Added
- Parameter `delete_listen_key` to `stop_stream()`, so it's possible to disable the deletion of the `listen_key`
### Changed 
- The new `listen_key` keep alive interval is 10 min, with 30 min we have expired listenKeys in some cases. [issue#275](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/275)
- `websockets` version 10.3 to 10.4
- Log level error to debug [issue#277](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/277)
### Fixed
- socket.py typo in `unicorn_fy.trbinance_com_websocket()`
- `manager.get_stream_buffer_byte_size()` avoid division by zero error
- "Stream data not udpated after got event "listenKeyExpired" [issue#275](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/275)

## 1.41.0
### Added
- `debug` (bool) parameter to `BinanceWebSocketApiManager()`
- `manager.get_debug_log()`
- `manager.set_socket_is_ready()`
- `manager.set_socket_is_not_ready()`
- `_handle_task_result()` - a callback for eventtasks to retrive exceptions from within the asyncio loop.
### Changed
- `get_new_stream_id()` to `get_new_uuid_id()`
- Improved exception handling and restarts. This should have also positive effects to exection handling within the callback functions.
### Fixing 
- AsyncIO implementation and a temporary workaround for stopping userData streams 
- TRBinance replaced old URL `stream.binance.cc` by the new URL `stream-cloud.trbinance.com` [issue#249](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/249)

## 1.40.7
Codebase equal to 1.40.5, testing azure pipe
### Changed
- Exclude`tools` folder from package

## 1.40.6
Codebase equal to 1.40.5, just preparing conda-forge packaging

## 1.40.5
### Fixed
- Restart of error `sent 1011 (unexpected error) keepalive ping timeout` in sockets.py

## 1.40.4
### Changed
- Rebalanced default ping interval and timeout as well as closing timeout
### Removed
- Passing of `stream_buffer_name=stream_buffer_name` to `specific_process_stream_data()` 

## 1.40.3
### Changed
- Not catching general eception anymore - we must catch the specific exceptions: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/324f9fa191608946fb2973acc7de484e15f5030a/unicorn_binance_websocket_api/sockets.py#L220
### Fixed
- Catch KeyError before the general exception in sockets to give better feedback within the callback function.
- Error in coroutine loop handling

## 1.40.2
### Changed
- default ping interval and timeout
### Fixed
- `TypeError` in `print_summary()`
- typo in `print_summary()` `title` text
- Callback implementation of `create_stream()`

## 1.40.1
- test release

## 1.40.0
### Added
- `close_timeout_default`, `ping_interval_default`, `ping_timeout_default`
- `socket_is_ready` system to `_restart_stream()`
- `process_stream_data` parameter for callback function to `create_stream()`
- `high_performance` parameter to make `create_stream()` a non blocking
- `title` to `print_summary()` and `print_stream_info()`
### Fixed
- `BinanceWebSocketApiManager.stop_stream()` doesn't stop the stream immediately [issue#161](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/161)

## 1.39.0
### Changed
- Send the `DISCONNECT` stream_signal only on status change (one time!) and if previous status is not None
### Fixed
- KeyError in `get_listen_key_from_restclient()` if `stream_id` invalid
- Added `socket_is_ready` to `stream_is_crashing()`

## 1.38.1
### Fixed 
- Websocket fails to reconnect [issue#131](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/131)

## 1.38.0
### Added
- `is_stream_signal_buffer_enabled()`

## 1.37.2
### Fixed 
- `stream_is_crashing()` did not send stream_signal `DISCONNECT` - Without this fix no stream_signal was sent in some 
disconnect cases! 
### Changed
- Replaced the time.sleep() in create_stream with self.socket_is_ready system
 
## 1.37.1
### Fixed
- Catching `KeyError` in `manager.get_stream_buffer_length()` if buffer_name not exits

## 1.37.0
### Added
- `get_stream_buffer_length()` can now return the size of all stream_buffers and also of a specific stream_buffer.
### Changed
-  Bump websockets from 10.1 to 10.2 [PR#237](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/237) 

## 1.36.1
### Fixed
- UnboundLocalError: local variable 'error_msg' referenced before assignment in sockets.py line 209 reported in [issue#235](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/235)
- Avoid error 2 of this post: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/131#issuecomment-1042747365

## 1.36.0
### Changed
- removed "unicorn_binance_websocket_api_"-part of the module file names (more info: [Discussions](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/discussions/231))
- renamed logger name of all modules to "unicorn_binance_websocket_api", in the implementation of 
[PR#223](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/223) every module has had 
its own logger name.
 
## 1.35.0
### Added
- `manager.get_new_stream_id()` to avoid security alerts like "CodeQL py/clear-text-logging-sensitive-data"
### Changed
- `stream_id` is no longer an uuid, but a string from now on!
- Correctly scope loggers so that it plays nicely with others. [PR#223](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/223), [issue#164](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/164) 
- Removed pathlib from requirements [PR#224](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/224)
- Moved from https://github.com/oliver-zehentleitner to https://github.com/oliver-zehentleitner/
### Fixed
- asyncio.TimeoutError [issue#221](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/221) 
### Removed 
- Support for the new compression feature introduced in websockets10.0 because the new feature is removed in further 
websockets versions

## 1.34.2
### Changed
- "binance.com-coin-futures" to "binance.com-coin_futures"
### Fixed
- fix "Default argument value is mutable" error of `subscribe_from_stream()`
- No data received from binance.com-coin-futures with websockets==10.0 [issue#208](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/208) 

## 1.34.1
### Added
- Exception handling for `websockets.exceptions.NegotiationError` in connection class.
### Changed
- Logging of `restclient._request()`
### Fixed
- No data received from binance.com-futures with websockets==10.0 [issue#199](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/199) 

## 1.34.0
### Changed
- Bump websockets from 9.1 to 10.0 - Drop support for Python 3.6, added support for Python 3.10! 
  [PR#195](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/195)

## 1.33.1
### Added
- Logging websockets version on start up (logging level: INFO)
### Changed 
- `print_summary()` "most" to "peak".
### Fixed
- More accurate measurement of the received data quantity.

## 1.33.0
### Added
- `process_stream_signals` callback support [issue#160](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/160)
### Changed
- `self.stream_signal_buffer` is not a list anymore, it's changed to `collections.deque()`  

## 1.32.0
Now `stream_buffer` can be used as FIFO or LIFO stack, and it's possible to define a max length for it.
### Added
- `clear_stream_buffer()` to delete all items on the `stream_buffer` stack.
- `get_stream_buffer_maxlen()` to get the `maxlen` value of the stack.
- Support for `stream_buffer_maxlen` in methods of `BinanceWebSocketApiManager()` class `_create_stream_thread()`, `print_stream_info()`, `__init__()`, 
`_add_stream_to_stream_list()`, `_create_stream_thread()`, `create_stream()`, `replace_stream()`.
- Support for FIFO and LIFO in `pop_stream_data_from_stream_buffer(mode="FIFO")`
### Changed
- `get_used_weight()` replaces `get_binance_api_status()`
- `self.stream_buffer` and `self.stream_buffer[xxx]` are not lists anymore, their type has changed to `collections.deque()`  

## 1.31.0
### Added 
- Added partial support for wss://dstream.binance.com - perpetual coin futures! [PR#163](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/163) - Thanks to [M3tz3l](https://github.com/M3tz3l)
### Changed
- Bump websockets from 8.1 to 9.1 [PR#176](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/176)
### Fixed
- Exception Handling listen_key [issue#143](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/143)
- Unsubscribe_from_stream method doesn't handle uppercase symbols while subscribe_to_stream does [issue#167](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/167)
### Removed 
- binance.je support (Binance Jersey has ceased operations.)

## 1.30.0
### Added
- `get_event_loop_by_stream_id()`
- `disable_colorama` in `BinanceWebSocketApiManager()`: This is needed to make ubwa compatible with 
  [SwiftBar](https://github.com/swiftbar/SwiftBar)
### Changed
- The asyncio event loop is now saved to `self.stream_list[stream_id]['event_loop']`
- added `delete_listen_key_by_stream_id()` to `stop_stream()`
  [issue#161](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/161#issuecomment-814934231)

## 1.29.0
### Added
- `General Exception` handling in `start_socket()` [PR#142 thx @gronastech and @lordofserenity](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/142)
- Support for trbinance.com Websockets 
### Fixed
- `UnboundLocalError: local variable 'market' referenced before assignment` in `create_websocket_uri()` [PR#142 thx @gronastech and @lordofserenity](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/142)

## 1.28.0
### Changed
- the stream signal `DISCONNECT` includes `last_received_data_record` which returns now `None` if there is no record available
### Fixed
- Cannot use `in` with RuntimeError, must convert to string first. [PR#136 thx @Bosma](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/136)
### Removed
- Deprecated methods `set_private_api_config()` and `get_websocket_uri_length()`

## 1.27.0
### Added
- `timeout=10` to [`get_result_by_request_id()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_result_by_request_id): 
  Wait for `timeout` seconds to receive the requested result or return `False`
- logging the use of stream_buffer or process_stream_data and the used OS plattform
- individual `max_subscriptions_per_stream` for each endpoint
- `stream_signal_buffer` to receive signals if a stream gets connected or disconnected
- ['add_to_stream_signal_buffer()'](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=add_to_stream_signal_buffer#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.add_to_stream_signal_buffer)
- ['pop_stream_signal_from_stream_signal_buffer()'](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=pop_stream_signal#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.pop_stream_signal_from_stream_signal_buffer)
- Support for stream signals: `CONNECT`, `DISCONNECT`, `FIRST_RECEIVED_DATA` 
### Changed
- max subscriptions of futures endpoints to 200 [issue#127](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/127)
- max subscriptions of jex endpoint to 10
### Fixed
-  Added a gracefull shutdown if the Python interpreter dies [issue#131](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/131)
 
## 1.26.0
### Added
- parameter `ping_interval`, `ping_timeout`, `close_timeout` to [`manager.create_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=create_stream#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.create_stream)
  and [`replace_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=replace_stream#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.replace_stream)
- show `ping_interval`, `ping_timeout`, `close_timeout` in [`print_stream_info()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=print_stream_info#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.print_stream_info)
- `manager.set_heartbeat()` to `connection.send()`
- [`get_result_by_request_id()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_result_by_request_id)
### Changed
- log warning about high cpu usage is logged after 5 seconds if > 95% 

## 1.25.0
### Added
- [`get_user_agent()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.get_user_agent)
### Changed
- `get_stream_subscriptions()` returns now the used `request_id` instead of `True`

## 1.24.0
### Added
- [`output_default`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=output_default)
  to `BinanceWebSocketApiManager` 
### Removed
- unused import of `ujson` in connection class
- 4 parameters from `_create_stream_thread`

## 1.23.0
### Added
- timestamp to `receiving_speed_peak` in manager
- log warning if the cpu usage is > 95%
- logging.info if new `highest_receiving_speed` is reached
### Fixed
- `listen_key` was printed to logfiles
- `listen_key` cache time was not set in `get_listen_key()` so it pinged immediately after its creation again, which caused
a higher weight
- restart stream if "The future belongs to a different loop than the one specified as the loop argument" [issue#121](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/121)
### Changed
- renamed `_add_socket_to_socket_list` to `_add_stream_to_stream_list`
 
## 1.22.0
### Added
- `get_current_receiving_speed_global()`
- better logging in socket class
- `highest_receiving_speed` in `print_summary()`
### Changed
- renamed variable `ubwa` to `manager` in restclient class
- renamed variable `unicorn_binance_websocket_api_manager` to `manager` in socket class
- renamed variable `total_receiving_speed` to `average_receiving_speed`
- renamed variable `unicorn_binance_websocket_api_connection` to `connection`
- renamed variable `unicorn_binance_websocket_api_socket` to `socket`
- shorted user agent string for rest and websocket client
### Removed
- removed the sending of the payload in __aenter__ in connection class, from now on its only done in the socket class!
 
## 1.21.0
### Added 
- `is_update_availabe_unicorn_fy()` and `get_version_unicorn_fy()`
- `new_output` to [`replace_stream()`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html?highlight=replace_stream#unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager.BinanceWebSocketApiManager.replace_stream)
### Changed
- Rewrite of `BinanceWebSocketApiRestclient()`, its more or less stateless but compatible to the current system. Now 
we use one instance globally instead of creating a new one every time we need it. It will help to implement isolated
margin with more than one symbol. [issue#111](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/111)
- `time.sleep()` in `_frequent_checks()` from 0.1 to 0.3 seconds
### Fixed
- `RuntimeError` exception in `_create_stream_thread()` - no handling added, only logging and a "Todo"

## 1.20.0
### Added
- `dict` to [`create_stream(output='dict')`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream)
### Fixed
- StreamBuffer reset on restart [issue#119](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/119)
 
## 1.19.0
### Added
- [`UnicornFy`](https://github.com/oliver-zehentleitner/unicorn-fy) to 
[`create_stream(output='UnicornFy')`](https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api/unicorn_binance_websocket_api.html#unicorn_binance_websocket_api.manager.BinanceWebSocketApiManager.create_stream)
### Changed
- Links in docstrings

## 1.18.2
### Fixed
- added KeyError exception and `return False` to a few methods
- binance endpoints expects `symbol` not `symbols`
- RuntimeException in `close()`

## 1.18.1
### Fixed
- restclient: `symbol` to `symbols` 

## 1.18.0
### Added
- binance.com testnets (spot, margin, isolated_margin, future)
- `show_secrets_in_logs` parameter 
### Changed
- `symbol` to `symbols` (isolated_margin)
### Fixed
- update `binance_api_status`

## 1.17.4
### Added
- `replace_stream()`: new_stream_label=None, new_stream_buffer_name=False, new_symbol=False, new_api_key=False, 
new_api_secret=False
### Fixed 
- reconnect counter (bug since 1.17.0)

## 1.17.3
### Fixed
- [issue #109](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/109)
- [issue #110](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/110)

## 1.17.2
### Added 
- Handling of unknown error msg from Binance if uri = dict in connection class

## 1.17.1
### Fixed
- reference of api_key and secret in connection class

## 1.17.0
### Added
- Isolated margin endpoints [issue #109](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/109)
- Support for `@arr@@s1` [issue #101](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/101)
- Added `symbol` to `print_stream_info()`
- Added `api_key` and `api_secret` to `create_stream()` [issue #84](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/84) 

## 1.16.9
### Added
- Restart to ssl.SSLError exception in connection
### Removed
- error 2 code [PR #98](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/98) (Thanks Flowelcat)

## 1.16.7
### Added
- Restart again if OSError in `BinanceWebSocketApiConnection()`
### Changed
- Logging in `BinanceWebSocketApiConnection()`

## 1.16.6
### Changed
- Loglevels
### Fixed
- Fixed exception that thrown when api key is real but was deleted from binance. [PR #96](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/96) (Thanks Flowelcat)
- Package configuration is wrong. Currently one needs to have the bin-folder of the venv in the PATH. That is not feasible since you often have one venv per project. [PR #97](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/97) (Thanks uggel)

## 1.16.5
- REMOVED

## 1.16.4
### Changed
- Loglevels
### Fixed
- Fixed double slash bug when getting listen key for userDataStream. 
[PR #87](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/87) 
- Fixed RuntimeError in connection row 243 (added restart)

## 1.16.3
### Fixed
- restart if "with connection" in socket gets closed
- exception json.decoder.JSONDecodeError: respond = request_handler.json() 

## 1.16.2
### Fixed
- Exception AttributeError Info: module 'asyncio.base_futures' has no attribute 'InvalidStateError' [issue #72](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/83)

## 1.16.1
### Fixed
- exception in `print_suammary()`

## 1.16.0
### Added
- stream_buffer control: create_stream(channels, markets, stream_buffer_name=None): 
If `False` the data is going to get written to the default stream_buffer, set to `True` to read the data via 
`pop_stream_data_from_stream_buffer(stream_id)` or provide a string to create and use a shared stream_buffer 
and read it via `pop_stream_data_from_stream_buffer('string')`.
- `add_to_ringbuffer_error()`
- `add_to_ringbuffer_result()`
- `set_ringbuffer_error_max_size()`
- `set_ringbuffer_result_max_size()`
- `get_errors_from_endpoints()`
- `get_results_from_endpoints()`
- `get_ringbuffer_error_max_size()`
- `get_ringbuffer_result_max_size()`
### Changed
- renamed `restart_stream()` to `_restart_stream` and execute it only with a valid restart_request
### Fixed
- Ensure that during a restart, only the recent thread is able to send the payload for subscription

## 1.15.0
### Added 
- psutil (new requirement)
- exception handling of `websockets.exceptions.InvalidMessage` [issue #72](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/72)
- general exception handling
- show threads, memory and cpu usage in `print_summary()`
- `get_process_usage_memory()`
- `get_process_usage_cpu()`
- `get_process_usage_threads()`
### Fixed
- Close WS only if open in connection class row 190 [issue #72](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/72)
### Removed
- some code in connection row 206 which is not needed anymore and is causing a coroutine error
- `is_websocket_uri_length_valid()`

## 1.14.0
### Added
- new parameter `stream_label` in `manager.create_stream()`[issue #60](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/60)
- `manager.get_stream_label()` [issue #60](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/60)
- `manager.get_stream_id_by_label()` [issue #60](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/60)
- `manager.set_stream_label()` [issue #60](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/60)
- added `stream_label` to `manager.print_stream_info()` [issue #60](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/60)
- added `stream_label` to `manager.print_summary()` [issue #60](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/60)
- `manager.help()`
- `unicorn_binance_websocket_api_exceptions.py` with exception `StreamRecoveryError` and `UnknownExchange`
- `fill_up_space_right()`
- `self.restart_timeout`
### Changed
- raising `UnknownExchange` or `StreamRecoveryError` instead of `ValueError`
- `fill_up_space()` to `fill_up_space_left()`
### Fixed
- reset the payloads of a stream at a stream restart
- moved some code for a stream restart from `_keepalive_streams()` to `restart_stream()` which caused that the direct
call of `restart_stream()` worked only inside of `_keepalive_streams()`
- handling of `RuntimeWarning` in class connection at row 189
### Removed
- code to start new `_keepalive_streams()` and `_frequent_checks()` threads

## 1.13.0
### Added
- `disable_print` in `print_summary()` [pull #48](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/48)
- `print_summary_export_path` - if provided, the lib is going to export the output of `print_summary()` to a PNG image.
- `get_number_of_all_subscriptions()` and show all subscriptions number in `print_summary()`
### Fixed
- ping listen_key if "!userData" is in `channels`, not only in `markets`. 
- format of some logs
- stream buffer size [issue #51](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/51),
 [pull #51](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/52)

## 1.12.0
### RECOMMENDED UPDATE!
https://github.com/binance-exchange/binance-official-api-docs/blob/5fccfd572db2f530e25e302c02be5dec12759cf9/CHANGELOG.md#2020-04-23
### Added
- avoid sending more than 5 messages per stream per second [issue #45](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/45)
- stop streams and set status to "crashed" if they exceed the limit of 1024 subscriptions per stream [issue #45](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/45)
    - `is_stop_as_crash_request()`
    - `stop_stream_as_crash()`
- `get_limit_of_subscriptions_per_stream()`
- `get_number_of_free_subscription_slots()`
- `BinanceWebSocketApiManager(throw_exception_if_unrepairable=True)` - raise `StreamRecoveryError` if a stream is not repairable 
(invalid api-key format or exceeding the 1024 subscription limit)

### Changed
- loglevel `connection.send()` loglevels from error to critical.
- loglevel `manager.create_websocket_uri()` of known errors from error to critical.
### Fixed
- `OSError` exception for `self.monitoring_api_server.start()` if its already started
- `for keepalive_streams_id in self.keepalive_streams_list:` added threadding lock ([issue #47](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/47))

## 1.11.0
### Added
- binance jex
### Changed
- dependency websockets from 7.0 to 8.1 which needs python>=3.6.1 ([issue #11](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/11))
### Fixed
- expception handling of send() ([issue #43](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/43))
- thread lock for `frequent_checks_list` ([comment #590914274](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/11#issuecomment-590914274))
- `current_receiving_speed` did not reset to 0 if all streams are offline

## 1.10.6
### Added
- fill_up_space_centered()
- update check on manager start
### Changed
- print_stream_info() and print_summary(): unicorn-binance-websocket-api_<version>-python_<version> in top boarder row
- count subscriptions
### Fixed
- lower for cex and upper for dex with exceptions for arr, $all, ! and array channels

## 1.10.5
### Fixed
- `lower()` markets in `create_payload()` and exception for `!userData`
- get_active_stream_list() took len() of false item
- reconnect handling in send() ([issue #40](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/40))

## 1.10.4
### Changed
- making `self.stream_buffer` thread safe

## 1.10.3
### Changed
- removed simplejson exception in restclient
- set OSError from error to critical

## 1.10.2
### Fixed
- `['receives_statistic_last_second']` dict is changing size during iteration. ([issue #37](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/37))

## 1.10.1
### Changed
- Using ujson instead of stock json lib
- cleaning `create_payload()` for CEX 

## 1.10.0
### Important infos, [please read!](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/38)
### Added
- `unicorn_binance_websocket_api_manager.is_exchange_type()`
- support for subscribe/unsubscribe for CEX websockets
- `unicorn_binance_websocket_api_manager.get_stream_subscriptions()`
- `unicorn_binance_websocket_api_manager.increase_transmitted_counter()` and added output to 
  `print_summary()` and `print_stream_info()` 
- `split_payload()`
### Changed
- The 8004 char limit for URIs on websocket connect is bypassed via subscriptions with websocket.send() and 
`is_websocket_uri_length_valid()` allways returns `True` now!
### Fixed
- Subscribe/unsubscribe items of DEX websockets ([card #5](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/projects/5#card-23700264))
- `['receives_statistic_last_second']` dict is changing size during iteration. ([issue #37](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/37))

## 1.9.1
### Added
- Python version in print_stream_info() and print_summary()
### Fixed 
- Typo in text string

## 1.9.0
### Added 
- Endpoints for www.binance.com margin UserData listenkey ([issue #35](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/35))
### Changed
- Endpoints for www.binance.com spot UserData listenkey ([issue #35](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/35))
- Endpoints for www.binance.com futures UserData listenkey ([issue #35](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/35))

## 1.8.2
### Fixed
- Errors when creating private DEX streams ([issue #34](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/34))

## 1.8.1
### Changed
- Moved docs to GitHub pages

## 1.8.0
### Added 
- binance.com Futures websocket support and [example_binance_futures.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/example_binance_futures.py) and [example_bookticker.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/example_bookticker.py) ([issue#32](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/32))

## 1.7.0
### Added 
- binance.us websocket support and [example_binance_us.py](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/example_binance_us.py) ([issue#22](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/22))

## 1.6.6
### Fixed
- Trailing / is no longer accepted by the endpoints: 
https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/31

## 1.6.5
### Fixed
- 'websockets>=7.0' to 'websockets==7.0': Websockets 8 is released, and it seems to be not compatible

## 1.6.4
### Added
- Amount of active streams to icinga status msg
### Fix
- RuntimeError in _frequent_checks

## 1.6.3
### Fix
- 'except websockets.exceptions.InvalidStatusCode as error_msg:' moved to right place 

## 1.6.2
### Fix
- 'except websockets.exceptions.InvalidStatusCode as error_msg:' in connnection line 97 with restart
- 'except KeyError:' in connection line 162

## 1.6.1
### Fix
- get_monitoring_status_plain(): exception for outdated UnicornFy

## 1.6.0
### Added
- is_update_availabe_check_command()
- get_latest_version_check_command()
- get_latest_release_info_check_command()
### Changed
- get_monitoring_status_plain()
- get_monitoring_status_icinga()
- _start_monitoring_api()
### Removed!
- https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/blob/master/tools/icinga/ 
(to https://github.com/oliver-zehentleitner/check_unicorn_monitoring_api)

## 1.5.0
### Added 
- support for binance.org and testnet.binance.org websockets
- exchange name to icinga status msg
- binance_manager init: throw exception for unknown exchanges
- get_current_receiving_speed()
- exchange name and lib version to print_stream_info()
- current_receiving_speed to print_summary() and print_stream_info()
- get_exchange()
- set_private_dex_config() **(not in use for now)**
- subscribe_to_stream() - **(dont use in productive! It's not clean and will get rewritten and maybe change behaviour)**
- unsubscribe_from_stream() - **(dont use in productive! It's not clean and will get rewritten and maybe change 
behaviour)**
- _create_payload()
### Changed 
- rewrite create_websocket_url(): 
    1. a multiplex socket now returns false if it includes a single stream type like !userData, !Ticker or !miniTicker
    2. added support for binance.org Binance Chain DEX
- is_websocket_uri_length_valid() now always returns True for DEX websockets

## 1.4.0
### Added 
- support for binance.je (Binance Jersey) websockets
- logging on failure in `create_stream()`
- `add_string` in `print_summary()` and `print_stream_info()`
- `warn_on_update` in `get_monitoring_status_icinga()`, `get_monitoring_status_plain()` and `start_monitoring_api()`
- support for binance jersey https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/21
- show the used exchange in `print_summary()` and `print_stream_info()`
### Fixed
- removed space from `total_received_length` in `get_monitoring_status_icinga()` to avoid 'no data' error in ICINGA

## 1.3.10
### Added
- exception for `asyncio.base_futures.InvalidStateError` by DaWe35 
https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/18
### Changed
- create_stream() returns False if websocket URL is too long
### Fixed
- `is_websocket_uri_length_valid()` to work with !userData on the pre-test in `create_stream()` without api secrets

## 1.3.9
### Changed
- Docstrings for `markets` and `channels` to support: str, tuple, list, set
- Fine-tuning of perfdata output in `get_monitoring_status_plain()` and `get_monitoring_status_icinga()`

## 1.3.8
### Added
- `get_stream_buffer_length()` by DaWe35 https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/12
### Fixed
- the `stream_buffer` FIFO stack was a LIFO stack (Thanks to DaWe35 for recognizing and fixing this issue 
https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/pull/12)
- `get_stream_buffer_byte_size` returns now the real size of the stream_buffer

## 1.3.7
### Changed
- added round received items to 2 decimals instead of 0

## 1.3.6
- wrong version in manager class ...

## 1.3.5
### Added
- is_manager_stopping()
### Fixed
- is_update_available returns False if github API is not available.

## 1.3.4
### Changed
- get_monitoring_status_icinga(): update check

## 1.3.3
### Added
- get_monitoring_status_icinga(): reconnects and update check
- get_monitoring_status_plain()
- start_monitoring_api()
- 1 hour cache for release checks on GitHub
- stop_monitoring_api()
### Rewrite
- ./tools/icinga/check_binance_websocket_api_manager (check_command for ICINGA)
### Changed 
- example_monitoring.py

## 1.3.2
### Added
- example_monitoring.py and tools/check_binance_websocket_api_manager
- get_monitoring_status_icinga tests for available updates and changes the `return_code` to WARNING if an update is 
available. but I recognized an API ban from GitHub in cause of too many requests. I have to extend it ...
### Changed
- get_monitoring_status_icinga: changed `status` dict node to `return_code`

## 1.3.1
### Changed
- changing output of get_monitoring_status_icinga

## 1.3.0
### Added
- get_monitoring_status_icinga() in manager class

## 1.2.8
### Added
- lib version to print_summary()
### Fixed
- Typo in text in print_summary()
- KeyError in manager class row 148

## 1.2.7
### Fixed
- Bug in class UnicornFy: kline_close_time had the value kline_start_time
### Changed
- Moved UnicornFy from UNICORN Binance WebSocket API to its own [repository](https://github.com/oliver-zehentleitner/unicorn-fy) 
- connection handling (improved)

## 1.2.6
### Fixed
- `markets` in keepalive listen_key can come as str or as list and the routine only handled it as list, now str gets 
converted to list to keep the function working

## 1.2.5
### Added 
- "UTC" text to printed times
### Fixed
- listen_key 30 min cache

## 1.2.4
### Added 
- method to delete a listen_key
- binance_api_status added to print_stream_info()
### Changed
- README.md

## 1.2.3
### Changed
- rewrite coloring for status_code in print_summary
- ping_interval from None to 20 seconds
### Fixed
- listen_key keepalive didnt work propper

## 1.2.2
### Fixed
- TypeError in print_summary()

## 1.2.1
### Added 
- handling for status_code and used_weight from the binance REST Api (used for listen_key) - see `get_binance_api_status()`
### Fixed
- reconnect issues
### Changed
- log levels

## 1.2.0
### Changed
- if no method is provided to BinanceWebSocketApiManager when creating the instance, then all data will be written to 
the stream_buffer.
- comments and code in examples

## 1.1.20
### Changed
- show stream_buffer content if items len > 50
### Removed
- removed stream_buffer log

## 1.1.19
### Change
- renamed get_stream_data_from_stream_buffer to pop_stream_data_from_stream_buffer 
### Fixed 
- IndexError in pop_stream_data_from_stream_buffer

## 1.1.18
### Removed
- _forward_stream_buffer_data: system change - no pushing anymore, it's better to buffer everything and run an import class
 in a separate thread, that is able to reconnect to the database

## 1.1.17
### Changed
- rewrite of keepalive and frequentchecks restarts

## 1.1.16
### Changed 
- stream_buffer logging: log amount of items in buffer

## 1.1.15
### Changed 
- stream_buffer logging: log amount of items in buffer
### Fixed
- added two macOS specific exceptions to connection class for better reconnect management

## 1.1.14
### Fixed
- updated the "update" methods in manager class (error handling while no internet connection)
- trying other behaviour on "400 - bad request" error 
- added handling for -2015 error from get_listen_key_from_restclient in create_websocket_uri

## 1.1.13
### Changed
- changed the waiting time before setting a restart request on 400 error to 5 seconds in connection class
### Fixed
- replaced tabs in print_summary() with blanks

## 1.1.12
### Fixed
- KeyError in unicorn_binance_websocket_api_connection.py error exception 414
- UnicornFy was very buggy with ticker and miniTicker handling

## 1.1.11
### Fixed
- KeyError in unicorn_binance_websocket_api_manager.py

## 1.1.10
### Added
- restarting streams row to print_summary()
- show active restarting and stopped streams only if not 0
- error message handling for userData streams
- reconnect depends on disconnect reason now (network or api-settings)
### Fixed
- del restart request in stop_stream()

## 1.1.9
### Fixed
- !miniTicker and !userData didn't work in cause of lower case all currencies. added an exception for them.

## 1.1.8
### Added
- pypi_install_packaging_tools.sh
### Changed
- README.md
- Removed 2nd argument from binance_websocket_api_manager.stream_is_stopping()
### Fixed
- Tabs in print_summary() for windows platform
- Fixing format errors from auto reformat in unicorn_binance_websocket_api_connection

## 1.1.7 failed build

## 1.1.6
### Fixed
- Catching "ssl.SSLError" BinanceWebSocketApiConnection.receive()
- Improvment of reconnect on invalid URI caused by no network issue and a missing listen_key from Binance

## 1.1.5
### Added
- 30 min cache for Binance "listenKey" from rest api to avoid weight costs and hammering the Binance API on a 
flapping network connection
### Fixed
- Reconnect issue on userData stream
- Reset "has_stopped" attr from "stream_list" after a conncection restart
- Modyfied docstrings descriptions
- Tabs in print_summary() on windows
