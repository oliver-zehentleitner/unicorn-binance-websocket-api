# TASKS.md — Development Backlog

Tasks collected from codebase analysis (2026-04-01). Ordered by priority within each group.

---

## High Priority

### [ ] Bump UBRA dependency pin once Portfolio Margin PAPI methods are released
- `connection_settings.py`/`restclient.py` already route `binance.com-portfolio_margin`
  to `ubra.portfolio_margin_stream_get_listen_key()` / `_keepalive()` / `_close()`
  (see [issue #452](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/452))
- These methods don't exist in any released `unicorn-binance-rest-api` yet — only in the
  local dev copy. Bump the `unicorn-binance-rest-api` pin in `pyproject.toml`/`setup.py`/
  `requirements.txt` only after that UBRA version has actually shipped to PyPI
- Until then, `binance.com-portfolio_margin` streams will fail at listenKey acquisition on
  a freshly `pip install`ed UBWA

### [x] Remove DEX support
- Remove `binance.org` and `binance.org-testnet` from `connection_settings.py` (`Exchanges` enum, `DEX_EXCHANGES`, `CONNECTION_SETTINGS`)
- Remove all `if self.exchange == "binance.org"` branches in `sockets.py`, `manager.py`, `restclient.py`
- Remove `dex_user_address` from manager and `stream_list` dict
- Update `AGENTS.md` supported exchanges table
- Bump version

### [x] Rebuild listen key handling — remove REST-based approach
- Binance removed REST listenKey endpoints for Spot/Margin in February 2026
- PR #411: new WS API subscription flow (`userDataStream.subscribe.signature`) for Spot/Margin
- PR #413: deadlock fix in `create_websocket_uri()` (get_number_of_subscriptions inside stream_list_lock)
- Futures exchanges keep the existing listenKey flow (unaffected)
- `_ping_listen_key()` and `restclient.keepalive_listen_key()` still active for Futures — leave as-is

### [x] Remove check_lucit_collector + Icinga support
- Removed `get_latest_release_info_check_command()`, `get_latest_version_check_command()`,
  `is_update_available_check_command()`, `get_monitoring_status_icinga()`,
  `start_monitoring_api()`, `stop_monitoring_api()`, `_start_monitoring_api_thread()`
- Removed `restserver.py` entirely
- Removed `flask`, `flask_restful`, `cheroot` dependencies
- Cleaned `get_monitoring_status_plain()` of all check_lucit references; method retained for programmatic use

### [x] Upgrade websockets + Python 3.14 support (GIL only — no-GIL in PR 3)
- Upgraded `websockets==11.0.3` → `>=14.0`
- Updated exception handling: `InvalidStatusCode` → `websockets.exceptions.InvalidStatus` with `.response.status_code`
- Added Python 3.14 to CI (`unit-tests.yml`) and wheel builds (`build_wheels.yml`)
- Dropped Python 3.8 (EOL), minimum is now 3.9
- Updated `setup.py`, `pyproject.toml`, `requirements.txt`, `environment.yml`, `meta.yaml`

### [ ] Add rate-limit backoff strategy (429 handling)
- Currently: 429 response from Binance crashes the stream (`manager.py:_run_socket()`)
- Implement exponential backoff before restart on 429
- Log clearly how long the backoff will be
- Consider a global rate-limit state shared across streams

---

## Medium Priority

### [ ] Replace stream_list dict entries with @dataclass
- `manager.py:_add_stream_to_stream_list()` — 30+ key raw dict per stream
- Create `StreamState` dataclass in new file `stream_state.py`
- Gives: type safety, IDE autocomplete, typo protection, easier refactoring
- No behavioral change required

### [ ] Fix linear request_id scan in sockets.py
- `sockets.py:174–186` — scans raw JSON string for each pending request_id as substring
- O(n×m), false-positive-prone, runs on every received message for WS API streams
- Fix: parse JSON once, extract `id` field directly, use dict lookup

### [ ] Modernize SOCKS5 proxy using websockets native support
- Current implementation in `connection.py` manually creates a `socks.socksocket()` (PySocks) and passes it
  via `sock=` to `websockets.connect()` — a low-level workaround
- websockets 14 supports proxies natively via the `proxy` parameter in `connect()`
- Replace the manual PySocks socket setup with `proxy="socks5://user:pass@host:port"` (or similar)
- Allows removing the manual `netloc` parsing, `host:port` split, and `socks.socksocket` setup in `__aenter__`
- May allow simplifying or removing the PySocks (`socks`) dependency

### [ ] Remove wildcard imports
- `from .exceptions import *` in `manager.py`, `connection.py`, `sockets.py`
- Replace with explicit imports
- Enables proper static analysis with mypy/pyright

---

## To Discuss

### [ ] GitHub update check — make async or lazy
- `__init__` currently makes a synchronous HTTP request to `api.github.com` on every instantiation
- Options: (a) move to background thread, (b) make lazy (only on first explicit call), (c) remove entirely
- Decision needed before implementing

---

## Done / Accepted as-is

- **Thread-per-stream model** — intentional design. Isolation allows killing a thread+loop atomically. Works well in practice.
- **API secrets in stream_list** — acceptable for a developer-facing library
- **SSL verification flag** — acceptable for a developer-facing library
