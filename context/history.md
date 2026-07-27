# History

## Repo origin: LUCIT-Systems-and-Development

**Status:** superseded — repo now lives under `oliver-zehentleitner`
**Evidence:** confirmed
**Source:** git history; earliest commits reference `github.com/LUCIT-Systems-and-Development/unicorn-binance-websocket-api`

The repo was previously hosted under the `LUCIT-Systems-and-Development` GitHub org, with LUCIT branding, a LUCIT licensing/monitoring layer, and a contributor-copyright-assignment clause in `CONTRIBUTING.md`. It moved to `oliver-zehentleitner` and was cleaned up across many commits in 2026 (`remove LUCIT`, `Clean remaining LUCIT references outside the conda pipeline`, etc.) — same cleanup wave as the rest of the suite (see `unicorn-binance-suite`'s `context/history.md`).

**Reason:** LUCIT is no longer part of how this project is licensed, distributed, or supported.

**Also part of the same cleanup (commit `7f73d966`):** the in-repo `build_conda.yml` workflow was removed — conda-forge's own feedstock (`unicorn-binance-websocket-api-feedstock`) is the only conda build path now, same as the rest of the suite (see `unicorn-binance-suite`'s `context/packaging.md` for the fuller reasoning, which applies here too).

## Icinga/monitoring REST server removed, not just rebranded

**Status:** active
**Evidence:** confirmed
**Source:** commit `ee615105`

`restserver.py` (a Flask REST server exposing only a LUCIT/Icinga monitoring check-command endpoint) was deleted entirely — `start_monitoring_api()`, `stop_monitoring_api()`, `get_monitoring_status_icinga()`, and the related check-command methods are gone, along with the `flask`/`flask_restful`/`cheroot` dependencies. `get_monitoring_status_plain()` was kept, stripped of `check_lucit` references.

**Reason:** this was LUCIT-specific monitoring-vendor integration (Icinga check commands), not a generic feature — removing it was part of decoupling from LUCIT, not a reduction in supported functionality for actual users of the library.

**Stale docs caught while writing this:** `AGENTS.md`'s "Directory Structure" and "Key Classes" tables still listed `restserver.py` / `BinanceWebSocketApiRestServer` as if it still exists — it doesn't (verified: not present in `unicorn_binance_websocket_api/`). Fixed alongside this history entry. Don't confuse it with `restclient.py` (`BinanceWebSocketApiRestclient`), which is unrelated and still active — a REST client used for stream/listenKey management, not a REST server.

## Hardcoded LUCIT org URL broke `get_latest_release_info()`

**Status:** superseded — fixed
**Evidence:** confirmed
**Source:** commit `c2eb03ac`

After the repo moved from `LUCIT-Systems-and-Development` to `oliver-zehentleitner`, `get_latest_release_info()` still queried the GitHub API using the old org's URL, so the built-in "is an update available" check silently looked at the wrong (now-stale or gone) repo.

**Reason it slipped through:** an org/repo rename doesn't fail loudly in a hardcoded API URL — the old endpoint may keep responding (redirect or stale data) rather than erroring, so nothing surfaced the mismatch until it was checked directly.

**Revisit when:** the repo or org is renamed/moved again — grep for hardcoded `github.com/<org>/...` API URLs before assuming a rename is purely cosmetic.
