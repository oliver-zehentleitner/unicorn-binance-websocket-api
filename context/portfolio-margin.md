# Portfolio Margin exchange (`binance.com-portfolio_margin`)

## Scoped to listenKey/user-data only, no market-data or WS API support

**Type:** constraint
**Status:** active
**Evidence:** confirmed
**Source:** issue [#452](https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api/issues/452), commit `b53277f2`

`binance.com-portfolio_margin` only supports the user-data stream lifecycle (`wss://fstream.binance.com/pm/ws/<listenKey>`, listenKey acquired/kept-alive/closed via UBRA's PAPI methods). There is no `websocket_api_base_uri` (no WS API/order placement) and no public market-data endpoint for this exchange.

**Reason:** Binance's Portfolio Margin API itself doesn't expose those surfaces (yet) — the scope here tracks what's actually available upstream, not an arbitrary internal limitation.

## Deliberately outside `BINANCE_FUTURES_EXCHANGES`

**Type:** decision
**Status:** active
**Evidence:** confirmed
**Source:** commit `b53277f2`: "deliberately outside BINANCE_FUTURES_EXCHANGES"

**Rejected alternative:** folding `binance.com-portfolio_margin` into the existing `BINANCE_FUTURES_EXCHANGES` group, since it's futures-adjacent and could reuse that code path.

**Reason it was rejected:** Portfolio Margin keeps the legacy `/ws/<listenKey>` URL form, while `binance.com-futures` / `binance.com-futures-testnet` already moved to the newer `/private/ws?listenKey=...&events=...` form. Grouping them would mean branching on exchange inside shared futures-path code for a URL-shape difference — kept as its own exchange entry instead so the routing stays a lookup, not a conditional.

## Follow-up: graceful degradation for the not-yet-released dependency

**Type:** workaround
**Status:** active
**Evidence:** confirmed
**Source:** commit `f44ee416`, found during self-review of PR #453

`_init_ubra()` previously let UBRA's `UnknownExchange` exception propagate uncaught whenever the installed UBRA version didn't yet recognize `binance.com-portfolio_margin` (true for any UBRA release before PAPI support landed). That crashed the calling stream thread instead of degrading to `(None, None)`, which is what the surrounding `AttributeError` handling was already designed to produce. `get_listen_key()`, `keepalive_listen_key()`, and `delete_listen_key()` now check `_init_ubra()`'s return value and bail out cleanly instead.

**Note:** this is a controlled degradation for a specific, known "dependency not released yet" condition on a single exchange's optional listenKey path — not a general precedent for swallowing errors. The suite's broader convention is to fail loud on broken invariants rather than silently continue; this case is about a well-defined missing-feature check, not a corrupted-data path.

**Revisit when:** UBRA ships full Portfolio Margin support as a stable released version — at that point `UnknownExchange` for this exchange should no longer be reachable in practice, and the degradation path becomes dead code worth re-checking.
