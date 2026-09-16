# Context index

## 0

## 1

## 2

## 3

## 4

## 5

## 6

## 7

## 8

## 9

## A

## B

## C

## D

## E

## F

## G

## H

- [history.md](history.md) — the repo's LUCIT-Systems-and-Development origin, the Icinga/monitoring removal, and a stale-docs fix that came out of it

## I

## J

## K

## L

## M

## N

## O

## P

- [portfolio-margin.md](portfolio-margin.md) — why `binance.com-portfolio_margin` is scoped to listenKey-only and kept outside `BINANCE_FUTURES_EXCHANGES`

## Q

## R

## S

- [stream-loop.md](stream-loop.md) — untimed `recv()` after the first receives (deliberate, `wait_for` overhead; stop latency on idle streams accepted) and the profiled ~5 µs per message UBWA used to add (debug f-strings, locks, duplicate checks), the ablation, and the hot-path slim-down that removed most of it, and why endpoint responses are detected in the first 256 characters instead of by scanning the whole payload

## T

## U

## V

## W

- [websocket-library.md](websocket-library.md) — why `picows` is integrated via its `websockets`-compatible API and the core listener API was measured and not built, separate exception families (incl. the `InvalidStatus.response` shape difference), fail-loud selection, opt-in/non-default status plus the 24 h soak gate before the release, the proxy path (URL passed natively to both libraries since websockets 15.0 / picows 2.3.0, PySocks gone), the websockets credential-decoding gap (#1761) refused at construction, the unverified-TLS incident on that path, the benchmark numbers behind it, and the big-message artifact of the loopback replay (firehose sender vs. `SO_RCVBUF`)

## X

## Y

## Z
