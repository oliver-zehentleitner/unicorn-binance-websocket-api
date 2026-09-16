# Stream loop (`sockets.py` / `connection.py`)

## `recv()` runs without timeout after the first receives - `stop_stream()` waits for the next message

**Type:** decision
**Status:** active
**Evidence:** confirmed
**Source:** maintainer confirmation 2026-09-10; `connection.py` (`timeout_disabled`), git history: threshold `processed_receives_total > 3` predates commit `fd370d8a` (2024-03-27, which added the `timeout_disabled` flag), today `> 10`

`BinanceWebSocketApiConnection.receive()` wraps `recv()` in
`asyncio.wait_for(..., timeout=1)` only until a stream has more than 10
processed receives; from then on, as long as the stream has subscriptions,
it awaits `recv()` without a timeout. `stop_stream()` merely sets
`stop_request`, which `receive()` checks at the top of each call.
Consequence: on a stream that has subscriptions but currently receives
nothing, a stop (or crash request) takes effect only when the next message
arrives - or when the peer closes. Against Binance this is invisible because
data keeps flowing; it surfaced in the local-server unit test for the
websocket-library switch, where the test server had to send a heartbeat for
the manager to shut down.

**Reason:** deliberate. `wait_for()` costs a task plus a timer per message,
which is not wanted on the hot path once a stream is known to be alive. The
timed phase at the start exists for the opposite case: a loop that never
receives anything (bad subscription, dead endpoint) must still be stoppable,
so the first receives are polled with a 1 s timeout until the stream has
proven it delivers data.

**Accepted consequence:** the stop latency on an established but currently
idle stream is acceptable to the maintainer; not a bug.

**Rejected alternative:** keeping a (long) timeout permanently - rejected
because of the per-message overhead. Closing the websocket from
`stop_stream()` directly was not in contention.

## Per-message work in the loop that is not the transport - profiled

**Type:** decision
**Status:** active
**Evidence:** confirmed
**Source:** profiling pass 2026-09-10: cProfile of the stream thread (300k aggTrade messages from the local replay server, picows) plus a cumulative ablation with lean replacements of the hot-path methods, both libraries, median of 3; benchmark harness `dev/test_websocket_library_benchmark.py`

The websocket-library benchmark showed a constant ~5 µs per message that
UBWA spends on top of either library (raw libraries 2-3 µs/msg, through UBWA
6-9 µs/msg). Profiling attributes it as follows, per received message:

- **18 `logger.debug(f"...")` calls** while debug logging is off. The
  f-string is built before `debug()` checks the level, ~0.1 µs each. 12 of
  them are `stream_list_lock was entered` / `Leaving stream_list_lock` pairs
  inside `set_heartbeat()`, `increase_received_bytes_per_second()` and
  `increase_processed_receives_statistic()`; the rest are entry logs of
  `receive()`, `is_stop_request()`, `is_crash_request()` (each with a
  `get_debug_log()` call) and the dispatch log in `start_socket()`.
- **7 lock acquire/release cycles** (`stream_list_lock` x5,
  `total_received_bytes_lock`, `total_receives_lock`), ~0.17 µs each.
- **Duplicate work:** `set_heartbeat()` runs twice per message (in
  `receive()` and at the top of the loop iteration), `is_stop_request()` and
  `is_crash_request()` twice (loop condition and `raise_exceptions()` inside
  `receive()`).
- `sys.getsizeof(str(msg))` for the byte statistics (~0.1 µs, and it counts
  the str object header, not the payload).

**Ablation, small messages (0.2 KB), through the full stack, `raw_data`:**

| Stage (cumulative) | websockets msgs/s | CPU µs/msg | picows msgs/s | CPU µs/msg |
|---|---|---|---|---|
| A baseline | 119,880 | 8.41 | 166,906 | 6.07 |
| B no debug f-strings in the hot path | 160,992 | 6.28 | 268,185 | 3.82 |
| C + per-stream counters without `stream_list_lock` | 181,873 | 5.57 | 306,244 | 3.34 |
| D + heartbeat/stop/crash once per iteration | 181,969 | 5.56 | 344,516 | 2.93 |
| E + `len()` instead of `sys.getsizeof(str())` | 191,839 | 5.30 | 370,564 | 2.76 |

Raw libraries for reference: websockets ~3.0 µs, picows ~2.0 µs. Stage E
leaves ~0.8 µs of UBWA overhead with picows (dispatch cascade, one debug
call, counters) and ~2.3 µs with websockets.

**Lock-free counters, why it is safe and where it is not:** the per-stream
fields (`last_heartbeat`, `processed_receives_total`, the per-second
dicts) have a single writer, the stream's own thread; `_frequent_checks()`
in the manager thread reads them and prunes old timestamp keys after
`copy.deepcopy()` under the lock. A lock-free `+=` on an *existing* key is
safe; *inserting* a new timestamp key (once per second) while the manager
thread deep-copies the dict can raise "dictionary changed size during
iteration", so the insert path must keep the lock. That is the split the
ablation's stage C did not yet make - the production change must.

**Implemented** (same day, all four stages, PR "perf: slim down the stream
loop hot path"): the 18 debug f-strings in the hot path are gone (the
`lock entered/leaving` pairs and the per-call entry logs; `send()` and the
non-hot-path logs are untouched), the per-stream counters take
`stream_list_lock` only on the once-per-second key insert, `receive()` no
longer duplicates heartbeat and stop/crash checks, byte statistics use
`len()` of the payload. Result through the full stack, `raw_data`, median
of 3, same machine:

| Scenario | ~msg size | msgs | websockets msgs/s | picows msgs/s | picows speedup | websockets CPU µs/msg | picows CPU µs/msg |
|---|---|---|---|---|---|---|---|
| small_aggtrade | 0.2 KB | 300,000 | 201,912 | 403,316 | 2.00x | 5.1 | 2.5 |
| medium_kline | 0.3 KB | 150,000 | 195,460 | 371,019 | 1.90x | 5.2 | 2.9 |
| large_depth20 | 1.0 KB | 60,000 | 153,187 | 259,960 | 1.70x | 6.8 | 4.1 |
| xlarge_depth_diff | 9.1 KB | 30,000 | 64,172 | 67,972 | 1.06x | 16.3 | 15.4 |
| huge_ticker_arr | 453.9 KB | 600 | 1,768 | 1,662 | 0.94x | 608.7 | 641.0 |
| multiplex_mix | 0.2 KB | 120,000 | 180,406 | 334,188 | 1.85x | 5.7 | 3.2 |

Compared with the pre-optimization table in `websocket-library.md`:
websockets 116k -> 202k msgs/s (1.7x), picows 163k -> 403k msgs/s (2.5x) at
0.2 KB. picows now sits at ~2.5 µs/msg against ~2.0 µs for the raw library.

**Rejected alternative for the debug logs:** keeping them behind an
`if self.debug:` guard. Rejected because the removed lines carried no
information (`lock was entered` / `Leaving lock`, entry of a getter), and
even a guarded call costs ~0.05 µs x 18 per message. Logs that report an
event or an error stay.

**Revisit when:** a second writer for `last_heartbeat`,
`processed_receives_total` or the per-second dicts is introduced - then the
lock-free increments in `set_heartbeat()`, `increase_received_bytes_per_second()`
and `increase_processed_receives_statistic()` need the lock back.

## Endpoint responses are detected in the first 256 characters, not by scanning the payload

**Type:** decision
**Status:** active
**Evidence:** confirmed
**Source:** big-message re-measurement 2026-09-16 (`websocket-library.md`, "Benchmark results"); `dev/test_websocket_library_benchmark.py` before/after on the same machine (8 cores, websockets 16.0, picows 2.3.0), median of 3; `RESPONSE_SCAN_CHARS` in `sockets.py`; unit test `test_response_markers_scanned_in_head_only`
**Revisit when:** Binance changes a response envelope so that `result`, `error` or the request id can sit beyond the first 256 characters (a new leading field with variable-length content)

Every received message used to be scanned twice in full, `"error" in msg`
and `"result" in msg`, to route endpoint responses into the error/result
ringbuffers; in WS API mode the pending request ids were searched the same
way. On a 454 KB `!ticker@arr` message those scans were ~315 µs of the
~600 µs UBWA spent per message - more than receiving and decoding it - and
they were what made UBWA slower than the loopback wire in the replay
benchmark (the trigger of the picows big-message artifact). Now the checks
run on `msg[:RESPONSE_SCAN_CHARS]` (256).

**Reason 256 is enough:** the markers are in the JSON head by construction.
Stream endpoint: `{"result":null,"id":N}`, `{"error":{...},"id":N}`. WS
API: `{"id":"<uuid, 36 chars>","status":NNN,"result"|"error":...}` puts the
marker at ~55 characters; `rateLimits` follows the result. 256 leaves
headroom for longer ids and whitespace. A data message (`{"stream":...`,
`{"e":...`) never carries these keys in its head.

**Also a correctness fix:** a data payload that contained the word `error`
or `result` anywhere (a symbol note, a text field) was copied into the
ringbuffers on top of being delivered - the new unit test sends such a
payload and fails on the previous code.

**Rejected alternatives:**

- Parse every message with `orjson` and look at the keys: costs a full parse
  in `raw_data` mode where none is otherwise needed, on the same order as
  the scans it would replace.
- Exact `startswith()` checks (`{"result"`, `{"error"`, `{"id"`): brittle
  against field order and whitespace in the envelopes, and the WS API
  envelope starts with `id`, not with the marker.
- Leave it: the scans were the dominant per-message cost for anything above
  ~10 KB, see the table.

**Before/after, through the full stack, `raw_data`, median of 3:**

| Scenario | ~msg size | websockets msgs/s before -> after | picows msgs/s before -> after | websockets CPU µs/msg | picows CPU µs/msg |
|---|---|---|---|---|---|
| small_aggtrade | 0.2 KB | 199,903 -> 199,732 | 389,817 -> 380,421 | 5.1 -> 5.1 | 2.7 -> 2.7 |
| large_depth20 | 1.0 KB | 156,952 -> 167,888 | 277,895 -> 315,078 | 6.9 -> 6.5 | 4.0 -> 3.6 |
| xlarge_depth_diff | 9.1 KB | 66,368 -> 100,373 | 67,336 -> 117,231 | 16.0 -> 10.9 | 15.7 -> 9.4 |
| huge_ticker_arr | 453.9 KB | 1,810 -> 4,540 | 1,690 -> 3,904 | 625.5 -> 281.5 | 678.7 -> 318.5 |

`dict` mode, 454 KB: websockets 391 -> 468 msgs/s, picows 427 -> 522. Small
messages are unchanged (the slice of a 200-byte string is noise). In the
loopback firehose replay picows still trails websockets at 454 KB in
`raw_data` mode (0.86x) because the consumer is still slower than the wire
there; with `--rcvbuf 131072` the same scenario is picows 1.55x (5,218 vs
3,375 msgs/s, 213 vs 334 µs) and 9 KB is 1.41x.
