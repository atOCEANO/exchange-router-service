<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <b>Validator Guide</b> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Validator Guide

Adapter compliance is validated via the validator package at `tests/validator/`. The runner reads `/{exchange}/capabilities` and runs only the probes that apply to the features the adapter claims to support, so an honest capabilities map is the difference between a clean test run and noise. All declared endpoints must pass before submitting a Pull Request.

Getting capabilities wrong in either direction surfaces as a failure, not a skip. Claim `True` for a feature the adapter does not implement and the route either 501s (recorded by the `error_paths` probe) or succeeds in a way that contradicts the schema (recorded by `capabilities_drift`). Claim `False` for a feature that actually works and `capabilities_drift` flags it: a `rest: False` route that returns 200 is a hard FAIL.

<br>
<br>

## Quick orientation

| You want to ... | Open this file |
| --- | --- |
| Run the suite or interpret results | this document, sections [Running the suite](#running-the-suite) and [Run output](#run-output) |
| Add a new probe | `tests/validator/probes/<your_probe>.py` next to a similar one. Register in `probes/__init__.py::market_probes` or `exchange_probes` |
| Tune an env knob (timeouts, concurrency, freshness threshold) | `tests/validator/config.py` |
| Change how results are aggregated or written | `tests/validator/results.py` (`Aggregator`) and `tests/validator/reporting.py` (`write_json`, `LiveReporter`) |
| Change the orchestration (capability walk, semaphores) | `tests/validator/runner.py` |
| Change the CLI surface | `tests/validator/__main__.py` |

<br>
<br>

## Layout

The CLI in `__main__.py` invokes `runner.run(...)`, which executes a fixed top-of-run sequence (`/status`, `/version`, `/exchanges`), then fans out to one task per exchange. Inside each exchange task it reads `/capabilities`, runs the exchange-level probes, then iterates per market type, picks a symbol, and runs the market-level probes filtered by `applies(ctx)`. Results feed into an `Aggregator`; `LiveReporter` updates the console as they arrive and `write_json` persists the final summary.

| File | Class / Role | Lines (approx) |
| --- | --- | --- |
| `__main__.py` | CLI entry: `python -m tests.validator [exchange ...]` | 100 |
| `config.py` | env knobs, interval map, symbol picker, theoretical-max | 120 |
| `runner.py` | capability walk, semaphore gating, `asyncio.gather` | 285 |
| `results.py` | `ProbeResult`, `ErrorType`, `Aggregator` | 175 |
| `reporting.py` | JSON writer, rich live console, end-of-run failure table | 135 |
| `probes/__init__.py` | re-exports + `market_probes()` / `exchange_probes()` registry | 75 |
| `probes/base.py` | `Probe` ABC, `ProbeContext`, `fetch`, validators, REST throttling | 190 |
| `probes/snapshot.py` | `SnapshotProbe` + ticker / book / mark check helpers | 110 |
| `probes/orderbook.py` | `OrderBookProbe` (multi-depth, ordering, freshness) | 130 |
| `probes/trades.py` | `TradesProbe` (multi-limit, uniqueness, ordering) | 130 |
| `probes/paginated.py` | `PaginatedProbe` (recent + 4 anchored sub-probes) | 250 |
| `probes/websocket.py` | `WebSocketProbe` + `WS_CHANNEL_MODEL` registry | 165 |
| `probes/info.py` | `InfoProbe` (`/info` symbol metadata) | 80 |
| `probes/markets.py` | `MarketsProbe` (`/markets` listing) | 70 |
| `probes/capabilities.py` | `CapabilitiesConsistencyProbe` (drift detection) | 140 |
| `probes/errors.py` | `ErrorPathProbe` (404/400 negative paths) | 60 |
| `probes/cross_route.py` | `CrossRouteConsistencyProbe` (ticker vs book vs mark) | 90 |
| `probes/symbol.py` | `SymbolResolutionProbe` (canonical spot symbol resolves) | 70 |

<br>
<br>

## The `Probe` ABC

Every probe inherits from a single ABC: `Probe` in `probes/base.py`. Each subclass owns one route or one cross-cutting check, declares whether it `applies(ctx)`, and returns `List[ProbeResult]` from `run(ctx)`. Each `ProbeResult` carries a structured `evidence: dict` (offending values, request params) plus a categorical `ErrorType`. Aggregations by failure type read directly from `ErrorType` rather than parsing free-form text.

The base class gives every probe two helpers so probes don't repeat their identity at every result construction:

- `self.result(ctx, name, started, status=..., error_type=..., message=..., evidence=...)` builds a `ProbeResult` populated with `self.route`, `self.kind`, the context's exchange/market/symbol, and the latency derived from `started → time.time()`.
- `self.http_failure(ctx, name, started, status_code, data, net_err)` builds the standard fail result for non-200 responses.

<br>
<br>

## What the runner validates

Probes run against every advertised route. Each probe carries the capability fields that gate it.

### Snapshot routes

`ticker`, `book_ticker`, `mark_price`. Pydantic shape, freshness (`now - timestamp < SNAPSHOT_FRESHNESS_MS`), and route-specific invariants: ticker `low_24h <= price <= high_24h`, book `bid <= ask` with positive sizes, mark price positive.

### Orderbook

Probed at every depth in `caps.depths`, or `[1, 20, max_depth]` when no discrete list is declared. Each level: bids strictly descending, asks strictly ascending, top-of-book not crossed, no zero or negative price or qty, `len <= depth`. Failure evidence carries the offending pair (`{"side": "bids", "idx": 5, "prev": 30001.5, "this": 30001.7}`).

### Trades

Probed at `[1, min(100, max_limit), max_limit]`. Asserts ascending timestamps (router contract), unique `id` values, every `side in {"buy","sell"}`, `len <= limit`.

### Paginated routes

`agg_trades`, `candles`, `funding_rate`, `open_interest`, `liquidations`, `long_short_ratio`. Five sub-probes per route:

- `recent`: no `start`, big limit, validates ascending order and freshness. Theoretical-max calibration tied to `retention_ms` and `period_ms`, so a 1-week candle ask for 3000 records is treated as PASS at ~250 records. The freshness threshold defaults to `3 × period_ms + 30s`; per-`(exchange, market_type, period)` overrides live in `config.py::FRESHNESS_OVERRIDE_MS` for upstreams documented to publish slowly (e.g. KuCoin INVERSE 1m skips empty buckets, so the override sets 600s).
- `limit=1`: must return exactly 1.
- `limit=5`: must return at most 5; > 5 is a hard FAIL.
- `start=1h_ago` and `start=24h_ago`: last record at or before the anchor (skipped if `paginated: False`); skipped at 24h for periods > 12h.
- `start=future`: now + 1h, expected empty (returning records is a WARN).

### WebSocket

Holds the connection for `WS_TEST_DURATION` (default 180s) and validates **every** frame against `WS_CHANNEL_MODEL`. Minimum frame thresholds are per-channel (`config.py::WS_MIN_FRAMES`):

| Channel | Min frames | Rationale |
| --- | --- | --- |
| `orderbook` | 10 | Very chatty |
| `ticker`, `book_ticker` | 5 | Steady cadence |
| `trades`, `agg_trades`, `mark_price` | 1 | Activity-dependent |
| `liquidations` | 0 | Rare event; absence is not a failure |

Below threshold: WARN. Zero frames with threshold > 0: FAIL. Zero frames with threshold = 0: PASS with note `no_frames_in_window`. Any malformed frame: FAIL with the offending payload in evidence.

### Capabilities drift

For every route advertised as `rest: False`, calls the route and asserts the service returns non-200 (501 preferred). For `orderbook`, requests `depth = max_depth + 1` and asserts the response either errors or clamps. Drift findings appear under their own report section and are hard FAILs.

### Cross-route consistency

WARN-only. Asserts `|ticker.price - (book.bid + book.ask)/2| / ticker.price < 0.5%`, and on linear `|mark_price - ticker.price| < 1%`. Live-market jitter never produces FAILs here.

### Symbol resolution

SPOT-only. Hits a known canonical symbol per exchange (`BTCUSDT` everywhere except Kraken which uses `XBTUSDT`) and asserts the response symbol matches. Implicitly validates KuCoin/OKX `_spot_symbol_map` and Kraken's `_ensure_spot_ws_map`.

### Negative paths

Runs first. Unknown exchange (expects 404), bad market type (expects 400/422), bad symbol (expects 400/404).

### /info and /markets

`InfoProbe` validates the SymbolInfo list and asserts `min_qty <= max_qty` and non-negative precision on a sample. `MarketsProbe` checks for uppercase, no slash, and no duplicates.

<br>
<br>

## Concurrency

```
MAX_CONCURRENT_EXCHANGES            default 5     all exchanges in parallel
MAX_CONCURRENT_REST_PER_EXCHANGE    default 4     REST probes within an exchange
MAX_CONCURRENT_WS_PER_EXCHANGE      default 32    WS probes within an exchange
MIN_REST_INTERVAL_MS                default 250   minimum spacing between REST calls to the same exchange
```

REST and WS probes for the same exchange run in parallel against a shared service (WS holds 180s windows; serializing them after REST roughly doubles wall time). Use `--parallel 1` (or set `MAX_CONCURRENT_EXCHANGES=1`) for serial debugging.

### Why the validator throttles itself (`MIN_REST_INTERVAL_MS`)

The validator runs hundreds of REST probes per exchange in a few minutes. Real clients of this microservice almost never produce that kind of load. `MIN_REST_INTERVAL_MS` keeps the validator from tripping upstream rate-limit responses that would otherwise look like adapter failures in the report. This is test hygiene, not a rate limiter the microservice relies on for production safety.

The check sits inside the validator's `fetch` helper and derives the exchange name from the endpoint path (`/<exchange>/<market_type>/...`). Default 250ms is conservative against accidental concurrent bursts when `MAX_CONCURRENT_REST_PER_EXCHANGE > 1`. Set `MIN_REST_INTERVAL_MS=1000` (or higher) for an extra-safe run at high concurrency, or `0` to disable entirely. Endpoints without an exchange prefix (`/status`, `/version`, `/exchanges`) are not throttled. WS probes are not affected.

Production rate-limit safety lives in the adapters, not in this knob (see [Rate limit headers and proactive backoff](Contributor_Guide.md#rate-limit-headers-and-proactive-backoff) in the Contributor Guide). The two layers are independent: when both apply, the slower spacing wins.

<br>
<br>

## Run output

Every run writes a fresh folder under `tests/runs/<YYYY-MM-DD_HH-MM-SS>[_<exchanges>]/` containing a single file:

- `results.json`: machine-readable, versioned (`schema_version: 1`). Contains the full `ProbeResult` list plus the aggregated `summary` block:
  - `totals`: pass / warn / fail / skip counts.
  - `by_error_type`: categorical breakdown (network, schema, logic, empty, ...).
  - `by_exchange`: per-exchange totals.
  - `rest_latency_by_route`: REST round-trip latency per route (count, p50, p95, p99, max). REST only because WS probes hold the connection for the full `WS_TEST_DURATION` window, which is a configured duration rather than a service-latency signal.
  - `ws_by_channel`: per-channel WS metrics (count, frames received p50/p95/max, time-to-first-frame p50/p95). Surfaces the meaningful WS measurements (throughput and warm-up time) instead of forcing them into a latency bucket.

The `tests/runs/` folder is gitignored. The Docker compose mount `volumes: - .:/app` means runs from inside the container land on the host filesystem. The console renders a live progress bar during the run and a failure table at the end.

<br>
<br>

## Running the suite

```bash
# Point at the running router (default http://localhost:8040)
export API_URL=http://localhost:8040

# Run against all registered exchanges
python -m tests.validator

# Test a specific adapter
python -m tests.validator okx

# Test multiple adapters
python -m tests.validator binance bybit

# Control adapter parallelism via CLI (overrides MAX_CONCURRENT_EXCHANGES)
python -m tests.validator --parallel 1                  # serial, one adapter at a time
python -m tests.validator -p 2 binance bybit kraken     # two adapters in flight at any time

# Extended WebSocket observation window
WS_TEST_DURATION=300 python -m tests.validator
```

`python tests/validate_adapters.py [...]` is a thin shim that forwards to the same entry point. Both forms accept identical arguments.

<br>
<br>

## How to add a new probe

1. Pick the closest existing probe and copy its file. Most new probes look like `InfoProbe` (single REST call) or `SnapshotProbe` (REST call + per-route check function).
2. Set the three class attributes: `kind` (`"rest"` or `"ws"`), `route` (the capability key), `name` (what shows up in the JSON; usually equal to `route` unless you have sub-probes).
3. Implement `applies(ctx)`. Gate on `ctx.caps_slice.get(self.route, {}).get("rest")` (or `"ws"`) so the probe is only run when the adapter advertises the feature. Returning `False` for unsupported markets is correct, not lazy.
4. Implement `async run(ctx)`. Always:
   - Capture `started = time.time()` before the network call.
   - On non-200, return `[self.http_failure(ctx, self.name, started, status, data, err)]`.
   - On 200, validate shape with `validate_one` or `validate_list`, then route-specific invariants.
   - Build the success result with `self.result(ctx, self.name, started, status="pass", error_type=ErrorType.OK, message=..., evidence=...)`.
5. Register it in `probes/__init__.py`: append to `market_probes()` for per-market probes, or to `exchange_probes()` for top-of-exchange ones (rare, currently `ErrorPathProbe`).
6. If you added a new route the adapters expose, also update `CapabilitiesConsistencyProbe`'s route list so drift is caught for it.

A new probe is ~50-80 lines. If yours is bigger, look at `PaginatedProbe` for how to split into sub-probe methods.

<br>
<br>

## Code conventions

- **Result construction**: always go through `self.result(...)` or `self.http_failure(...)`. No bare `ProbeResult(...)` calls inside probe code.
- **Evidence**: keep it small and machine-readable. Numbers, strings, flat dicts. The `body_preview` truncation in `http_failure` (200 chars) is the precedent.
- **No raw `print`**: console output is owned by `LiveReporter` and `render_failure_summary` in `reporting.py`. Use `logger` for anything else.
- **No mutation of `ProbeResult` outside `PaginatedProbe`**: the post-hoc `res.status = "fail"` pattern lives only inside `PaginatedProbe.run()` because its sub-probes need a two-phase verdict (data validity, then domain-specific check). Don't propagate it.
- **Throttling**: REST calls go through `fetch()` in `base.py`, which enforces `MIN_REST_INTERVAL_MS` per exchange. Don't bypass it.

<br>
<br>

## Interpreting failures

A FAIL in the report does not always mean an adapter regression. Rule out these before chasing a bug:

- **Upstream publish lag on analytics-style endpoints.** OKX `rubik/stat` (open interest, long/short ratio) and KuCoin OI history publish their buckets with a delay that fluctuates with upstream load. The `recent` sub-probe's freshness threshold defaults to `3 × period + 30s` (about 45 minutes for 15m); a slow window can push real lag above that. Re-run after 20 to 30 minutes. If the failure clears, it was transient upstream. If the same `(exchange, market_type, period)` keeps failing, add an entry to `config.py::FRESHNESS_OVERRIDE_MS`, the mechanism already in use for KuCoin INVERSE 1m.

- **Quiet-market WebSocket activity.** `trades`, `agg_trades`, and `liquidations` depend on real events. On low-volume contracts a 180-second window can legitimately produce few or zero frames. Liquidations have threshold 0 for this reason. Trades and agg_trades (threshold 1) WARN on quiet pairs; raising `WS_TEST_DURATION` reduces the chance, at the cost of wall time.

- **Identical staleness across unrelated routes.** When two routes that share a bucket cadence (for example `candles:15m` and `open_interest:15m`) report byte-identical age, the cause is upstream cadence, not adapter logic. The validator captures `now_ms` once at run start. Both probes measure against that snapshot, so an upstream that has not yet published its newest bucket leaks the same lag everywhere.

- **Real upstream 5xx exhaustion.** When `_make_request` exhausts retries and surfaces a 500, the upstream had a real outage during the run. The validator does not paper over it; the failure is honest but not actionable on the adapter side.

When in doubt: re-run with `--parallel 1 <exchange>` to isolate, and compare the FAIL `last_ts` value against the upstream's own public endpoint for that pair. If the upstream itself does not have the recent record, the validator is reporting honestly.

<br>
<br>

## Coverage limits

The validator is a route-and-shape verifier, not a full integration test. It does **not** test:

- Multiple symbols per market (one BTC/ETH-stable pair is picked per market).
- WebSocket reconnect or heartbeat-silence behaviour. (Validates frame shape and minimum count over a sustained window; forced-disconnect tests are out of scope as flaky.)
- Concurrent or rate-limited access beyond the configured semaphores.
- Authenticated endpoints (the router currently exposes only public routes).
- Malformed input handling beyond the negative-path triplet.
