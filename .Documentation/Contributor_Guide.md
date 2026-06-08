<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <a href="Scope.md">Scope</a> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href="Troubleshooting.md">Troubleshooting</a> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Capabilities_Contract.md">Capabilities Contract</a> &nbsp;•&nbsp; 
  <a href="Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <b>Contributor Guide</b>
</sub>

<br>
<br>
<br>
<br>

## Contributor Guide

The router is extended through isolated exchange adapters. Almost every contribution lives in `src/exchanges/<name>/` and leaves the routing core untouched. This guide walks through the work in roughly the order you will do it: set up a local loop, build the adapter, satisfy the contract, follow the code standards, and run the test suite before opening a PR.

### Record construction, the short version

Every record (Trade, Candle, MarkPrice, etc.) goes through a `build_*` helper in `src/exchanges/base.py`. Adapters pass in raw upstream values; the builders construct the nested value objects and compute USD where derivable. The wire-format spec is in [API Reference](API_Reference.md#response-shapes); the builder signatures are below.

An inverse trade, where USD is computed as `native × contract_size` and is price-independent:

```python
from src.exchanges.base import (
    build_qty_value, build_volume_value, build_oi_value,
    build_funding_current, build_funding_historical, build_funding_convention,
)

trade = Trade(
    symbol      = "BTCUSD",
    market_type = MarketType.INVERSE,
    quote       = "USD",
    price       = 50000.0,
    qty         = build_qty_value(native=5, qty_unit="contract", contract_size=100.0, price=50000.0),
    side        = "buy",
    timestamp   = ts,
)
```

All adapter sites that construct records pass `quote = info.quote_asset` at the row level. Look up `info = await self._info_for(market_type, model_symbol)` once at the start of each route handler / stream generator and reuse for every record in the response.

Cycle length for funding lives in an adapter-internal `_funding_interval_cache: Dict[MarketType, Dict[str, int]]` keyed by model symbol. SymbolInfo's `funding: Optional[FundingConvention]` block carries only the categorical `kind` (no `cycle_ms`); MarkPrice and FundingRate row constructors read the actual cycle_ms from the internal cache.

### The `_warm()` hook

`BaseExchange` exposes `async def _warm(self) -> None` (default: no-op) as the override point for adapter prebuilding. Override it to declare what should be warmed at startup. The base class owns everything else: it spawns the warm in a background task during `preload()`, times each step, logs a structured timeline, and contains failures so one adapter's warm cannot block another.

Inside `_warm()`, call `await self._step("label", awaitable)` once per logical warm. Each step's start, duration, and outcome are logged automatically. Steps run sequentially within an adapter (a chronological per-adapter timeline in the log); cross-adapter parallelism is preserved by the background-task wrapping.

```python
async def _warm(self) -> None:
    await self._step("spot_info",    self._ensure_info_cache(MarketType.SPOT))
    await self._step("linear_info",  self._ensure_info_cache(MarketType.LINEAR))
    await self._step("inverse_info", self._ensure_info_cache(MarketType.INVERSE))
```

If a warm step has its own bounded-concurrency fan-out (per-symbol lookups with a semaphore, for example), put that logic in a regular adapter method and pass the call to `_step`. The semaphore guard and per-symbol failure handling stay outside `_warm()`, so the orchestration line reads as one intent:

```python
async def _warm(self) -> None:
    await self._step("linear_funding", self._warm_funding_intervals(MarketType.LINEAR))


async def _warm_funding_intervals(self, market_type: MarketType) -> None:
    cache = await self._ensure_info_cache(market_type)
    sem   = asyncio.Semaphore(10)
    tasks = [self._warm_one_funding_interval(info, sem) for info in cache.values()]

    await asyncio.gather(*tasks, return_exceptions=True)


async def _warm_one_funding_interval(self, info: SymbolInfo, sem: asyncio.Semaphore) -> None:
    async with sem:
        try:
            await self._funding_interval_ms_for(info.native_symbol)
        except Exception as e:
            logger.warning(f"funding interval lookup failed for {info.native_symbol}: {e}")
```

By convention, `_warm()` (and any `_warm_*` helpers it calls) sits right after `shutdown()` in the adapter file; every existing adapter follows this placement. The `_ensure_info_cache` / `_ensure_*_map` methods referenced in the examples are adapter-internal lazy caches that already exist in every adapter; listing them in `_warm` simply forces eager warming at startup.

Do not override `preload()` directly. The base class seals it; the override point is `_warm()`. If your adapter does not need prebuilding (one bulk endpoint covers all metadata), simply don't override `_warm`.

<br>
<br>

## Local Development

The service ships as a Docker container, but iterating on an adapter through `docker-compose up --build` on every change is slow. For day-to-day work, run the service directly. Create the venv once, then activate it, install dependencies, and launch with autoreload:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn src.main:app --reload --port 8040
```

On Windows, activate the venv with `.venv\Scripts\activate`. The `--reload` flag restarts the worker whenever a file under `src/` changes. Pair it with the test suite running in another terminal for a tight edit-test loop.

### Dependencies

The repo has a single `requirements.txt` covering everything: the running service (FastAPI, uvicorn, httpx, websockets, orjson) and the local tools (rich, used by the auditor). The Dockerfile installs this file as-is, so the same dependency set is present in the production image and in a contributor's venv. There is no separate dev dependency file.

**Adding a new dependency:** put it in `requirements.txt`. If it is genuinely contributor-only (a profiler, a linter, a heavy debugging library), keep it out of the requirements file and document the install command in this guide instead, so the production image stays lean.

When something breaks, it helps to bypass FastAPI entirely. Instantiate the adapter in a Python REPL and call its methods directly. The stack trace is cleaner, and you can inspect intermediate state without routing a request end to end.

When iterating without `--reload`, set `--port` on the uvicorn command line directly; the `EXCHANGE_ROUTER_SERVICE_PORT` env var is only consumed by `docker-compose.yml` and is not read by the Python service.

<br>
<br>

## Adapter Implementation

All adapters must inherit from `BaseExchange` in `src/exchanges/base.py`. The following checklist covers everything a compliant adapter needs to satisfy before it can be merged:

- [ ] **Capability Mapping:** Implement `get_capabilities()` returning the full list of supported REST routes and WebSocket channels.
- [ ] **Data Normalization:** Map all raw upstream JSON payloads to the Pydantic models in `src/models.py`.
- [ ] **Market Routing:** Handle `spot`, `linear`, and `inverse` market types, including any subdomain or parameter differences between them.
- [ ] **Symbol Normalization:** Implement `get_model_symbol(api_symbol, market_type)` to translate raw exchange symbols to normalized form (e.g. `BTCUSD_PERP` → `BTCUSDT`), and `get_api_symbol(symbol, market_type)` to reverse the translation when constructing upstream requests. Normalized symbols must be bare pairs with no suffixes.
- [ ] **Perpetuals Filter:** For `linear` and `inverse` markets, `_fetch_exchange_info` must exclude dated and quarterly contracts. Only perpetual instruments should appear in `/markets` and `/markets/{symbol}`.
- [ ] **`native_symbol` Field:** Populate `native_symbol` on every `SymbolInfo` object with the raw exchange symbol before normalization.
- [ ] **`funding` Field:** Set `SymbolInfo.funding = None` on spot, `build_funding_convention("discrete")` on discrete-funding perps, and `build_funding_convention("continuous")` on continuous-funding perps.
- [ ] **`build_*` constructors:** Every record-construction site (Trade, AggTrade, Liquidation, BookTicker, Ticker, Candle, MarkPrice, OpenInterest, FundingRate) must go through the matching `base.build_*` helper. Do not construct nested value objects (`QtyValue`, `VolumeValue`, `OiValue`, `FundingCurrent`, `FundingHistorical`) by hand.
- [ ] **`quote` field at row level:** Every record that carries quote-currency values reads `quote = info.quote_asset` from the adapter's SymbolInfo cache and passes it at the row level. Look up once per route handler / WS stream and reuse.
- [ ] **Internal `_funding_interval_cache`:** Discrete-funding adapters maintain a per-symbol `cycle_ms` cache. Shape varies: Binance, Bybit, KuCoin use `Dict[MarketType, Dict[str, int]]` keyed by model symbol; OKX uses `Dict[str, int]` keyed by upstream `inst_id`. Populate it during `_fetch_exchange_info` (or as a warm step in `_warm()` for adapters that need per-symbol upstream calls); read it from MarkPrice and FundingRate constructors. SymbolInfo's `funding` block carries only the categorical `kind`, never `cycle_ms`.
- [ ] **`_warm()` (optional):** Override when the adapter benefits from prebuilding caches at startup (info caches, symbol maps, per-symbol metadata fetches). Inside `_warm()`, call `await self._step("label", awaitable)` once per logical step; the base class handles background-task wrapping, timing, and structured logging. Adapters with one bulk endpoint that covers all metadata don't need to override.
- [ ] **Backward-anchored pagination:** Every method that takes `start_time` must treat it as an inclusive backward-walking upper bound. Route the call through `_paginate_backwards` (or equivalent) seeded with `start_time` on the first iteration, so the result contains records with `ts <= start_time`. This is the universal contract; do not introduce forward-walking from `start_time`.
- [ ] **Registry Registration:** Add an `__init__.py` that imports the adapter class (e.g., `from .adapter import KrakenAdapter`). The auto-loader relies on this import to discover subclasses of `BaseExchange`.

<br>
<br>

## Capabilities Contract

Every adapter declares its supported routes through `get_capabilities()`. This is the single source of truth: route methods read from it, the auditor reads from it, external clients read it. The full schema (field-by-field meanings, the `paginated`/`max_limit`/`retention_ms` interaction, where to source each value from, the unsupported-route convention) lives in its own document.

See [Capabilities Contract](Capabilities_Contract.md). When adding a new adapter, copy `_build_capabilities` from an existing adapter under `src/exchanges/` as a starting point rather than reconstructing the schema by hand.

<br>
<br>

## Data Normalization

All data returned by an adapter must go through the Pydantic models in `src/models.py` before reaching the routing layer. Raw dictionaries are not accepted.

- **Validation:** Unmapped or malformed fields cause an immediate validation error.
- **Schema stability:** The router and downstream clients depend on these models being consistent across adapters.
- **Type safety:** This keeps price, volume, and quantity precision consistent between exchanges.

Before integrating with the router, validate normalization locally by instantiating your adapter directly in a Python REPL and confirming that all network responses parse through the relevant models without errors.

### When the Models Don't Fit

If an upstream response carries a field that no existing model captures, you have three options, in order of preference:

1. **Drop the field.** Most of the time the extra data is not relevant to the router's contract. If it is noise, leave it out of the normalized output.
2. **Add an optional field to an existing model.** If the field is conceptually shared across exchanges and just happens to be missing from yours, extend the model with an `Optional[...]` field and default it to `None`. Other adapters can start populating it later without breaking the schema.
3. **Add a new model.** If the data type is genuinely new (new market type, new derivative), propose a new Pydantic model in `src/models.py` as part of the PR.

Do not add adapter-specific fields under a generic name, and do not return raw dicts as an escape hatch. The normalization contract is the whole point of the router.

<br>
<br>

## Code Standards

* **Async I/O.** All network calls must use `httpx` or `websockets` in an async context. A blocking call inside an adapter stalls the entire event loop.
* **No raw dicts across the boundary.** Adapter methods must return Pydantic models from `src/models.py`. If you find yourself wanting to return a `dict`, add a model instead.
* **Fail with `ValueError` or `NotImplementedError`.** These are the exception types the route layer knows how to translate into HTTP responses (4xx and 501 respectively). Use `ValueError` for bad input and upstream validation failures, `NotImplementedError` for features the adapter does not support on a given market type.
* **No hand-rolled retry logic at the call site.** `_make_request` (or the adapter's equivalent) handles retries and backoff. Per-call retry loops fight the rate limiter.
* **Logging via `logging.getLogger("<adapter>_adapter")`.** Keep each adapter's logs isolated so they can be filtered independently.

For exchange-specific behaviors (rate limit tiers, symbol translation quirks, API version notes) see [Exchange Notes](Exchange_Notes.md). For internal mechanics that adapter authors need but users do not, see [Adapter Internals](#adapter-internals) below.

<br>
<br>

## Testing

Adapter compliance is validated via the auditor package at `tools/auditor/` (see [Auditor Guide](Auditor_Guide.md)). The runner reads `/{exchange}/capabilities` and runs only the probes that apply to the features the adapter claims to support, so an honest capabilities map is the difference between a clean test run and noise. All declared endpoints must pass before submitting a Pull Request.

See [Auditor Guide](Auditor_Guide.md) for the full probe catalogue, env knobs, concurrency model, run output, and CLI examples.

<br>
<br>

## Service Lifecycle

For reference, the router uses a FastAPI lifespan manager (`@asynccontextmanager`) to handle startup and shutdown. On startup, the auto-loader walks `src/exchanges/`, instantiates every `BaseExchange` subclass it finds, and registers it in `EXCHANGE_REGISTRY`. On shutdown, the manager closes all active WebSocket tasks cleanly and calls `shutdown()` on each adapter to release connection pools and any other resources the adapter holds.

Contributors rarely need to touch this layer. The adapter owes the lifecycle a correct `shutdown()` that closes every client it opened in `__init__`, and optionally a `_warm()` (see [The `_warm()` hook](#the-_warm-hook) above) if startup prebuilding is needed.

Once `shutdown()` is correct and `python -m tools.auditor` passes cleanly, the adapter is ready for review.

<br>
<br>

## Adapter Internals

These are conventions adapter authors follow that are not visible from the user-facing schema. They live here, not in Exchange Notes, because callers do not need them.

### `_paginate_backwards` helper

Every method that accepts `start_time` should route through `_paginate_backwards` (or its equivalent in the adapter), seeded with `start_time` on the first iteration. The helper collects forward-sorted batches, walks the response back by setting the upstream end-anchored parameter to one less than the oldest record's timestamp, and stops when no new records arrive or `max_requests` is reached. The user-facing contract is "inclusive backward-walking upper bound"; this is how that contract is implemented.

### Upstream pagination parameters

| Exchange | Native param | Inclusive? | Adapter handling |
| :--- | :--- | :--- | :--- |
| Binance | `endTime` | yes | direct passthrough |
| Bybit | `endTime` (`end` on klines) | yes | direct passthrough |
| KuCoin | `endAt` (spot, seconds), `to` (futures klines, ms), `to` (funding `from`/`to` ms), `endAt` (OI, ms) | exclusive on the spot/futures endpoints | send `start + 1`; for spot klines convert ms to seconds |
| OKX | `after` (REST), `end` (rubik) | `after` is exclusive, `end` is inclusive | for `after`, send `start + 1` to make it inclusive |
| Kraken | `since`, `from` | forward-only | compute `synthetic_since = start - limit * interval`, fetch forward, truncate to `ts <= start`, tail to `limit` |

### Perpetuals filter

| Exchange | Mechanism |
| :--- | :--- |
| Binance | filter `contractType == "PERPETUAL"` on `/dapi/v1/exchangeInfo` and `/fapi/v1/exchangeInfo` |
| Bybit | pass `contractType=LinearPerpetual` or `InversePerpetual`; also re-check on the response |
| Kraken | check `type` plus instrument prefix: `PI_*` and `PF_*` are perpetuals, `FI_*` and `FF_*` are dated |
| KuCoin | `/api/v1/contracts/active` returns only perpetuals; split linear vs inverse by `isInverse` and filter `status == "Open"` |
| OKX | filter `ctType == "linear"` or `"inverse"` plus `state == "live"` on `instType=SWAP` |

### Symbol round-trip

Adapters implement `get_api_symbol(symbol, market_type)` and `get_model_symbol(api_symbol, market_type)`. The two must compose: `get_model_symbol(get_api_symbol(s, m), m) == s` for every supported `(s, m)`. `get_api_symbol` reattaches whatever suffix or separator the upstream needs (Binance `_PERP`, Kraken `PI_`/`PF_`, OKX `-` and `-SWAP`).

### Spot symbol cache (`_resolve_symbol`)

For exchanges whose REST API expects a separator-bearing native id (KuCoin `BTC-USDT`, OKX `BTC-USDT`, Kraken `XBT/USD`), splitting a normalized flat symbol like `BTCUSDT` into base/quote requires knowing which suffixes are valid quote currencies. A hardcoded list (`SPOT_QUOTES`) goes stale as exchanges add new stablecoins. Use the upstream's own symbol metadata instead.

The pattern (mirrors `KrakenAdapter._ensure_spot_ws_map` and `KuCoinAdapter._ensure_spot_symbol_map`):

```python
self._spot_symbol_map: Dict[str, str] = {}     # "BTCUSDT" -> "BTC-USDT"
self._spot_symbol_map_lock = asyncio.Lock()

async def _ensure_spot_symbol_map(self) -> None:
    if self._spot_symbol_map:
        return
    async with self._spot_symbol_map_lock:
        if self._spot_symbol_map:
            return
        data = await self._make_request(...)
        built: Dict[str, str] = {}
        for s in data or []:
            if not active(s):
                continue
            built[f"{s['base']}{s['quote']}".upper()] = s["native_id"]
        self._spot_symbol_map = built

async def _resolve_symbol(self, symbol: str, market_type: MarketType) -> str:
    if market_type != MarketType.SPOT:
        return self.get_api_symbol(symbol, market_type)
    flat = symbol.upper().replace("/", "").replace("-", "")
    try:
        await self._ensure_spot_symbol_map()
    except Exception:
        return self.get_api_symbol(symbol, market_type)
    native = self._spot_symbol_map.get(flat)
    if native:
        return native
    raise ValueError(f"Symbol {symbol} not listed on <exchange> spot")
```

Route methods call `await self._resolve_symbol(symbol, market_type)` instead of the sync `self.get_api_symbol(...)`. The cache is lazy-loaded on first spot use, double-checked-locked for concurrency, and survives a fetch failure by falling back to the static heuristic.

When the cache loads successfully and the requested symbol is missing, raise `ValueError` (which the router maps to HTTP 400). Some exchanges return a stub object for unknown symbols rather than a clean error, so trusting upstream is unsafe. The cache is the source of truth. Newly listed symbols become resolvable on the next service restart.

`get_api_symbol` and `SPOT_QUOTES` are the fallback path for the rare case where the live `/symbols` fetch fails, not the primary mechanism.

KuCoin uses `/api/v1/symbols` (fields: `symbol`, `baseCurrency`, `quoteCurrency`, `enableTrading`). OKX uses `/api/v5/public/instruments?instType=SPOT` (fields: `instId`, `baseCcy`, `quoteCcy`, `state == "live"`). Kraken's WS-name cache reads `/AssetPairs` for a different reason, its REST API accepts altname directly.

### Rate limit headers and proactive backoff

Implementation patterns live here; user-facing behaviour lives in [Exchange Notes](Exchange_Notes.md#rate-limit-and-ban-protection); the state machine lives in [System Architecture](System_Architecture.md#rate-limiting).

When adding a new adapter, read the upstream's rate-limit policy and pick the right pattern; do not assume every exchange looks like Binance's weight model. The validator's `MIN_REST_INTERVAL_MS` knob is independent and only protects the test suite from itself.

Per-exchange implementation specifics:

| Exchange | Header(s) | Trigger |
| :--- | :--- | :--- |
| Binance | `x-mbx-used-weight-1m` | back off 2s if used weight > 1150/1200 |
| Bybit | `X-Bapi-Limit-Status`, `X-Bapi-Limit-Reset-Timestamp` | back off until reset if remaining < 10 |
| KuCoin | none (30s rolling window, no per-response remaining-weight header) | reactive only on HTTP 429 |
| OKX | none | reactive only on HTTP 429 |
| Kraken | none | reactive on HTTP 429/418/502/503/504/520, body-level `EAPI:Rate limit exceeded` and `EService:Throttled: <ts>` |

Reactive backoffs (HTTP 429, soft codes like Bybit's `retCode: 10006`, Kraken's body-level errors) all funnel into the same `_backoff_until` mechanism.

### Kraken-specific retry behavior

Kraken's documented rate-limit semantics differ enough from the other exchanges that the adapter does more than simple `_backoff_until` scheduling. The implementation in [src/exchanges/kraken/adapter.py](../src/exchanges/kraken/adapter.py) `_make_request` enforces both proactive spacing (so we rarely trigger Kraken's limits in the first place) and a sophisticated retry strategy when we do.

**Proactive spacing (always on, even on the happy path):**

- **`_base_interval_ms = 200ms`** between any two Kraken calls globally. Cheap insurance against simultaneous bursts when concurrent probes happen to fire at the same instant.
- **`_pair_interval_ms = 1100ms`** between successive calls to `/0/public/OHLC` or `/0/public/Trades` for the same `pair`. Kraken docs gate these specifically per IP per pair with "1 per second or less" guidance; 1.1s gives 10% margin under that cap.

**Retry strategy when something does go wrong:**

1. **Body-level signals**, not just HTTP status. Kraken sometimes returns HTTP 200 with `error: ["EAPI:Rate limit exceeded"]` or `error: ["EService:Throttled: <unix_ts>"]` in the JSON body. The adapter parses these and treats them as rate-limit signals, sleeping until the absolute timestamp when given.
2. **Exponential backoff with full jitter**. Up to 8 retries on Spot REST (5 on Futures) with delays of `2^attempt` seconds capped at 60s, multiplied by `0.5 + random()`. This avoids consecutive retries landing inside the same cooldown window, which Kraken explicitly documents as making the cooldown last longer. Worst-case total backoff is ~4 minutes for the most stubborn cooldowns; most signals clear in ~10-30s.
3. **Global forward-rate slowdown after rate-limit confirmation**. When HTTP 429/418 or a body-level rate-limit signal fires, `_extra_interval_ms` is set to 1000ms on top of the base 200ms (so 1.2s spacing) for the next 60s. All subsequent calls (not just the failing one) honor this spacing. Per Kraken's docs: "additional calls would be restricted for a few seconds (or possibly longer if calls continue to be made while the rate limits are active)."

When all retries are exhausted, `_make_request` raises `ValueError(f"Kraken throttled or unavailable: {url} (last_status=..., last_body_error=...)")`. The router translates ValueError to HTTP 400 with the message in the body, so callers see actionable detail instead of opaque 500.

If a future contributor finds these knobs incorrect for a specific Kraken behavior, the doc reference is `https://docs.kraken.com/api/docs/guides/spot-rest-ratelimits` and `https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-`.

### WebSocket multiplexing via `StreamHub`

Every adapter's `_ws_connect` is implemented on top of `StreamHub` (in [src/exchanges/base.py](../src/exchanges/base.py)). Each hub owns exactly **one** persistent upstream WebSocket and fans incoming messages out to per-topic queues. Multiple subscribers acquire a queue per topic via `hub.subscribe(topic)` and release it via `hub.unsubscribe(topic, q)`. The hub sends `SUBSCRIBE` upstream when a topic's refcount transitions from 0 to 1 and `UNSUBSCRIBE` when it drops back to 0; on reconnect it re-subscribes every topic that still has subscribers.

This is the difference between "1000 subscribed symbols open 1000 sockets to Binance" (banned in minutes under Binance's 300 conn / IP / 5 min cap) and "1000 subscribed symbols share one socket carrying 1000 SUBSCRIBE messages".

A `StreamHub` is a tiny adapter-agnostic shell driven by five callbacks:

| Callback | Returns | Purpose |
| :--- | :--- | :--- |
| `connect()` | connected websocket | Open the upstream WS. Adapter handles bullet tokens, welcome frames, etc. inside this coroutine. |
| `subscribe_payload(topics)` | iterable of payloads | One or more JSON dicts (or raw strings) to send to subscribe to the listed topics. Adapter decides whether to batch or send one-per-topic. |
| `unsubscribe_payload(topics)` | iterable of payloads | Same shape, for unsubscribe. |
| `route(msg)` | topic str or `None` | Given a parsed inbound message, return the topic it belongs to. Returning `None` drops the message (welcome frames, pongs, subscribe acks). |
| `keepalive_payload` (optional) | static payload | Sent every `keepalive_interval` seconds. Use for OKX's raw `"ping"`, Bybit's `{"op": "ping"}`, KuCoin's `{"id": ..., "type": "ping"}`. Set to `None` if the `websockets` library's own `ping_interval` handles it (Binance, Kraken). |

The adapter owns a small `_hubs` dict keyed by whatever distinguishes upstream connections for that exchange (typically `MarketType`, sometimes `(MarketType, bucket)` for Binance USDM, sometimes a synthetic key for Kraken spot's per-payload-shape hubs). `_ws_connect` looks up or lazily creates the right hub, calls `subscribe`, yields from the queue, and unsubscribes in `finally`. `shutdown()` calls `await hub.close()` on every hub it created.

When adding a new exchange, the work is: identify the upstream's subscribe / unsubscribe / keepalive shape, write the four (or five) callbacks, and let the hub do the rest.

### Subscription payload quirks

Documented here so future adapter authors know what to look for when wrapping a new exchange.

- **Binance.** Standard JSON `{"method": "SUBSCRIBE", "params": [...], "id": ...}`. Connect to the **combined-stream** URL (`/stream`, not `/ws`); responses arrive wrapped as `{"stream": "...", "data": {...}}` so routing is just `msg["stream"]`. No client keepalive needed; the `websockets` library's `ping_interval=20` handles it. USDM still needs the per-bucket URL routing (see "Binance USD-M WebSocket routing buckets" below): one hub per `(market_type, bucket)`.
- **Bybit.** Standard JSON `{"op": "subscribe", "args": [...], "req_id": "sub-<ms>"}` (the `req_id` is opaque, used by Bybit to correlate responses; the adapter generates a fresh one per subscribe call). Routing field is `topic`. Send `{"op": "ping"}` every 20 seconds via the hub's `keepalive_payload`.
- **Kraken spot.** Use the v2 WebSocket protocol on `wss://ws.kraken.com/v2`. Translate `XBT`/`XDG` to `BTC`/`DOGE` when constructing subscription payloads. The `ticker` channel needs `event_trigger: "bbo"` to emit BBO updates without trade activity. The `trade` channel needs `"snapshot": true` to emit recent trades on connect. Because incoming messages don't echo `event_trigger`/`depth`/`snapshot`, two subscriptions that differ only in those flags are indistinguishable on the receive side; the adapter therefore creates a **separate hub per subscription shape** (one for ticker, one for ticker-bbo, one for book-depth-N, etc.) keyed off the params dict in `_hub_key`.
- **Kraken futures.** Use the v1 WebSocket protocol on a separate URL (`wss://futures.kraken.com/ws/v1`); message shapes differ from v2 spot. Routing is `f"{feed}:{product_id}"`. Multiple route methods can share a single subscription on the same `feed` (futures `ticker` carries last/bid/ask/markPrice/indexPrice/fundingRate all at once, so `stream_ticker`, `stream_book_ticker`, and `stream_mark_price` all subscribe to the same `ticker:<product>` topic; the hub fans the same message into each consumer's queue).
- **KuCoin.** Two-step handshake. `POST /api/v1/bullet-public` against the spot or futures host returns `{token, instanceServers: [{endpoint, pingInterval, ...}]}`. The hub's `connect()` performs the bullet fetch, builds `endpoint?token=...&connectId=<uuid>`, opens the WS, and consumes the welcome frame before returning. Subscribe payload: `{"id": ..., "type": "subscribe", "topic": "/market/ticker:BTC-USDT", "privateChannel": false, "response": false}` (one per topic; KuCoin doesn't batch multiple topics in one subscribe). Routing is `msg["topic"]`. Heartbeat: `{"id": ..., "type": "ping"}` every ~15 seconds. Spot and futures use different bullet endpoints and topic prefixes (`/market/`, `/spotMarket/` vs `/contractMarket/`, `/contract/`).
- **OKX.** OKX closes idle connections after 30 seconds. Send a raw `"ping"` (not JSON) every 25 seconds; the server replies with raw `"pong"`. OKX subscribe args are dicts, not strings, so the adapter keeps a `_topic_args: Dict[str, dict]` mapping the synthetic topic key back to the wire-format arg the hub callbacks reconstruct. The key is `f"{channel}:{instId or instType or ''}"`: instance-scoped channels (`tickers`, `trades`, etc.) key on `instId`; the global `liquidation-orders` channel keys on `instType` (`"SWAP"`) since the wire arg carries no `instId`.

### Internal endpoints behind `open_interest` and `long_short_ratio`

For adapter authors hitting these surfaces:

- **OKX OI** comes from `/api/v5/rubik/stat/contracts/open-interest-history`. The history endpoint returns `[ts, oi (contracts), oiCcy (coin units), oiUsd (USD notional)]`. The adapter picks different columns per market type: linear (`BTC-USDT-SWAP`) reads `oiCcy` (coin units) and emits `unit: "base"`; inverse (`BTC-USD-SWAP`) reads `oi` (contracts) and emits `unit: "contract"` with `contract_size` from upstream `ctVal`. The `oiUsd` column is dropped because the model does not carry a USD slot (most exchanges don't expose one; see Exchange Notes for the per-exchange unit table).
- **OKX L/S** comes from `/api/v5/rubik/stat/contracts/long-short-account-ratio`, parameterized by `ccy` (currency), not `instId`.
- **Kraken OI and L/S** come from the Kraken Futures chart analytics API (`https://futures.kraken.com/api/charts/v1/analytics/{symbol}/{type}`). The adapter uses the close value of each OHLC-style bucket as the representative.
- **KuCoin OI** comes from the unified analytics endpoint `https://api.kucoin.com/api/ua/v1/market/open-interest`, parameterized by `symbol` (futures form) and `interval` (`5min`, `15min`, `30min`, `1hour`, `4hour`, `1day`). The endpoint lives on the spot host even though it serves futures data. Response rows are `{ts, openInterest}`. KuCoin does not expose long/short ratio publicly.

### Binance USD-M WebSocket routing buckets

USD-M futures (`wss://fstream.binance.com`) requires per-stream routing into three sub-endpoints: `/public`, `/market`, `/private`. A subscription on the wrong bucket connects but silently drops frames, so the adapter selects the correct bucket per topic before opening the connection.

| Stream | Bucket |
|--------|--------|
| `<symbol>@trade` | public |
| `<symbol>@bookTicker` | public |
| `<symbol>@depth<level>@<speed>` | public |
| `<symbol>@ticker` | market |
| `<symbol>@miniTicker` | market |
| `<symbol>@markPrice` (and `@1s` variant) | market |
| `<symbol>@aggTrade` | market |
| `<symbol>@forceOrder` | market |

Routing is handled by `BinanceAdapter._usdm_bucket_for_topic`. Adding a new USD-M stream means matching an existing entry (`depth*` normalises to `depth`) or adding a key to `_USDM_BUCKETS`. Unknown channels default to `public`. COIN-M (INVERSE, `wss://dstream.binance.com`) and SPOT (`wss://stream.binance.com:9443`) do not use bucketed routing.

### KuCoin: two REST domains

KuCoin splits its public REST surface across two hosts: `api.kucoin.com` for spot and the unified analytics endpoints (open interest history under `/api/ua/v1/`), and `api-futures.kucoin.com` for everything contract-related (contracts list, ticker, orderbook, trades, klines, mark price, funding history). The router routes per call. There is no caller-visible effect.

### KuCoin: timestamp unit normalisation

KuCoin mixes timestamp units across endpoints. Spot trade history (`/api/v1/market/histories`) and futures ticker, orderbook, trade history, and execution streams return timestamps in **nanoseconds**. Spot stats, futures klines, mark price, funding history, and open interest history return **milliseconds**. Spot kline rows lead with **seconds**. The adapter normalises all of these to milliseconds before they reach the model, but contributors debugging raw upstream payloads will see the source units.

### OKX: inverse base/quote derivation

For SWAP instruments (linear and inverse), OKX returns empty `baseCcy`/`quoteCcy` on the instruments endpoint. The adapter derives `base_asset` and `quote_asset` from the upstream `uly` field. If `uly` is missing, both fields fall back to empty strings.
