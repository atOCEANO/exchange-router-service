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
  <a href="Validator_Guide.md">Validator Guide</a> &nbsp;•&nbsp; 
  <b>Contributor Guide</b>
</sub>

<br>
<br>
<br>
<br>

## Contributor Guide

The router is extended through isolated exchange adapters. Almost every contribution lives in `src/exchanges/<name>/` and leaves the routing core untouched. This guide walks through the work in roughly the order you will actually do it: set up a local loop, build the adapter, satisfy the contract, follow the code standards, and run the test suite before opening a PR.

<br>
<br>

## Local Development

The service ships as a Docker container, but iterating on an adapter through `docker-compose up --build` on every change is slow. For day-to-day work, run the service directly:

```bash
# Create and activate a venv (once)
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the service with autoreload
uvicorn src.main:app --reload --port 8040
```

The `--reload` flag restarts the worker whenever a file under `src/` changes. Pair it with the test suite running in another terminal for a tight edit-test loop.

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
- [ ] **Perpetuals Filter:** For `linear` and `inverse` markets, `get_exchange_info` must exclude dated and quarterly contracts. Only perpetual instruments should appear in `/markets` and `/info`.
- [ ] **`native_symbol` Field:** Populate `native_symbol` on every `SymbolInfo` object with the raw exchange symbol before normalization.
- [ ] **Backward-anchored pagination:** Every method that takes `start_time` must treat it as an inclusive backward-walking upper bound. Route the call through `_paginate_backwards` (or equivalent) seeded with `start_time` on the first iteration, so the result contains records with `ts <= start_time`. This is the universal contract; do not introduce forward-walking from `start_time`.
- [ ] **Registry Registration:** Add an `__init__.py` that imports the adapter class (e.g., `from .adapter import KrakenAdapter`). The auto-loader relies on this import to discover subclasses of `BaseExchange`.

<br>
<br>

## Capabilities Contract

Every adapter must implement `get_capabilities()`, which returns a dict describing what the adapter supports for each route. The router exposes this at `GET /{exchange}/capabilities`. Clients call it to know what they can ask for. The verification harness reads it to calibrate every probe (which depths to test, what limits the router serves, whether the route is paginated, what retention window applies). The capabilities dict is the single source of truth for what each route supports: route methods read from it, the verify harness reads from it, external clients read it.

### Per-route-type uniform schema

Each route type has its own fixed schema. **Within a route type, every adapter declares the same fields, in the same order, with `null` for values that don't apply.** Different route types have different field sets (`ticker` doesn't declare `max_depth`, `orderbook` doesn't declare `intervals`), but every exchange's `ticker` looks the same shape and every exchange's `orderbook` looks the same shape.

There are no factored-out variables inside `_build_capabilities`. Every market block is written out fully at its point of use. Yes, this duplicates the candle interval list across `SPOT`/`LINEAR`/`INVERSE`. That's the deliberate trade-off for self-contained, scan-readable market blocks.

```python
def _build_capabilities(self) -> Dict[str, Any]:
    return {
        "name": self.name,
        "markets": {
            MarketType.SPOT: {
                "ticker": {
                    "rest": True,
                    "ws":   True,
                },
                "book_ticker": {
                    "rest": True,
                    "ws":   True,
                },
                "orderbook": {
                    "rest":      True,
                    "ws":        True,
                    "depths":    [5, 10, 20, 50, 100, 500, 1000],
                    "max_depth": 1000,
                },
                "trades": {
                    "rest":      True,
                    "ws":        True,
                    "max_limit": 1000,
                },
                "agg_trades": {
                    "rest":         True,
                    "ws":           True,
                    "paginated":    True,
                    "max_limit":    None,
                    "retention_ms": None,
                },
                "candles": {
                    "rest":         True,
                    "ws":           False,
                    "paginated":    True,
                    "max_limit":    None,
                    "retention_ms": None,
                    "intervals":    ["1m", "5m", "15m", ...],
                },
                "mark_price": {
                    "rest": False,
                    "ws":   False,
                },
                "funding_rate": {
                    "rest":         False,
                    "ws":           False,
                    "paginated":    False,
                    "max_limit":    None,
                    "retention_ms": None,
                },
                "open_interest": {
                    "rest":         False,
                    "ws":           False,
                    "paginated":    False,
                    "max_limit":    None,
                    "retention_ms": None,
                    "intervals":    None,
                },
                "liquidations": {
                    "rest":         False,
                    "ws":           False,
                    "paginated":    False,
                    "max_limit":    None,
                    "retention_ms": None,
                },
                "long_short_ratio": {
                    "rest":         False,
                    "ws":           False,
                    "paginated":    False,
                    "max_limit":    None,
                    "retention_ms": None,
                    "intervals":    None,
                },
            },
            MarketType.LINEAR: { ... },   # full block, no factored variables
            MarketType.INVERSE: { ... },  # full block, no factored variables
        },
    }
```

### Per-route-type schemas

Each route type defines its own field set. Declare every field for every adapter, even when the route is unsupported on this exchange (use `null` for inapplicable values).

| Route type | Fields |
| :--- | :--- |
| `ticker`, `book_ticker`, `mark_price` (snapshots) | `rest`, `ws` |
| `orderbook` | `rest`, `ws`, `depths`, `max_depth` |
| `trades` | `rest`, `ws`, `max_limit` |
| `agg_trades` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms` |
| `candles` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms`, `intervals` |
| `funding_rate` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms` |
| `open_interest` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms`, `intervals` |
| `liquidations` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms` |
| `long_short_ratio` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms`, `intervals` |

### Field reference

| Field | Type | Meaning |
| :--- | :--- | :--- |
| `rest` | bool | REST endpoint is exposed by the router. `False` means the route returns 501. |
| `ws` | bool | WebSocket stream is exposed by the router. |
| `paginated` | bool | Caller can pass a time anchor (`start_time=`); router walks back through history. `False` means the route returns whatever the upstream's most-recent page offers and no historical anchor is honoured (e.g. Bybit `/v5/market/account-ratio`). |
| `max_limit` | int \| null | The upstream's hard ceiling for routes that **cannot** paginate (`paginated: False`). The caller cannot get more than this from the route; the adapter clamps `limit=` to it. `null` for paginated routes (the adapter walks back through history; no router-side cap), for snapshots that take no `limit=` parameter (ticker, mark_price), and for unsupported routes. Per-page upstream caps used inside the adapter's pagination loop are inline literals; they are network mechanics, not part of the contract. |
| `retention_ms` | int \| null | Upstream's documented historical window in milliseconds. `null` means "asset-bounded" (data goes back as far as the asset has existed, like candles). Always written as a multiplication expression for readability: `30 * 24 * 60 * 60 * 1000` for 30 days, `7 * 24 * 60 * 60 * 1000` for 7 days. Python folds the constant at parse time. |
| `intervals` | list[str] \| null | Period strings the route accepts. `null` only on routes that have no period parameter (e.g. funding rate, where the cadence is upstream-dictated). |
| `depths` | list[int] \| null | Discrete depth buckets the upstream supports (e.g. Binance `[5, 10, 20, 50, 100, 500, 1000]`). `null` means continuous range, any integer up to `max_depth` is accepted. |
| `max_depth` | int \| null | Depth ceiling for orderbook. `null` only when the orderbook route is unsupported on this market. |

### How `paginated`, `max_limit`, and `retention_ms` interact

Three meaningful semantic states for routes that serve historical data:

| `paginated` | `max_limit` | meaning |
| :--- | :--- | :--- |
| `False` | `null` | Snapshot route (ticker, mark_price). No `limit=` parameter. |
| `False` | int | Single-page route (Bybit L/S, OKX trades). Caller gets the most recent N; no further history reachable. The adapter clamps any `limit=` the caller passes to this ceiling. |
| `True` | `null` | Paginated route (candles, OI, funding). Caller can pass a time anchor; the adapter walks back through history, paginating internally as far as upstream serves data. **No router-side ceiling.** Effective ceiling is upstream retention (`retention_ms`), the asset's age, or, defensively, the adapter's `_paginate_backwards.max_requests = 100` safety cap (~100 × per-page-cap records). |

The harness uses `min(big_limit, retention_ms / period_ms, market_age_ms / period_ms)` to compute the realistic ceiling on the recent-summary probe so a 1-week candle ask for 3000 records does not WARN; the ceiling is whichever is smallest of `big_limit`, `retention_ms // period_ms`, and the crypto-market age (since 2010-01-01).

### The 100-page safety cap (`max_requests`)

Each adapter's `_paginate_backwards` (and Kraken's custom paginator) is bounded by an internal `max_requests = 100` page count. Natural termination ("no more new data") handles every correctly-paginating case; the page cap only fires for genuinely pathological asks (e.g. `?limit=1_000_000` on 1m candles, which would need ~120,000 pages). When the cap fires before the requested `limit` is reached, the adapter logs a `WARNING` and returns whatever it managed to collect. This is observability, not a behavior contract; consumers should not rely on the 100-page cap as a guaranteed behavior.

If you want a different cap (e.g. for a heavy-historical use case), fork `_paginate_backwards` and bump the constant. We did not surface this as a capability field because it's defense-in-depth, not a contract.

### Where to source values from

- **`depths`**: orderbook docs almost always list supported depths. Binance: "Valid limits: 5, 10, 20, 50, 100, 500, 1000". KuCoin: two endpoints, `/level2_20` and `/level2_100`. Bybit: spot `[50, 200]`, futures `[50, 200, 500]`. Continuous-range exchanges (Kraken, OKX) declare `depths: None` and just declare `max_depth`.
- **`max_limit`**: the upstream's hard ceiling for non-paginated routes only (OKX `/trades`: 100, Bybit `/recent-trade`: 60 spot / 1000 futures, Bybit `/account-ratio`: 500). For paginated routes set `null`. The adapter walks back through history with no router-side cap, bounded only by retention or the asset's age. Per-page upstream caps that the pagination loop uses internally (e.g. Binance candles 1500/req, OKX 300/req) are inline literals in the route method, not declared in the capability map.
- **`retention_ms`**: read upstream docs for the route. Binance OI and L/S retain 30 days. OKX rubik/stat retain 30 days. KuCoin OI retains roughly 7 days. Funding rate is asset-bounded everywhere we ship, so `None`. When you cannot find an explicit retention statement, `None` is the safe choice; the harness falls back to the crypto-market-age ceiling.
- **`paginated`**: read the request schema. If there is no `start` / `endTime` / `begin` / `after` field, declare `False`. Bybit's L/S endpoint is one example that ships with `False`; others may follow if upstream schemas drop the time anchor.
- **`intervals`**: copy from the route's documented interval list, in ascending order. The adapter is responsible for mapping these to whatever the upstream actually expects (e.g. Bybit's "1d" → "D").

### Unsupported routes

When a route is unsupported on a given exchange (`rest=False, ws=False`), declare every field the route type defines, with `null` for inapplicable values:

```python
"agg_trades": {
    "rest":         False,
    "ws":           False,
    "paginated":    False,
    "max_limit":    None,
    "retention_ms": None,
},
```

This keeps the per-route-type schema uniform across exchanges. A consumer reading `caps["markets"][mt]["agg_trades"]["max_limit"]` always finds the key. No missing-key handling, no special cases per exchange. Verbosity is the cost; uniformity is the gain.

### Single source of truth

`_build_capabilities` runs once in `__init__` and the result is cached on `self._capabilities`. `get_capabilities()` returns the cached dict. Route methods read what they need via `self.get_capabilities()`. For example, `get_trades` clamps the caller's `limit` to the declared `max_limit`:

```python
limit = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
```

Per-page upstream caps (Binance candles per-call: 1500, OKX per-call: 300) are **not** part of the capabilities dict; they are inline literals inside the route method, used only when paginating. They are network mechanics, not a contract with the consumer. Only the consumer-facing ceiling (`max_limit`) appears in the capabilities map.

The canonical implementation lives in `src/exchanges/binance/adapter.py` (`BinanceAdapter._build_capabilities`). When adding a new adapter, copy it as a starting point rather than reconstructing this schema by hand.

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
* **Fail with `ValueError` or `NotImplementedError`.** These are the two exception types the route layer knows how to translate into HTTP responses. Use `ValueError` for bad input and upstream validation failures, `NotImplementedError` for features the adapter does not support on a given market type.
* **No hand-rolled retry logic at the call site.** `_make_request` (or the adapter's equivalent) handles retries and backoff. Per-call retry loops fight the rate limiter.
* **Logging via `logging.getLogger("<adapter>_adapter")`.** Keep each adapter's logs isolated so they can be filtered independently.

For exchange-specific behaviors (rate limit tiers, symbol translation quirks, API version notes) see [Exchange Notes](Exchange_Notes.md). For internal mechanics that adapter authors need but users do not, see [Adapter Internals](#adapter-internals) below.

<br>
<br>

## Testing

Adapter compliance is validated via the validator package at `tests/validator/`. The runner reads `/{exchange}/capabilities` and runs only the probes that apply to the features the adapter claims to support, so an honest capabilities map is the difference between a clean test run and noise. All declared endpoints must pass before submitting a Pull Request.

See [Validator Guide](Validator_Guide.md) for the full probe catalogue, env knobs, concurrency model, run output, and CLI examples.

<br>
<br>

## Service Lifecycle

For reference, the router uses a FastAPI lifespan manager (`@asynccontextmanager`) to handle startup and shutdown. On startup, the auto-loader walks `src/exchanges/`, instantiates every `BaseExchange` subclass it finds, and registers it in `EXCHANGE_REGISTRY`. On shutdown, the manager closes all active WebSocket tasks cleanly and calls `shutdown()` on each adapter to release connection pools and any other resources the adapter holds.

Contributors rarely need to touch this layer. The only thing an adapter owes the lifecycle is a correct `shutdown()` implementation that closes every client it opened in `__init__`.

Once `shutdown()` is correct and `python -m tests.validator` (or `python tests/validate_adapters.py`) passes cleanly, the adapter is ready for review.

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

The pattern (mirrors `KrakenAdapter._ensure_spot_ws_map`):

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
        return self.get_api_symbol(symbol, market_type)   # heuristic fallback only on fetch failure
    native = self._spot_symbol_map.get(flat)
    if native:
        return native
    raise ValueError(f"Symbol {symbol} not listed on <exchange> spot")
```

Route methods call `await self._resolve_symbol(symbol, market_type)` instead of the sync `self.get_api_symbol(...)`. The cache is lazy-loaded on first spot use, double-checked-locked for concurrency, and survives a fetch failure by falling back to the static heuristic.

When the cache loads successfully and the requested symbol is missing, raise `ValueError` (which the router maps to HTTP 400). Some exchanges (KuCoin) return a stub object for unknown symbols rather than a clean error, so trusting upstream is unsafe. The cache is the source of truth. Newly listed symbols become resolvable on the next service restart.

`get_api_symbol` and `SPOT_QUOTES` stay as the fallback path for the rare case where the live `/symbols` fetch fails; they are no longer the primary mechanism.

KuCoin uses `/api/v1/symbols` (fields: `symbol`, `baseCurrency`, `quoteCurrency`, `enableTrading`). OKX uses `/api/v5/public/instruments?instType=SPOT` (fields: `instId`, `baseCcy`, `quoteCcy`, `state == "live"`). Kraken's WS-name cache reads `/AssetPairs` for a different reason, its REST API accepts altname directly.

### Rate limit headers and proactive backoff

The user-facing patterns (header-driven proactive, reactive on rejection, proactive minimum-spacing, the 30-second fail-fast cutoff) live in [Exchange Notes → Rate-limit and ban protection](Exchange_Notes.md#rate-limit-and-ban-protection). When adding a new adapter, read the upstream's rate-limit policy and pick the right pattern; do not assume every exchange looks like Binance's weight model. The validator's `MIN_REST_INTERVAL_MS` knob is independent and only protects the test suite from itself.

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
2. **Exponential backoff with full jitter**. Up to 8 retries on Spot REST (5 on Futures) with delays of `2^attempt` seconds capped at 60s, multiplied by `0.5 + random()`. This avoids consecutive retries landing inside the same cooldown window, which Kraken explicitly documents as making the cooldown last longer. Worst-case total backoff is ~4 minutes for the most stubborn cooldowns; in practice most signals clear in ~10-30s.
3. **Global forward-rate slowdown after rate-limit confirmation**. When HTTP 429/418 or a body-level rate-limit signal fires, `_extra_interval_ms` is set to 1000ms on top of the base 200ms (so 1.2s spacing) for the next 60s. All subsequent calls (not just the failing one) honor this spacing. Quoting Kraken: "additional calls would be restricted for a few seconds (or possibly longer if calls continue to be made while the rate limits are active)."

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

- **Binance.** Standard JSON `{"method": "SUBSCRIBE", "params": [...], "id": ...}`. Connect to the **combined-stream** URL (`/stream`, not `/ws`); responses arrive wrapped as `{"stream": "...", "data": {...}}` so routing is just `msg["stream"]`. No client keepalive needed; the `websockets` library's `ping_interval=20` handles it. USDM still needs the per-bucket URL routing (see "Binance USD-M WebSocket routing buckets" below) — one hub per `(market_type, bucket)`.
- **Bybit.** Standard JSON `{"op": "subscribe", "args": [...]}`. Routing field is `topic`. Send `{"op": "ping"}` every 20 seconds via the hub's `keepalive_payload`.
- **Kraken spot.** Use the v2 WebSocket protocol on `wss://ws.kraken.com/v2`. Translate `XBT`/`XDG` to `BTC`/`DOGE` when constructing subscription payloads. The `ticker` channel needs `event_trigger: "bbo"` to emit BBO updates without trade activity. The `trade` channel needs `"snapshot": true` to emit recent trades on connect. Because incoming messages don't echo `event_trigger`/`depth`/`snapshot`, two subscriptions that differ only in those flags are indistinguishable on the receive side; the adapter therefore creates a **separate hub per subscription shape** (one for ticker, one for ticker-bbo, one for book-depth-N, etc.) keyed off the params dict in `_hub_key`.
- **Kraken futures.** Use the v1 WebSocket protocol on a separate URL (`wss://futures.kraken.com/ws/v1`); message shapes differ from v2 spot. Routing is `f"{feed}:{product_id}"`. Multiple route methods can share a single subscription on the same `feed` (futures `ticker` carries last/bid/ask/markPrice/indexPrice/fundingRate all at once, so `stream_ticker`, `stream_book_ticker`, and `stream_mark_price` all subscribe to the same `ticker:<product>` topic; the hub fans the same message into each consumer's queue).
- **KuCoin.** Two-step handshake. `POST /api/v1/bullet-public` against the spot or futures host returns `{token, instanceServers: [{endpoint, pingInterval, ...}]}`. The hub's `connect()` performs the bullet fetch, builds `endpoint?token=...&connectId=<uuid>`, opens the WS, and consumes the welcome frame before returning. Subscribe payload: `{"id": ..., "type": "subscribe", "topic": "/market/ticker:BTC-USDT", "privateChannel": false, "response": false}` (one per topic; KuCoin doesn't batch multiple topics in one subscribe). Routing is `msg["topic"]`. Heartbeat: `{"id": ..., "type": "ping"}` every ~15 seconds. Spot and futures use different bullet endpoints and topic prefixes (`/market/`, `/spotMarket/` vs `/contractMarket/`, `/contract/`).
- **OKX.** OKX closes idle connections after 30 seconds. Send a raw `"ping"` (not JSON) every 25 seconds; the server replies with raw `"pong"`. OKX subscribe args are dicts, not strings, so the adapter keeps a `_topic_args: Dict[str, dict]` mapping the synthetic topic key (`f"{channel}:{instId}"`) back to the wire-format arg the hub callbacks reconstruct.

### Internal endpoints behind `open_interest` and `long_short_ratio`

For adapter authors hitting these surfaces:

- **OKX OI** comes from `/api/v5/rubik/stat/contracts/open-interest-history`. The history endpoint returns `[ts, oi (contracts), oiCcy (coin units), oiUsd (USD notional)]`; the adapter maps `oiCcy` to `open_interest` and `oiUsd` to `value_usd`.
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
