<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <b>Contributor Guide</b>
</sub>

<br>
<br>
<br>
<br>

## Contributor Guide

The router is extended through isolated exchange adapters. Almost every contribution lives in `src/exchanges/<name>/` and leaves the routing core untouched. This guide walks through the work in roughly the order you will do it: set up a local loop, build the adapter, satisfy the contract, follow the code standards, and run the test suite before opening a PR.

<br>

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

<br>

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
    sem   = asyncio.Semaphore(2)
    tasks = [self._warm_one_funding_interval(info, sem) for info in cache.values()]

    await asyncio.gather(*tasks, return_exceptions=True)


async def _warm_one_funding_interval(self, info: SymbolInfo, sem: asyncio.Semaphore) -> None:
    async with sem:
        try:
            await self._funding_interval_ms_for(info.native_symbol)
        except Exception as e:
            logger.warning(f"funding interval lookup failed for {info.native_symbol}: {e}")
```

By convention, `_warm()` (and any `_warm_*` helpers it calls) sits right after `shutdown()` in the adapter file; every existing adapter follows this placement. `_ensure_info_cache` and `_info_for` are provided by `BaseExchange` (see [SymbolInfo cache](#symbolinfo-cache-base-provided) below); the `_ensure_*_map` methods referenced in the examples are adapter-internal lazy caches. Listing either kind in `_warm` simply forces eager warming at startup.

Do not override `preload()` directly. The base class seals it; the override point is `_warm()`. If your adapter does not need prebuilding (one bulk endpoint covers all metadata), simply don't override `_warm`.

<br>
<br>

## Local Development

The service ships as a Docker container, but iterating on an adapter through `docker compose up --build` on every change is slow. For day-to-day work, run the service directly. Create the venv once, then activate it, install dependencies, and launch with autoreload:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn src.main:app --reload --port 8040
```

On Windows, activate the venv with `.venv\Scripts\activate`. The `--reload` flag restarts the worker whenever a file under `src/` changes. Pair it with the test suite running in another terminal for a tight edit-test loop.

<br>

### Dependencies

The repo has a single `requirements.txt` covering everything: the running service (FastAPI, uvicorn, httpx, websockets, orjson) and the local tools (rich, used by the auditor). The Dockerfile installs this file as-is, so the same dependency set is present in the production image and in a contributor's venv. There is no separate dev dependency file.

**Adding a new dependency:** put it in `requirements.txt`. If it is genuinely contributor-only (a profiler, a linter, a heavy debugging library), keep it out of the requirements file and document the install command in this guide instead, so the production image stays lean.

When something breaks, it helps to bypass FastAPI entirely. Instantiate the adapter in a Python REPL and call its methods directly. The stack trace is cleaner, and you can inspect intermediate state without routing a request end to end.

When iterating without `--reload`, set `--port` on the uvicorn command line directly; the `EXCHANGE_ROUTER_SERVICE_PORT` env var is only consumed by `docker-compose.yml` and is not read by the Python service.

<br>

### Documentation diagrams

`dev/diagrams/` holds the mermaid source for every hand-made diagram in `.Documentation/imgs/`, one `.mmd` per image, plus the shared palette in `config.json`. Nothing in the service depends on it and it is not installed anywhere; it exists so that no picture in the documentation is a file nobody can remake. Rendering the whole set writes straight into the image directory:

```bash
docker run --rm --shm-size=1g \
  -v "${PWD}/dev/diagrams:/diagrams" \
  -v "${PWD}/.Documentation/imgs:/out" \
  --entrypoint sh minlag/mermaid-cli \
  -c 'set -e; for f in /diagrams/*.mmd; do n=$(basename "$f" .mmd); /home/mermaidcli/node_modules/.bin/mmdc -i "$f" -o "/out/$n.png" -c /diagrams/config.json -p /diagrams/puppeteer.json -b transparent -s 3; done'
```

The images are numbered rather than named and the descriptive name survives only in their alt text: 204644 is the README hero, 204646 the REST lifecycle, 204647 the WebSocket fan-out, 204648 the startup lifecycle, 204649 the funding timelines, 204650 backward pagination, 204651 rate-limit and ban avoidance. All seven were reconstructed by reading the rendered PNG, because no source was ever kept, so a rerun redraws them rather than reproducing the originals.

**Three things the setup depends on.** The image's bundled headless-shell is broken with an ENOENT, so `puppeteer.json` points `executablePath` at the chromium the image also ships, and its entrypoint is `mmdc` itself, so the command above clears it and calls the binary by full path. The background has to be `transparent` rather than white, or the images invert badly against GitHub's dark mode, which is what the replaced set did. And no `-w`: mermaid's own width keeps the wide diagrams at a consistent 2352 across, which is why the `width=` values in the pages can stay as they are.

**On rerunning.** Layout is deterministic: every image comes back at the identical pixel dimensions every time. The byte stream is not, for the five diagrams that contain a stadium node. Anti-aliasing along a rounded outline lands a handful of edge pixels differently between runs, on the order of 0.013 percent of the image, so a rerun is visually identical but shows as a modified file in git. Do not read that diff as a change; check the dimensions instead.

**The palette** in `config.json` is the same file emsl uses, so the two repos' diagrams are one visual set: node fill `#16232e`, a teal `#2ee6a6` border for anything the service does itself, a blue `#4d9feb` one for anything a caller or an upstream initiates, `#e6edf3` text, `#8b949e` arrows, trebuchet sans. Two classes are local to this repo, both on the rate-limit and lifecycle flows: `#c98500` for a degraded path that still returns, and `#ff5470` for a terminal one. It also names the cluster and edge-label colours, which look like padding and are not: mermaid derives whatever the palette leaves out, and left to itself it draws cluster frames as opaque brown boxes.

<br>
<br>

## Adapter Implementation

All adapters must inherit from `BaseExchange` in `src/exchanges/base.py`. The following checklist covers everything a compliant adapter needs to satisfy before it can be merged:

- [ ] **Capability Mapping:** Implement `get_capabilities()` returning the full list of supported REST routes and WebSocket channels.
- [ ] **`_fetch_exchange_info`:** Implement `async def _fetch_exchange_info(market_type) -> List[SymbolInfo]`. The base class owns the cache around it (`_ensure_info_cache`, `_info_for`, the 24h refresh); the adapter only supplies the fetch. Do not declare your own `_info_cache` fields.
- [ ] **Data Normalization:** Map all raw upstream JSON payloads to the Pydantic models in `src/models.py`.
- [ ] **Market Routing:** Handle `spot`, `linear`, and `inverse` market types, including any subdomain or parameter differences between them.
- [ ] **Symbol Normalization:** Implement `get_model_symbol(api_symbol, market_type)` to translate raw exchange symbols to normalized form (e.g. `BTCUSD_PERP` → `BTCUSD`), and `get_api_symbol(symbol, market_type)` to reverse the translation when constructing upstream requests. Normalized symbols must be bare pairs with no suffixes.
- [ ] **Perpetuals Filter:** For `linear` and `inverse` markets, `_fetch_exchange_info` must exclude dated and quarterly contracts. Only perpetual instruments should appear in `/markets` and `/markets/{symbol}`.
- [ ] **`native_symbol` Field:** Populate `native_symbol` on every `SymbolInfo` object with the raw exchange symbol before normalization.
- [ ] **`funding` Field:** Set `SymbolInfo.funding = None` on spot, `build_funding_convention("discrete")` on discrete-funding perps, and `build_funding_convention("continuous")` on continuous-funding perps.
- [ ] **`build_*` constructors:** Every record-construction site (Trade, AggTrade, Liquidation, BookTicker, Ticker, Candle, MarkPrice, OpenInterest, FundingRate) must go through the matching `base.build_*` helper. Do not construct nested value objects (`QtyValue`, `VolumeValue`, `OiValue`, `FundingCurrent`, `FundingHistorical`) by hand.
- [ ] **`quote` field at row level:** Every record that carries quote-currency values reads `quote = info.quote_asset` from the adapter's SymbolInfo cache and passes it at the row level. Look up once per route handler / WS stream and reuse.
- [ ] **Internal `_funding_interval_cache`:** Discrete-funding adapters maintain a per-symbol `cycle_ms` cache. Shape varies: Binance, Bybit, KuCoin use `Dict[MarketType, Dict[str, int]]` keyed by model symbol; OKX uses `Dict[str, int]` keyed by upstream `inst_id`. Populate it during `_fetch_exchange_info` (or as a warm step in `_warm()` for adapters that need per-symbol upstream calls); read it from MarkPrice and FundingRate constructors. SymbolInfo's `funding` block carries only the categorical `kind`, never `cycle_ms`.
- [ ] **Internal `_contract_multiplier_cache` (contract-denominated upstreams only):** When the upstream sizes linear markets in contracts (KuCoin `multiplier`, OKX `ctVal`), keep the per-symbol multiplier in an adapter-internal `_contract_multiplier_cache: Dict[MarketType, Dict[str, float]]` populated during `_fetch_exchange_info`, and convert every contract-denominated linear surface to base units with it: data rows and `SymbolInfo.min_qty` / `max_qty` / `quantity_precision`. `SymbolInfo.contract_size` stays `null` on linear per the schema; it carries the quote notional per contract on inverse only.
- [ ] **`_warm()` (optional):** Override when the adapter benefits from prebuilding caches at startup (info caches, symbol maps, per-symbol metadata fetches). Inside `_warm()`, call `await self._step("label", awaitable)` once per logical step; the base class handles background-task wrapping, timing, and structured logging. Adapters with one bulk endpoint that covers all metadata don't need to override.
- [ ] **Backward-anchored pagination:** Every method that takes `start_time` must treat it as an inclusive backward-walking upper bound. Route the call through `_paginate_backwards` (or equivalent) seeded with `start_time` on the first iteration, so the result contains records with `ts <= start_time`. This is the universal contract; do not introduce forward-walking from `start_time`.
- [ ] **Registry Registration:** Add an `__init__.py` that imports the adapter class (e.g., `from .adapter import KrakenAdapter`). The auto-loader relies on this import to discover subclasses of `BaseExchange`.

<br>
<br>

## Capabilities Contract

Every adapter must implement `get_capabilities()`, which returns a dict describing what the adapter supports for each route. The router exposes this at `GET /{exchange}/capabilities`. Clients call it to know what they can ask for. The verification harness reads it to calibrate every probe (which depths to test, what limits the router serves, whether the route is paginated, what retention window applies). The capabilities dict is the single source of truth for what each route supports: route methods read from it, the verify harness reads from it, external clients read it.

<br>

### Per-route-type uniform schema

Each route type has its own fixed schema. **Within a route type, every adapter declares the same fields, in the same order, with `null` for values that don't apply.** Different route types have different field sets (`ticker` doesn't declare `max_depth`, `orderbook` doesn't declare `intervals`), but every exchange's `ticker` looks the same shape and every exchange's `orderbook` looks the same shape.

There are no factored-out variables inside `_build_capabilities`. Every market block is written out fully at its point of use, and the duplication across `SPOT`/`LINEAR`/`INVERSE` (e.g. the candle interval list) is intentional. The block reads top to bottom without indirection.

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
                "mark_price": {
                    "rest": False,
                    "ws":   False,
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
                    "completeness": None,
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

The canonical route order across every adapter is `ticker, book_ticker, mark_price, orderbook, trades, agg_trades, candles, funding_rate, open_interest, liquidations, long_short_ratio`.

<br>

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
| `liquidations` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms`, `completeness` |
| `long_short_ratio` | `rest`, `ws`, `paginated`, `max_limit`, `retention_ms`, `intervals` |

<br>

### Field reference

| Field | Type | Meaning |
| :--- | :--- | :--- |
| `rest` | bool | REST endpoint is exposed by the router. `False` means the route returns 501. |
| `ws` | bool | WebSocket stream is exposed by the router. |
| `paginated` | bool | Caller can pass a time anchor via the `start=` query parameter; router walks back through history. `False` means the route returns whatever the upstream's most-recent page offers and no historical anchor is honoured (some account-ratio endpoints are an example). |
| `max_limit` | int \| null | The upstream's hard ceiling for routes that **cannot** paginate (`paginated: False`). The caller cannot get more than this from the route; the adapter clamps `limit=` to it. `null` for paginated routes (the adapter walks back through history; no router-side cap), for snapshots that take no `limit=` parameter (ticker, mark_price), and for unsupported routes. Per-page upstream caps used inside the adapter's pagination loop are inline literals; they are network mechanics, not part of the contract. |
| `retention_ms` | int \| null | Upstream's documented historical window in milliseconds. `null` means "asset-bounded" (data goes back as far as the asset has existed, like candles). Always written as a multiplication expression for readability: `30 * 24 * 60 * 60 * 1000` for 30 days, `7 * 24 * 60 * 60 * 1000` for 7 days. Python folds the constant at parse time. |
| `intervals` | list[str] \| null | Period strings the route accepts. `null` only on routes that have no period parameter (e.g. funding rate, where the cadence is upstream-dictated). |
| `depths` | list[int] \| null | Discrete depth buckets the upstream supports (e.g. `[5, 10, 20, 50, 100, 500, 1000]`). `null` means continuous range, any integer up to `max_depth` is accepted. |
| `max_depth` | int \| null | Depth ceiling for orderbook. `null` only when the orderbook route is unsupported on this market. |
| `completeness` | `"partial"` \| `"full"` \| null | Liquidations-only. Declares whether the feed covers every liquidation (`"full"`) or a filtered subset (`"partial"`, typical for venues that throttle the public feed). `null` when liquidations are unsupported on this market. Every public liquidations feed observed so far is `"partial"`. |

<br>

### How `paginated`, `max_limit`, and `retention_ms` interact

Four meaningful semantic states for routes that serve historical data:

| `paginated` | `max_limit` | meaning |
| :--- | :--- | :--- |
| `False` | `null` | Snapshot route (ticker, mark_price). No `limit=` parameter. |
| `False` | int | Single-page route (some L/S and trades endpoints). Caller gets the most recent N; no further history reachable. The adapter clamps any `limit=` the caller passes to this ceiling. |
| `True` | `null` | Paginated route (candles, OI, funding). Caller can pass a time anchor; the adapter walks back through history, paginating internally as far as upstream serves data. **No router-side ceiling.** Effective ceiling is upstream retention (`retention_ms`), the asset's age, or, defensively, the per-adapter pagination safety cap (typically 100 pages; see [Pagination safety caps](#pagination-safety-caps) below). |
| `True` | int | Paginated route where the upstream documents a hard total cap on retrievable history (rare; OKX is the only adapter currently using this combination, on `funding_rate` with `max_limit: 300` and on `liquidations` with `max_limit: 100`). The adapter paginates normally; `max_limit` is metadata describing the upstream's ceiling rather than a router-side clamp. The auditor uses it to size the recent-probe ask so the probe does not WARN against an upstream-imposed cap. |

The harness uses `min(big_limit, retention_ms / period_ms, market_age_ms / period_ms)` to compute the realistic ceiling on the recent-summary probe so a 1-week candle ask for 3000 records does not WARN; the ceiling is whichever is smallest of `big_limit`, `retention_ms // period_ms`, and the crypto-market age (since 2010-01-01).

<br>

### Pagination safety caps

Most adapters use the shared `_paginate_backwards` helper, bounded by an internal `max_requests = 100` page count. When that cap fires before the requested `limit` is reached, the helper logs a `WARNING` and returns whatever it managed to collect. Natural termination ("no more new data") handles every correctly-paginating case; the 100-page cap only fires for genuinely pathological asks (e.g. `?limit=1_000_000` on 1m candles, which would need ~120,000 pages).

Two adapters use custom loops with different bounds: Kraken futures candles iterates at most 20 times (no warning on exhaustion) because the upstream chart endpoint returns large batches and 20 rounds cover the supported retention; OKX candles combines a "recent" fetch with `_paginate_backwards`-driven "history" walks, both bounded by 100 pages. These are defense-in-depth knobs, not part of the capability contract; consumers should not rely on a specific cap.

If you want a different bound, fork the helper. The cap is not surfaced as a capability field.

<br>

### Where to source values from

- **`depths`**: orderbook docs almost always list supported depths. Read the upstream's `/depth` (or equivalent) endpoint documentation and copy the discrete set verbatim. Continuous-range upstreams that accept any integer up to a cap declare `depths: None` and only declare `max_depth`. Per-exchange examples live in [Exchange Notes](Exchange_Notes.md).
- **`max_limit`**: the upstream's hard ceiling for non-paginated routes only. For paginated routes set `null`. The adapter walks back through history with no router-side cap, bounded only by retention or the asset's age. Per-page upstream caps that the pagination loop uses internally (the per-call limit on each upstream's history endpoint) are inline literals in the route method, not declared in the capability map.
- **`retention_ms`**: read upstream docs for the route. Common patterns: OI and long/short history retain 30 days on some venues, 7 days on others; funding rate is typically asset-bounded so `None` is correct. When you cannot find an explicit retention statement, `None` is the safe choice; the harness falls back to the crypto-market-age ceiling.
- **`paginated`**: read the request schema. If there is no `start` / `endTime` / `begin` / `after` field, declare `False`. Some upstream endpoints (notably some account-ratio endpoints) ship with no time anchor and are honest single-page routes.
- **`intervals`**: copy from the route's documented interval list, in ascending order. The adapter is responsible for mapping these to whatever the upstream expects (e.g. `"1d"` → `"D"`).

<br>

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

This keeps the per-route-type schema uniform across exchanges. A consumer reading `caps["markets"][mt]["agg_trades"]["max_limit"]` always finds the key. No missing-key handling, no special cases per exchange.

<br>

### Single source of truth

`_build_capabilities` runs once in `__init__` and the result is cached on `self._capabilities`. `get_capabilities()` returns the cached dict. Route methods read what they need via `self.get_capabilities()`. For example, `get_trades` clamps the caller's `limit` to the declared `max_limit`:

```python
limit = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
```

Per-page upstream caps (the per-call ceiling on each upstream's history endpoint) are **not** part of the capabilities dict; they are inline literals inside the route method, used only when paginating. They are network mechanics, not a contract with the consumer. Only the consumer-facing ceiling (`max_limit`) appears in the capabilities map.

When adding a new adapter, copy `_build_capabilities` from an existing adapter under `src/exchanges/` as a starting point rather than reconstructing the schema by hand.

<br>
<br>

## Data Normalization

All data returned by an adapter must go through the Pydantic models in `src/models.py` before reaching the routing layer. Raw dictionaries are not accepted.

- **Validation:** Unmapped or malformed fields cause an immediate validation error.
- **Schema stability:** The router and downstream clients depend on these models being consistent across adapters.
- **Type safety:** This keeps price, volume, and quantity precision consistent between exchanges.

Before integrating with the router, validate normalization locally by instantiating your adapter directly in a Python REPL and confirming that all network responses parse through the relevant models without errors.

<br>

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
* **Fail with `ValueError`, `UpstreamUnavailableError`, or `NotImplementedError`.** These are the exception types the route layer knows how to translate into HTTP responses (400, 503, and 501 respectively). Use `ValueError` for bad input and upstream validation failures, `UpstreamUnavailableError` (from `src/exchanges/base.py`, optionally with `retry_after` seconds for the `Retry-After` header) for throttle and ban windows where the caller should retry later, and `NotImplementedError` for features the adapter does not support on a given market type. `UpstreamUnavailableError` is a subclass of `AdapterError`, not `ValueError`, and the route layer maps it to `503` through its own handler, so pagination loops that re-raise on `ValueError` do not swallow it.
* **No hand-rolled retry logic at the call site.** `_make_request` (or the adapter's equivalent) handles retries and backoff. Per-call retry loops fight the rate limiter.
* **Logging via `logging.getLogger("<adapter>_adapter")`.** Keep each adapter's logs isolated so they can be filtered independently.

For exchange-specific behaviors (rate limit tiers, symbol translation quirks, API version notes) see [Exchange Notes](Exchange_Notes.md). For internal mechanics that adapter authors need but users do not, see [Adapter Internals](#adapter-internals) below.

<br>
<br>

## Testing

Adapter compliance is validated via the auditor package at `tools/auditor/` (see [Auditor Guide](Auditor_Guide.md)). The runner reads `/{exchange}/capabilities` and runs only the probes that apply to the features the adapter claims to support, so an honest capabilities map is the difference between a clean test run and noise. All declared endpoints must pass before submitting a Pull Request.

The suite runs locally, not in CI: exchange APIs block the datacenter IPs that hosted runners issue from, so a GitHub Actions run fails on refused connections rather than real defects. See [Auditor Guide](Auditor_Guide.md#why-the-suite-runs-locally-not-in-ci) for the full reasoning.

See [Auditor Guide](Auditor_Guide.md) for the full probe catalogue, env knobs, concurrency model, run output, and CLI examples.

<br>

### What CI does check

`.github/workflows/ci.yml` runs on every push to `main` and every pull request. It cannot run the auditor, so it checks only what needs no upstream, and the list is deliberately short: every adapter package registers an adapter and declares a non-empty capabilities map and at least one market type, the client SDK installs into a clean interpreter and imports, and the image builds, boots, and answers on `/status`, `/version` and `/exchanges`.

The first of those is the one worth knowing about. `load_exchanges()` catches and logs an adapter that fails to import, so a broken adapter does not stop the service, it just leaves it running with one exchange fewer and nothing anywhere fails. Counting the packages on disk against the registry is what turns that into a red build. Treat a green CI run as saying the service is assembled, never as saying an adapter is correct against its exchange; only an auditor run says that.

<br>
<br>

## Releasing

`SERVICE_VERSION` in `src/version.py` is the single source of the version, and **the tag must equal it**. CI enforces this rather than generating it: a workflow that wrote the version would leave a running container reporting a number that is in no commit. `v2.2.0` was tagged on a commit still reading `2.1.0` before the rule existed; every tag from `v2.2.1` on agrees, and the guard job is what keeps it that way.

To cut a release, bump `src/version.py` in its own commit (the history keeps these separate, `chore(version): bump service to X.Y.Z`), then tag `vX.Y.Z` and push the tag. `.github/workflows/release.yml` checks the tag against `src/version.py` and publishes a GitHub Release with generated notes. A manual dispatch from the Actions tab runs the guard and stops, so the check can be dry-run before you commit to a tag.

The release builds and attaches nothing, which is deliberate. A library has to arrive as a file, so emsl ships wheels; this is a service you run, and the artifact is the repository at the tag, which GitHub attaches by itself. The release body carries the three things a reader actually needs instead: running it from a checkout without Docker, building the image from the same checkout, and installing the client. The client SDK carries its own version in `client/exchange_router_client/_version.py` and installs straight from the tag, so publishing a wheel here would only produce a file whose number disagrees with the release it is attached to.

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

<br>

### SymbolInfo cache (base-provided)

`BaseExchange` owns the per-market SymbolInfo cache: `_ensure_info_cache(market_type)` and `_info_for(market_type, model_symbol)` live on the base class, backed by a single lock and a refresh policy (`INFO_REFRESH_S = 24h`). A cache older than the refresh window is re-fetched on the next request; if the refresh fetch fails while a cached copy exists, the stale copy is served and the next attempt is deferred by `INFO_RETRY_S = 1h`. The adapter's only obligation is `_fetch_exchange_info(market_type)`, which the base calls both on first use and on refresh, so anything the adapter populates inside it (such as `_funding_interval_cache`) refreshes on the same cadence. Adapters must not declare their own `_info_cache` fields.

<br>

### `_paginate_backwards` helper

Every method that accepts `start_time` should route through `_paginate_backwards`, which lives on `BaseExchange` (or its equivalent in the adapter, like the Kraken forward walk), seeded with `start_time` on the first iteration. The helper collects forward-sorted batches, walks the response back by setting the upstream end-anchored parameter to one less than the oldest record's timestamp, and stops when no new records arrive or `max_requests` is reached. The user-facing contract is "inclusive backward-walking upper bound"; this is how that contract is implemented.

<br>

### Upstream pagination parameters

| Exchange | Native param | Inclusive? | Adapter handling |
| :--- | :--- | :--- | :--- |
| Binance | `endTime` | yes | direct passthrough |
| Bybit | `endTime` (`end` on klines) | yes | direct passthrough |
| KuCoin | `endAt` (spot, seconds), `to` (futures klines, ms), `to` (funding `from`/`to` ms), `endAt` (OI, ms) | exclusive on the spot/futures endpoints | send `start + 1`; for spot klines convert ms to seconds |
| OKX | `after` (REST), `end` (rubik) | `after` is exclusive, `end` is inclusive | for `after`, send `start + 1` to make it inclusive |
| Kraken | `since`, `from` | forward-only | compute `synthetic_since = start - limit * interval`, fetch forward, truncate to `ts <= start`, tail to `limit` |

<br>

### Perpetuals filter

| Exchange | Mechanism |
| :--- | :--- |
| Binance | filter `contractType == "PERPETUAL"` on `/dapi/v1/exchangeInfo` and `/fapi/v1/exchangeInfo` |
| Bybit | pass `contractType=LinearPerpetual` or `InversePerpetual`; also re-check on the response |
| Kraken | check `type` plus instrument prefix: `PI_*` and `PF_*` are perpetuals, `FI_*` and `FF_*` are dated |
| KuCoin | `/api/v1/contracts/active` returns only perpetuals; split linear vs inverse by `isInverse` and filter `status == "Open"` |
| OKX | filter `ctType == "linear"` or `"inverse"` plus `state == "live"` on `instType=SWAP` |

<br>

### Symbol round-trip

Adapters implement `get_api_symbol(symbol, market_type)` and `get_model_symbol(api_symbol, market_type)`. The two must compose: `get_model_symbol(get_api_symbol(s, m), m) == s` for every supported `(s, m)`. `get_api_symbol` reattaches whatever suffix or separator the upstream needs (Binance `_PERP`, Kraken `PI_`/`PF_`, OKX `-` and `-SWAP`).

<br>

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

<br>

### Rate limit headers and proactive backoff

Implementation patterns live here; user-facing behaviour lives in [Exchange Notes](Exchange_Notes.md#rate-limit-and-ban-protection); the backoff and fail-fast flow lives in [System Architecture](System_Architecture.md#rate-limiting).

When adding a new adapter, read the upstream's rate-limit policy and pick the right pattern; do not assume every exchange looks like Binance's weight model. The validator's `MIN_REST_INTERVAL_MS` knob is independent and only protects the test suite from itself.

Per-exchange implementation specifics:

| Exchange | Header(s) | Trigger |
| :--- | :--- | :--- |
| Binance | `x-mbx-used-weight-1m` | back off 2s if used weight > 1150/1200; tracked per upstream host (api / fapi / dapi have independent weight buckets) |
| Bybit | `X-Bapi-Limit-Status`, `X-Bapi-Limit-Reset-Timestamp` | back off until reset if remaining < 10 |
| KuCoin | none (30s rolling window, no per-response remaining-weight header) | reactive only on HTTP 429 |
| OKX | none | reactive only on HTTP 429 |
| Kraken | none | reactive on HTTP 429/418/502/503/504/520, body-level `EAPI:Rate limit exceeded` and `EService:Throttled: <ts>` |

Reactive backoffs (HTTP 429, soft codes like Bybit's `retCode: 10006`, Kraken's body-level errors) all funnel into the same `_backoff_until` mechanism.

<br>

### Kraken-specific retry behavior

Kraken's documented rate-limit semantics differ enough from the other exchanges that the adapter does more than simple `_backoff_until` scheduling. The implementation in [src/exchanges/kraken/adapter.py](../src/exchanges/kraken/adapter.py) `_make_request` enforces both proactive spacing (so we rarely trigger Kraken's limits in the first place) and a sophisticated retry strategy when we do.

**Proactive spacing (always on, even on the happy path):**

- **`_base_interval_ms = 200ms`** between any two Kraken calls globally. Cheap insurance against simultaneous bursts when concurrent probes happen to fire at the same instant.
- **`_pair_interval_ms = 1100ms`** between successive calls to `/0/public/OHLC` or `/0/public/Trades` for the same `pair`. Kraken docs gate these specifically per IP per pair with "1 per second or less" guidance; 1.1s gives 10% margin under that cap.

**Retry strategy when something does go wrong:**

1. **Body-level signals**, not just HTTP status. Kraken sometimes returns HTTP 200 with `error: ["EAPI:Rate limit exceeded"]` or `error: ["EService:Throttled: <unix_ts>"]` in the JSON body. The adapter parses these and treats them as rate-limit signals, sleeping until the absolute timestamp when given.
2. **Exponential backoff with full jitter**. Up to 8 retries on Spot REST (5 on Futures) with delays of `2^attempt` seconds capped at 60s, multiplied by `0.5 + random()`. This avoids consecutive retries landing inside the same cooldown window, which Kraken explicitly documents as making the cooldown last longer. Worst-case total backoff is ~4 minutes for the most stubborn cooldowns; most signals clear in ~10-30s.
3. **Global forward-rate slowdown after rate-limit confirmation**. When HTTP 429/418 or a body-level rate-limit signal fires, `_extra_interval_ms` is set to 1000ms on top of the base 200ms (so 1.2s spacing) for the next 60s. All subsequent calls (not just the failing one) honor this spacing. Per Kraken's docs: "additional calls would be restricted for a few seconds (or possibly longer if calls continue to be made while the rate limits are active)."

When all retries are exhausted, `_make_request` raises `UpstreamUnavailableError(f"Kraken throttled or unavailable: {url} (last_status=..., last_body_error=...)")`. The router translates it to HTTP 503 with the message in the body, so callers see actionable detail instead of opaque 500.

If a future contributor finds these knobs incorrect for a specific Kraken behavior, the doc reference is `https://docs.kraken.com/api/docs/guides/spot-rest-ratelimits` and `https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-`.

<br>

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

<br>

### Subscription payload quirks

Documented here so future adapter authors know what to look for when wrapping a new exchange.

- **Binance.** Standard JSON `{"method": "SUBSCRIBE", "params": [...], "id": ...}`. Connect to the **combined-stream** URL (`/stream`, not `/ws`); responses arrive wrapped as `{"stream": "...", "data": {...}}` so routing is just `msg["stream"]`. No client keepalive needed; the `websockets` library's `ping_interval=20` handles it. USDM still needs the per-bucket URL routing (see "Binance USD-M WebSocket routing buckets" below): one hub per `(market_type, bucket)`.
- **Bybit.** Standard JSON `{"op": "subscribe", "args": [...], "req_id": "sub-<ms>"}` (the `req_id` is opaque, used by Bybit to correlate responses; the adapter generates a fresh one per subscribe call). Routing field is `topic`. Send `{"op": "ping"}` every 20 seconds via the hub's `keepalive_payload`.
- **Kraken spot.** Use the v2 WebSocket protocol on `wss://ws.kraken.com/v2`. Translate `XBT`/`XDG` to `BTC`/`DOGE` when constructing subscription payloads. The `ticker` channel needs `event_trigger: "bbo"` to emit BBO updates without trade activity. The `trade` channel needs `"snapshot": true` to emit recent trades on connect. Because incoming messages don't echo `event_trigger`/`depth`/`snapshot`, two subscriptions that differ only in those flags are indistinguishable on the receive side; the adapter therefore creates a **separate hub per subscription shape** (one for ticker, one for ticker-bbo, one for book-depth-N, etc.) keyed off the params dict in `_hub_key`.
- **Kraken futures.** Use the v1 WebSocket protocol on a separate URL (`wss://futures.kraken.com/ws/v1`); message shapes differ from v2 spot. Routing is `f"{feed}:{product_id}"`. Multiple route methods can share a single subscription on the same `feed` (futures `ticker` carries last/bid/ask/markPrice/indexPrice/fundingRate all at once, so `stream_ticker`, `stream_book_ticker`, and `stream_mark_price` all subscribe to the same `ticker:<product>` topic; the hub fans the same message into each consumer's queue).
- **KuCoin.** Two-step handshake. `POST /api/v1/bullet-public` against the spot or futures host returns `{token, instanceServers: [{endpoint, pingInterval, ...}]}`. The hub's `connect()` performs the bullet fetch, builds `endpoint?token=...&connectId=<uuid>`, opens the WS, and consumes the welcome frame before returning. Subscribe payload: `{"id": ..., "type": "subscribe", "topic": "/market/ticker:BTC-USDT", "privateChannel": false, "response": false}` (one per topic; KuCoin doesn't batch multiple topics in one subscribe). Routing is `msg["topic"]`. Heartbeat: `{"id": ..., "type": "ping"}` every ~15 seconds. Spot and futures use different bullet endpoints and topic prefixes (`/market/`, `/spotMarket/` vs `/contractMarket/`, `/contract/`).
- **OKX.** OKX closes idle connections after 30 seconds. Send a raw `"ping"` (not JSON) every 25 seconds; the server replies with raw `"pong"`. OKX subscribe args are dicts, not strings, so the adapter keeps a `_topic_args: Dict[str, dict]` mapping the synthetic topic key back to the wire-format arg the hub callbacks reconstruct. The key is `f"{channel}:{instId or instType or ''}"`: instance-scoped channels (`tickers`, `trades`, etc.) key on `instId`; the global `liquidation-orders` channel keys on `instType` (`"SWAP"`) since the wire arg carries no `instId`.

<br>

### Internal endpoints behind `open_interest` and `long_short_ratio`

For adapter authors hitting these surfaces:

- **OKX OI** comes from `/api/v5/rubik/stat/contracts/open-interest-history`. The history endpoint returns `[ts, oi (contracts), oiCcy (coin units), oiUsd (USD notional)]`. The adapter picks different columns per market type: linear (`BTC-USDT-SWAP`) reads `oiCcy` (coin units) and emits `unit: "base"`; inverse (`BTC-USD-SWAP`) reads `oi` (contracts) and emits `unit: "contract"` with `contract_size` from upstream `ctVal`. The `oiUsd` column is dropped because the model does not carry a USD slot (most exchanges don't expose one; see Exchange Notes for the per-exchange unit table).
- **OKX L/S** comes from `/api/v5/rubik/stat/contracts/long-short-account-ratio`, parameterized by `ccy` (currency), not `instId`.
- **Kraken OI and L/S** come from the Kraken Futures chart analytics API (`https://futures.kraken.com/api/charts/v1/analytics/{symbol}/{type}`). The adapter uses the close value of each OHLC-style bucket as the representative.
- **KuCoin OI** comes from the unified analytics endpoint `https://api.kucoin.com/api/ua/v1/market/open-interest`, parameterized by `symbol` (futures form) and `interval` (`5min`, `15min`, `30min`, `1hour`, `4hour`, `1day`). The endpoint lives on the spot host even though it serves futures data. Response rows are `{ts, openInterest}`. KuCoin does not expose long/short ratio publicly.

<br>

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

<br>

### KuCoin: two REST domains

KuCoin splits its public REST surface across two hosts: `api.kucoin.com` for spot and the unified analytics endpoints (open interest history under `/api/ua/v1/`), and `api-futures.kucoin.com` for everything contract-related (contracts list, ticker, orderbook, trades, klines, mark price, funding history). The router routes per call. There is no caller-visible effect.

<br>

### KuCoin: timestamp unit normalisation

KuCoin mixes timestamp units across endpoints. Spot trade history (`/api/v1/market/histories`) and futures ticker, orderbook, trade history, and execution streams return timestamps in **nanoseconds**. Spot stats, futures klines, mark price, funding history, and open interest history return **milliseconds**. Spot kline rows lead with **seconds**. The adapter normalises all of these to milliseconds before they reach the model, but contributors debugging raw upstream payloads will see the source units.

<br>

### OKX: inverse base/quote derivation

For SWAP instruments (linear and inverse), OKX returns empty `baseCcy`/`quoteCcy` on the instruments endpoint. The adapter derives `base_asset` and `quote_asset` from the upstream `uly` field. If `uly` is missing, both fields fall back to empty strings.
