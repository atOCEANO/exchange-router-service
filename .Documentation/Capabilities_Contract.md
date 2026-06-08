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
  <b>Capabilities Contract</b> &nbsp;•&nbsp; 
  <a href="Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Capabilities Contract

Every adapter must implement `get_capabilities()`, which returns a dict describing what the adapter supports for each route. The router exposes this at `GET /{exchange}/capabilities`. Clients call it to know what they can ask for. The verification harness reads it to calibrate every probe (which depths to test, what limits the router serves, whether the route is paginated, what retention window applies). The capabilities dict is the single source of truth for what each route supports: route methods read from it, the verify harness reads from it, external clients read it.

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

### How `paginated`, `max_limit`, and `retention_ms` interact

Four meaningful semantic states for routes that serve historical data:

| `paginated` | `max_limit` | meaning |
| :--- | :--- | :--- |
| `False` | `null` | Snapshot route (ticker, mark_price). No `limit=` parameter. |
| `False` | int | Single-page route (some L/S and trades endpoints). Caller gets the most recent N; no further history reachable. The adapter clamps any `limit=` the caller passes to this ceiling. |
| `True` | `null` | Paginated route (candles, OI, funding). Caller can pass a time anchor; the adapter walks back through history, paginating internally as far as upstream serves data. **No router-side ceiling.** Effective ceiling is upstream retention (`retention_ms`), the asset's age, or, defensively, the per-adapter pagination safety cap (typically 100 pages; see [Pagination safety caps](#pagination-safety-caps) below). |
| `True` | int | Paginated route where the upstream documents a hard total cap on retrievable history (rare; OKX is the only adapter currently using this combination, on `funding_rate` with `max_limit: 300` and on `liquidations` with `max_limit: 100`). The adapter paginates normally; `max_limit` is metadata describing the upstream's ceiling rather than a router-side clamp. The auditor uses it to size the recent-probe ask so the probe does not WARN against an upstream-imposed cap. |

The harness uses `min(big_limit, retention_ms / period_ms, market_age_ms / period_ms)` to compute the realistic ceiling on the recent-summary probe so a 1-week candle ask for 3000 records does not WARN; the ceiling is whichever is smallest of `big_limit`, `retention_ms // period_ms`, and the crypto-market age (since 2010-01-01).

### Pagination safety caps

Most adapters use the shared `_paginate_backwards` helper, bounded by an internal `max_requests = 100` page count. When that cap fires before the requested `limit` is reached, the helper logs a `WARNING` and returns whatever it managed to collect. Natural termination ("no more new data") handles every correctly-paginating case; the 100-page cap only fires for genuinely pathological asks (e.g. `?limit=1_000_000` on 1m candles, which would need ~120,000 pages).

Two adapters use custom loops with different bounds: Kraken futures candles iterates at most 20 times (no warning on exhaustion) because the upstream chart endpoint returns large batches and 20 rounds cover the supported retention; OKX candles combines a "recent" fetch with `_paginate_backwards`-driven "history" walks, both bounded by 100 pages. These are defense-in-depth knobs, not part of the capability contract; consumers should not rely on a specific cap.

If you want a different bound, fork the helper. The cap is not surfaced as a capability field.

### Where to source values from

- **`depths`**: orderbook docs almost always list supported depths. Read the upstream's `/depth` (or equivalent) endpoint documentation and copy the discrete set verbatim. Continuous-range upstreams that accept any integer up to a cap declare `depths: None` and only declare `max_depth`. Per-exchange examples live in [Exchange Notes](Exchange_Notes.md).
- **`max_limit`**: the upstream's hard ceiling for non-paginated routes only. For paginated routes set `null`. The adapter walks back through history with no router-side cap, bounded only by retention or the asset's age. Per-page upstream caps that the pagination loop uses internally (the per-call limit on each upstream's history endpoint) are inline literals in the route method, not declared in the capability map.
- **`retention_ms`**: read upstream docs for the route. Common patterns: OI and long/short history retain 30 days on some venues, 7 days on others; funding rate is typically asset-bounded so `None` is correct. When you cannot find an explicit retention statement, `None` is the safe choice; the harness falls back to the crypto-market-age ceiling.
- **`paginated`**: read the request schema. If there is no `start` / `endTime` / `begin` / `after` field, declare `False`. Some upstream endpoints (notably some account-ratio endpoints) ship with no time anchor and are honest single-page routes.
- **`intervals`**: copy from the route's documented interval list, in ascending order. The adapter is responsible for mapping these to whatever the upstream expects (e.g. `"1d"` → `"D"`).

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

### Single source of truth

`_build_capabilities` runs once in `__init__` and the result is cached on `self._capabilities`. `get_capabilities()` returns the cached dict. Route methods read what they need via `self.get_capabilities()`. For example, `get_trades` clamps the caller's `limit` to the declared `max_limit`:

```python
limit = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
```

Per-page upstream caps (the per-call ceiling on each upstream's history endpoint) are **not** part of the capabilities dict; they are inline literals inside the route method, used only when paginating. They are network mechanics, not a contract with the consumer. Only the consumer-facing ceiling (`max_limit`) appears in the capabilities map.

When adding a new adapter, copy `_build_capabilities` from an existing adapter under `src/exchanges/` as a starting point rather than reconstructing the schema by hand.
