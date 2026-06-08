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
  <b>Python SDK</b> &nbsp;•&nbsp; 
  <a href="Troubleshooting.md">Troubleshooting</a> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Capabilities_Contract.md">Capabilities Contract</a> &nbsp;•&nbsp; 
  <a href="Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Python SDK

The Python SDK is a thin async client over the router's REST and WebSocket interfaces. List-returning methods (trades, agg trades, candles, funding rate, open interest, liquidations, long/short ratio) return `pandas.DataFrame` objects indexed by datetime. Point-in-time snapshots (ticker, book ticker, orderbook, mark price) return plain dicts. Discovery methods return dicts.

The client supports `async with` for guaranteed cleanup, or you can call `await client.close()` manually.

### Schema and DataFrame layout

The wire format uses nested value objects; the full spec is in [API Reference](API_Reference.md#response-shapes). The SDK flattens those objects via `pd.json_normalize(sep="_")` before constructing the DataFrame, so a Candle row's `volume: {native, unit, contract_size, usd, usd_basis: {method, close, close_ts}}` becomes the columns `volume_native, volume_unit, volume_contract_size, volume_usd, volume_usd_basis_method, volume_usd_basis_close, volume_usd_basis_close_ts`. The resulting per-route column lists are documented under [DataFrame output](#dataframe-output).

### Helpers for funding math

```python
from exchange_router_client import ExchangeRouterClient
from exchange_router_client.funding import funding_paid, per_hour_view

async with ExchangeRouterClient("http://localhost:8040") as client:
    rows = await client._request("GET", "binance/linear/funding_rate/BTCUSDT", {"limit": 100})

    print(per_hour_view(rows[0]))

    t_open  = rows[0]["timestamp"]
    t_close = rows[-1]["timestamp"]
    print(funding_paid(rows, t_open, t_close, notional=1_000_000))
```

`funding_paid` handles both discrete (settlement events) and continuous (sample integration) correctly. Use it instead of hand-rolling the math.

The helpers operate on the raw wire-format row shape, with the nested `rate: {kind, per_cycle, cycle_ms}` block intact. The DataFrame methods (`get_funding_rate`, etc.) flatten that block into separate columns, which the helpers cannot read; the example above goes through `client._request` to keep the nested structure.

<br>
<br>

## Installation

Install directly from the repository:

```bash
pip install git+https://github.com/atOCEANO/exchange-router-service.git#subdirectory=client
```

For local development:

```bash
cd client
pip install -e .
```

<br>
<br>

## Quick Start

The SDK is async. The examples below use top-level `await`, which works in Jupyter and Python 3.10+ REPLs. In a normal `.py` script, wrap calls in `asyncio.run(...)`.

The client holds persistent network connections, so it must be released when you are done with it. Two ways to do that:

**Explicit close** (preferred):

```python
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

df = await client.get_candles(exchange="binance", market_type="spot", symbol="BTCUSDT", interval="1h", limit=100)
print(df.head())

await client.close()
```

**Context manager** (auto-close on exit, useful in scripts where you want guaranteed cleanup even on exception):

```python
from exchange_router_client import ExchangeRouterClient

async with ExchangeRouterClient(base_url="http://localhost:8040") as client:
    df = await client.get_candles(exchange="binance", market_type="spot", symbol="BTCUSDT", interval="1h", limit=100)
    print(df.head())
```

The `start` parameter, where applicable, follows the [router's pagination contract](API_Reference.md#pagination-semantics): pass the oldest timestamp you already have to walk further back.

<br>
<br>

## Examples

Four end-to-end recipes against a running router at `http://localhost:8040`. Each is written for a Jupyter cell or `python -i` REPL (top-level `await`); for a `.py` script, wrap the body in `asyncio.run(...)`.

### 10000 1-minute candles in a single call

`get_candles` is one SDK call but the router paginates upstream behind the scenes. Binance's per-page cap on `/api/v3/klines` is 1000 records (1500 on the USDM/COINM futures endpoints), so asking for 10000 spot candles triggers ~10 upstream requests inside the adapter. Rate-limit headers are watched, back-off is applied if needed, and same-millisecond duplicates are deduped. You wait once, you get one DataFrame, you do not get banned.

```python
import time
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

t0 = time.time()
df = await client.get_candles(
    exchange="binance",
    market_type="spot",
    symbol="BTCUSDT",
    interval="1m",
    limit=10000,
)
elapsed = time.time() - t0

print(f"Got {len(df)} 1m candles in {elapsed:.1f}s")
print(f"Range: {df.index.min()} to {df.index.max()}")

await client.close()
```

The same pattern applies to every paginated route (`agg_trades`, `funding_rate`, `open_interest`, `liquidations`, `long_short_ratio`). Pass `start` to anchor the window at a specific past timestamp instead of "now".

<br>

### Daily candles for every spot market in parallel

`fetch_multi_candles` discovers the symbol list with `get_markets` (which returns the lite-list dict), then runs up to `max_concurrent` candle fetches in parallel. Symbols that fail are dropped from the result, so iterate `data_map.items()` rather than assuming every input symbol returned data.

```python
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

markets_info = await client.get_markets(exchange="binance", market_type="spot")
symbols      = [m["symbol"] for m in markets_info["markets"]]
print(f"Discovered {markets_info['count']} spot markets")

data_map = await client.fetch_multi_candles(
    exchange="binance",
    market_type="spot",
    symbols=symbols,
    interval="1d",
    limit=100,
    max_concurrent=4,
)

print(f"Fetched {len(data_map)} of {len(symbols)}")
print(data_map["BTCUSDT"].tail())

await client.close()
```

<br>

### Best bid and ask across every registered exchange

The unified schema makes cross-exchange comparison straightforward: every adapter returns the same `BookTicker` shape. The only friction is symbol naming. Some venues keep legacy base-currency codes literal (Bitcoin as `XBT`, for example) while others use the modern codes (`BTC`), so build a per-exchange symbol map for whichever pair you are querying. Pairs spelled the same everywhere (`ETH`, `SOL`, `DOGE`) skip the per-exchange map.

```python
import asyncio
from exchange_router_client import ExchangeRouterClient

SYMBOL_BY_EXCHANGE = {
    "binance": "BTCUSDT",
    "bybit":   "BTCUSDT",
    "kraken":  "XBTUSDT",
    "kucoin":  "BTCUSDT",
    "okx":     "BTCUSDT",
}

client = ExchangeRouterClient("http://localhost:8040")

quotes = await asyncio.gather(*[
    client.get_book_ticker(exchange=ex, market_type="spot", symbol=sym)
    for ex, sym in SYMBOL_BY_EXCHANGE.items()
])

for ex, q in zip(SYMBOL_BY_EXCHANGE, quotes):
    print(f"{ex:8s}  bid {q['bid_price']:>12.2f}  ask {q['ask_price']:>12.2f}")

best_bid = max(quotes, key=lambda q: q["bid_price"])
best_ask = min(quotes, key=lambda q: q["ask_price"])
print(f"Cross-exchange spread: {best_ask['ask_price'] - best_bid['bid_price']:.2f}")

await client.close()
```

<br>

### Subscribe to a ticker with auto-reconnect

The router closes the WebSocket with code `1011` when the upstream exchange disconnects. The SDK surfaces this as `websockets.ConnectionClosed` from the `subscribe` async iterator. Catch it, sleep briefly, and re-enter the loop. The `StreamManager` shares any upstream connection across clients, so reconnect cost is local-only. The cell loops indefinitely; interrupt the kernel to stop.

```python
import asyncio
import websockets
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

while True:
    try:
        async for msg in client.subscribe("binance", "spot", "ticker", "BTCUSDT"):
            vol_usd = msg["volume_24h"]["usd"]
            print(f"{msg['symbol']}  {msg['price']:>10.2f}  vol24h_usd={vol_usd:.0f}")
    except websockets.ConnectionClosed as e:
        print(f"connection closed (code={e.code}); reconnecting in 2s")
        await asyncio.sleep(2)
```

<br>
<br>

## Reference

The client holds persistent network connections. Use `async with`, or call `await client.close()` when done to release resources.

<br>

### Errors

The client raises three exception types depending on where the failure happens:

* `ValueError` for any 4xx response from the router. The message includes the HTTP status and the router's `detail` field.
* `ConnectionError` when a request keeps failing after `max_retries` attempts. 5xx responses and connection-level errors are retried with linear backoff before this is raised.
* `RuntimeError` for anything unexpected that is not an HTTP or connection error.

`fetch_multi_candles` is the one exception to this rule. It logs failures per symbol and returns only the symbols that succeeded, so a single failed symbol does not abort the entire batch.

<br>

### DataFrame output

Market data methods that return a `pd.DataFrame` follow the same conventions:

* The index is a `DatetimeIndex` built from the response `timestamp` (Unix milliseconds). The index is timezone-naive and represents UTC.
* Column names are lowercased.
* Nested wire-format objects are **flattened with `pd.json_normalize(sep="_")`**. A Candle row's `volume: {native, unit, contract_size, usd, usd_basis: {method, close, close_ts}}` becomes the columns `volume_native, volume_unit, volume_contract_size, volume_usd, volume_usd_basis_method, volume_usd_basis_close, volume_usd_basis_close_ts`. A documented allow-list of numeric columns (OHLCV, `qty_native`/`qty_usd`/`qty_contract_size`, `bid_qty_*`, `ask_qty_*`, `volume_native`/`volume_usd`/`volume_contract_size`, `open_interest_native`/`open_interest_usd`/`open_interest_contract_size`, `rate_per_cycle`, `rate_cycle_ms`) is coerced via `pd.to_numeric`; values that cannot be parsed become `NaN`. Other columns stay as Python objects.
* For most analyses, the `*_usd` columns are what you want (already in quote-currency); the `*_native` columns preserve the upstream value for traceability.
* An empty response returns an empty `DataFrame`, not `None`.

<br>

### Discovery Methods

**`get_status`**
Returns a small dict with the service health: `{"status": "ok", "service": "exchange-router-service"}`. For the list of active adapters, use `get_exchanges`.

```python
await client.get_status()
```

**Parameters:** None

**`get_version`**
Returns the running service version as a plain string.

```python
await client.get_version()
```

**Parameters:** None

**`get_exchanges`**
Lists every enabled exchange adapter registered in the router.

```python
await client.get_exchanges()
```

**Parameters:** None

**`get_market_types`**
Returns the market categories supported by a specific exchange.

```python
await client.get_market_types(exchange: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name (e.g., `"binance"`).

**`get_capabilities`**
Returns the full feature map for an adapter. For each market type, every route declares whether REST and WebSocket are exposed, plus route-specific constraints (`max_limit`, `retention_ms`, `paginated`, supported `intervals`, orderbook `depths`/`max_depth`). The exact fields per route type are documented in the [Capabilities Contract](Capabilities_Contract.md).

```python
await client.get_capabilities(exchange: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.

**`get_markets`**
Returns the lite-list metadata for every tradable symbol in a market type, as a dict. Each entry carries `symbol`, `base_asset`, `quote_asset`, `qty_unit`, `contract_size` (null on spot/linear), and `funding` (null on spot, `{"kind": "discrete"|"continuous"}` on futures). Served entirely from the in-memory adapter cache: zero upstream calls per request. On `linear` and `inverse`, only perpetuals are included; dated and quarterly futures are excluded.

```python
result  = await client.get_markets(exchange="binance", market_type="linear")
count   = result["count"]
symbols = [m["symbol"] for m in result["markets"]]
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.

**Returns:** `Dict` with keys `exchange`, `market_type`, `count`, `markets`.

**`get_symbol_info`**
Returns the full `SymbolInfo` for one symbol: precision, order limits, contract size, and the funding convention.

```python
await client.get_symbol_info(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.
* `symbol` *(str)*: Routing-facing symbol (e.g. `"BTCUSDT"`, `"BTCUSD"`, or an `XBT`-prefixed form on venues that keep legacy base-currency codes).

**Returns:** `Dict`.

<br>

### Pricing

**`get_ticker`**
Returns the latest price and 24h rolling window statistics.

```python
await client.get_ticker(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.
* `symbol` *(str)*: Trading pair (e.g., `"BTCUSDT"`).

**`get_book_ticker`**
Returns the current best bid and ask price/quantity.

```python
await client.get_book_ticker(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.

<br>

### Trades

**`get_trades`**
Returns recent public trade executions as a DataFrame. Does not accept `start`; the upstream `/trades` endpoints across all supported exchanges return only the most recent N executions.

```python
await client.get_trades(exchange: str, market_type: str, symbol: str, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.
* `limit` *(int)*: Number of trades to return.

**Returns:** `pd.DataFrame` indexed by datetime.

**`get_agg_trades`**
Returns aggregated trade data.

```python
await client.get_agg_trades(exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 500)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.
* `start` *(int)*: Optional inclusive upper bound (Unix ms). Returns records with `ts <= start`, walking back. Omit for the most recent.
* `limit` *(int)*: Number of results to return.

**Returns:** `pd.DataFrame` indexed by datetime.

<br>

### Orderbook

**`get_orderbook`**
Returns the current L2 orderbook snapshot.

```python
await client.get_orderbook(exchange: str, market_type: str, symbol: str, depth: int = 20)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.
* `depth` *(int)*: Number of price levels to return (default: 20).

<br>

### Historical Data

**`get_candles`**
Returns historical OHLCV data as a DataFrame indexed by datetime.

```python
await client.get_candles(exchange: str, market_type: str, symbol: str, interval: str = "1h", limit: int = 100, start: Optional[int] = None)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.
* `symbol` *(str)*: Trading pair (e.g., `"BTCUSDT"`).
* `interval` *(str)*: Timeframe (e.g., `"1m"`, `"5m"`, `"1h"`, `"1d"`).
* `limit` *(int)*: Number of candles to return (default: 100).
* `start` *(int)*: Optional inclusive upper bound (Unix ms). Returns candles with `ts <= start`, walking back. Omit for the most recent.

**`fetch_multi_candles`**
Batch variant of `get_candles`. Fetches OHLCV for many symbols concurrently, with a semaphore capping parallel requests.

```python
results = await client.fetch_multi_candles(
    exchange: str,
    market_type: str,
    symbols: List[str],
    interval: str = "1h",
    limit: int = 1000,
    start: Optional[int] = None,
    max_concurrent: int = 4
)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbols` *(List[str])*: List of trading pairs to fetch.
* `interval` *(str)*: Timeframe (default: `"1h"`).
* `limit` *(int)*: Candles per symbol (default: 1000).
* `start` *(int)*: Optional inclusive upper bound (Unix ms), applied to every symbol. Omit for the most recent candles per symbol.
* `max_concurrent` *(int)*: Max simultaneous requests (default: 4).

**Returns:** `Dict[str, pd.DataFrame]` keyed by symbol. Symbols that fail are omitted, so always iterate `results.items()` instead of assuming all input symbols are present.

```python
results = await client.fetch_multi_candles("binance", "spot", ["BTCUSDT", "ETHUSDT", "SOLUSDT"], interval="1h", limit=500)

for symbol, df in results.items():
    print(symbol, df.shape)
```

<br>

### Futures

**`get_mark_price`**
Returns the mark price, index price, and current funding view. The response carries a nested `funding: {kind, per_cycle, cycle_ms, valid_until_ts}` block where `kind` is `"discrete"` (charged at `valid_until_ts`) or `"continuous"` (accrued per `cycle_ms`, sampled at the row's timestamp). Per-venue funding model is documented in [Exchange Notes](Exchange_Notes.md). Use the SDK's `funding_paid` helper to compute time-weighted funding cost without branching on `kind` by hand. Linear and inverse only.

```python
await client.get_mark_price(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.

**Returns:** `Dict` with top-level `symbol, market_type, quote, mark_price, index_price, funding, timestamp`.

**`get_funding_rate`**
Returns funding rate history for perpetual contracts.

```python
await client.get_funding_rate(exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `start` *(int)*: Optional inclusive upper bound (Unix ms). Returns records with `ts <= start`, walking back. Omit for the most recent.
* `limit` *(int)*: Number of results to return.

**Returns:** `pd.DataFrame` indexed by datetime.

**`get_open_interest`**
Returns open interest history. After `pd.json_normalize`, the DataFrame carries `open_interest_native` (raw upstream value), `open_interest_unit` (`"base"` on linear, `"contract"` on inverse), `open_interest_contract_size` (null on linear, populated on inverse), `open_interest_usd` (quote-currency notional), and `open_interest_usd_basis_method` (`"candle_close"` on linear, joined server-side; `"contract_size"` on inverse). On linear markets the route handler fetches matching candles after the OI rows arrive (sequential, not parallel) and joins them by timestamp to fill the `_usd` column. If the join misses, `_usd` is `null` but `_native` is always populated.

```python
await client.get_open_interest(exchange: str, market_type: str, symbol: str, period: str = "1h", start: Optional[int] = None, limit: int = 30)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `period` *(str)*: Data interval (e.g., `"1h"`).
* `start` *(int)*: Optional inclusive upper bound (Unix ms). Returns records with `ts <= start`, walking back. Omit for the most recent.
* `limit` *(int)*: Number of results to return (default: 30).

**Returns:** `pd.DataFrame` indexed by datetime.

**`get_liquidations`**
Returns recent forced liquidation events.

```python
await client.get_liquidations(exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `start` *(int)*: Optional inclusive upper bound (Unix ms). Returns records with `ts <= start`, walking back. Omit for the most recent.
* `limit` *(int)*: Number of results to return.

**Returns:** `pd.DataFrame` indexed by datetime.

**`get_long_short_ratio`**
Returns global long/short account ratio history. Some venues' upstream endpoints ignore `start`; see [Exchange Notes](Exchange_Notes.md) for the per-venue caveats.

```python
await client.get_long_short_ratio(exchange: str, market_type: str, symbol: str, period: str = "5m", start: Optional[int] = None, limit: int = 30)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `period` *(str)*: Data interval (e.g., `"5m"`, `"1h"`).
* `start` *(int)*: Optional inclusive upper bound (Unix ms). Returns records with `ts <= start`, walking back. Omit for the most recent.
* `limit` *(int)*: Number of data points (default: 30).

**Returns:** `pd.DataFrame` indexed by datetime.

<br>

### Real-time Streams

**`subscribe`**
Connects to a WebSocket feed and yields messages as an async generator. One subscription per connection. To switch channels or symbols, exit the iterator and open a new one.

```python
async for msg in client.subscribe("binance", "spot", "ticker", "BTCUSDT"):
    print(msg)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.
* `channel` *(str)*: One of `"ticker"`, `"book_ticker"`, `"trades"`, `"agg_trades"`, `"orderbook"`, `"mark_price"`, `"liquidations"`. Check `get_capabilities` first to confirm the channel is supported on that exchange and market type.
* `symbol` *(str)*: Trading pair.

**Yields:** `Dict[str, Any]`. Each message matches the shape of the equivalent REST response. A `ticker` subscription yields the same fields as `get_ticker`, an `orderbook` subscription yields the same fields as `get_orderbook`, and so on.

If the upstream exchange connection drops, the server closes the WebSocket with code `1011`. The generator will raise `websockets.ConnectionClosed`, which you should catch and handle by re-subscribing.
