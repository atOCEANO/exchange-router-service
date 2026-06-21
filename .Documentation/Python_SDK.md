<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9+-blue.svg" alt="Python 3.9+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <b>Python SDK</b> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Python SDK

The SDK (`exchange-router-client`) is a translation layer over the router's REST and WebSocket interfaces. It is synchronous by default: methods return their data directly, with no `await`. The sync client runs the async machinery on a private background event loop, so the same code works in a plain script and in a Jupyter cell without `asyncio.run` or top-level `await`. An `AsyncExchangeRouterClient` with the same surface is available when you want concurrency.

The return surface follows one rule, so you never have to remember what a call hands back:

> Anything tabular is a pandas **DataFrame** with flat, stable columns. A single quote is a flat **Row** you read with `t.price`. Batches are a **BatchResult**. Every quantity is named the same way everywhere: `x` (native), `x_usd` (quote notional), `x_unit` (what `x` counts).

| Kind | Returns |
|---|---|
| Time series (candles, trades, agg_trades, funding_rate, open_interest, liquidations, long_short_ratio) | `pandas.DataFrame`, datetime index, sorted oldest-first, flat stable columns |
| Point snapshot (ticker, book_ticker, mark_price, symbol_info) | `Row`, a flat `dict` with attribute access and a `.raw` wire-dict escape hatch |
| Order book | one `DataFrame` with columns `side, price, qty` |
| Batch (`*_many`) | `BatchResult`, behaves like a dict over the symbols that returned |
| Discovery (exchanges, market_types, capabilities, markets) | `dict` / `list` |

If you are coming from 3.x, see [Migrating from 3.x](#migrating-from-3x); the wire schema is unchanged, only the client's return shapes moved.

<br>
<br>

## Installation

```bash
pip install git+https://github.com/atOCEANO/exchange-router-service.git#subdirectory=client
```

For local development:

```bash
cd client
pip install -e .
```

Requires Python 3.9+.

<br>
<br>

## Quick Start

No `await`. This runs identically in a script and in a notebook.

```python
from exchange_router_client import ExchangeRouterClient

with ExchangeRouterClient("http://localhost:8040") as client:
    df = client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=100)
    print(df.tail())
```

The client holds persistent connections, so release it when done. The `with` block above guarantees cleanup; outside one, call `client.close()`.

The `start` parameter, where applicable, follows the [router's pagination contract](API_Reference.md#pagination-semantics): pass the oldest timestamp you already have to walk further back.

<br>
<br>

## The market handle

Binding the exchange, market type, and symbol once avoids repeating them on every call.

```python
m = client.market("kraken", "linear", "XBTUSD")

candles = m.candles("1h", 500)
funding = m.funding_rate(limit=200)
mark    = m.mark_price()
book    = m.orderbook(depth=50)

m.info()          # full SymbolInfo as a Row
m.quote           # "USD"
m.funding_kind    # "discrete" | "continuous" | None
m.exchange, m.market_type, m.symbol
```

Iterate every market in a category as handles:

```python
for m in client.markets("binance", "linear"):
    oi = m.open_interest("1h", 30)
```

The handle forwards to the flat `client.get_*` methods, which remain available for one-off calls and for code that iterates raw symbol strings.

<br>
<br>

## Series: the DataFrame

Series methods return a plain `pandas.DataFrame`. Nothing is wrapped, so `concat`, `merge`, `resample`, `to_parquet`, and pickling all work normally. The frame has:

* a `DatetimeIndex` built from `timestamp` (Unix milliseconds, UTC, timezone-naive), sorted oldest-first, so `df.iloc[-1]` is the newest row,
* short, lowercased, **flat** columns,
* a **stable** column set: a route always returns the same columns, NaN where a value is absent (no column that appears or disappears with the data),
* per-query constants in `df.attrs`, read right after the call,
* numeric columns coerced to numbers, text columns (`side`, `id`, the trade-id fields) left as objects,
* an empty DataFrame (with the route's columns) for an empty response, never `None`.

Columns and `df.attrs` per route:

| Route | Columns | `df.attrs` constants |
|---|---|---|
| candles | `open, high, low, close, volume, volume_usd` | `quote, interval, volume_unit, contract_size, usd_basis` |
| trades | `price, qty, qty_usd, side, id` | `quote, qty_unit, contract_size` |
| agg_trades | `price, qty, qty_usd, side, agg_id, first_trade_id, last_trade_id` | `quote, qty_unit, contract_size` |
| funding_rate | `rate, funding_per_hour, cycle_ms` | `quote, funding_kind` |
| open_interest | `open_interest, open_interest_usd` | `quote, interval, oi_unit, contract_size, usd_basis` |
| liquidations | `price, qty, qty_usd, side` | `quote, qty_unit, contract_size` |
| long_short_ratio | `ratio, long_account, short_account` | `interval, account_scope` |

Every frame's `attrs` also carries `exchange, market_type, symbol, schema_version, warnings`.

`funding_per_hour` is `per_cycle` normalized to a one-hour rate, so funding compares directly across venues with different cycle lengths. `cycle_ms` is always a column (it can vary mid-window); a change within the window raises a warning. On `long_short_ratio`, `long_account` and `short_account` are always columns and come back `NaN` on venues that expose only the ratio (`account_scope == "opaque"`).

```python
df = client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=3)
```

```
                       open      high      low     close    volume    volume_usd
datetime
2026-06-13 07:00:00  64010.1  64120.0  63980.2  64088.7    812.34   5.20e7
2026-06-13 08:00:00  64088.7  64200.5  64050.0  64175.3    934.12   5.99e7
2026-06-13 09:00:00  64175.3  64310.0  64120.0  64290.1   1002.55   6.44e7

df.attrs
{'exchange': 'binance', 'market_type': 'spot', 'symbol': 'BTCUSDT',
 'schema_version': 3, 'warnings': [], 'quote': 'USDT', 'interval': '1h',
 'volume_unit': 'base', 'contract_size': None, 'usd_basis': 'close'}
```

`df.attrs` holds constants for the query, read them right after the call. pandas drops `attrs` across `merge` / `groupby` / `concat`, but you rarely need them back: the comparable `*_usd` column is first-class, so cross-venue work uses it and never touches `unit`. When you do want provenance to survive a `concat`, see [with_provenance](#provenance-across-a-concat).

<br>
<br>

## Snapshots: the Row

`get_ticker`, `get_book_ticker`, `get_mark_price`, and `get_symbol_info` return a `Row`: a flat `dict` subclass with attribute access. It is a real dict, so it pickles, JSON-dumps, and drops into `pd.DataFrame([...])`. The nested value objects from the wire are flattened to `x` / `x_usd` / `x_unit`. The original nested wire dict is on `.raw`.

```python
t = client.get_ticker("binance", "linear", "BTCUSDT")

t.price             # 72154.30
t.volume_24h        # 12345.67        (native)
t.volume_24h_usd    # 890123456.78
t.volume_24h_unit   # "base"
t["price"]          # key access works too; it is a dict
t.timestamp         # 1781823215000   (unix milliseconds)
t.raw               # the original nested wire dict, untouched

rows = [client.get_ticker("binance", "linear", s) for s in symbols]
pd.DataFrame(rows)  # one row per symbol, for free
```

Mark price flattens funding and adds a per-hour figure:

```python
mp = client.get_mark_price("kraken", "linear", "XBTUSD")
mp.mark_price            # 72153.85
mp.index_price           # 72150.10   (or None)
mp.funding_kind          # "continuous"
mp.funding_per_cycle     # 0.0000118
mp.funding_per_hour      # 0.0000118  (derived, comparable across venues)
```

Row keys per snapshot (missing values are `None`, never absent):

| Snapshot | Keys |
|---|---|
| ticker | `symbol, market_type, quote, price, open_24h, high_24h, low_24h, volume_24h, volume_24h_usd, volume_24h_unit, price_change_percent, timestamp` |
| book_ticker | `symbol, market_type, quote, bid_price, bid_qty, bid_qty_usd, ask_price, ask_qty, ask_qty_usd, qty_unit, timestamp` |
| mark_price | `symbol, market_type, quote, mark_price, index_price, funding_kind, funding_per_cycle, funding_cycle_ms, funding_per_hour, funding_valid_until_ts, timestamp` |
| symbol_info | `symbol, native_symbol, base_asset, quote_asset, price_precision, quantity_precision, min_qty, max_qty, min_notional, qty_unit, contract_size, funding_kind` |

<br>
<br>

## Order book

`get_orderbook` returns one tidy `DataFrame` with columns `side, price, qty` (`side` is `"bid"` or `"ask"`), bids best-first then asks best-first. `quote`, `qty_unit`, and `timestamp` are in `df.attrs`.

```python
ob = client.get_orderbook("binance", "linear", "BTCUSDT", depth=20)

bids   = ob[ob.side == "bid"]
asks   = ob[ob.side == "ask"]
spread = asks.price.min() - bids.price.max()
ob.attrs["qty_unit"]    # "base"
```

<br>
<br>

## Provenance across a concat

`df.attrs` does not survive `pd.concat`. When you stitch frames from several venues and need per-row provenance, `with_provenance` promotes the constants to columns first:

```python
from exchange_router_client import with_provenance

frames = [
    with_provenance(client.get_candles("binance", "linear",  "BTCUSDT", interval="1h", limit=500)),
    with_provenance(client.get_candles("bybit",   "inverse", "BTCUSD",  interval="1h", limit=500)),
]
both = pd.concat(frames)
both.groupby("exchange")["volume_usd"].sum()   # exchange, symbol, quote, unit ride per-row
```

<br>
<br>

## Warnings

The SDK tells you when a result is missing data, approximate, or degraded, so you do not analyze it by accident. `verbose=True` is the default.

```python
client = ExchangeRouterClient("http://localhost:8040", verbose=True)
```

Set `verbose=False` once for a quiet client, or override per call:

```python
df = client.get_open_interest("binance", "linear", "BTCUSDT", verbose=False)
```

Warnings fire only when a result deviates from the clean case you would expect, never for facts that are always true of a market type. They surface through Python's `warnings` module under the `RouterDataWarning` category, so a test can promote them to errors and a consumer can filter them. They are also attached to the result's `df.attrs["warnings"]`.

```python
df = client.get_open_interest("binance", "linear", "BTCUSDT", period="5m", limit=30)
df.attrs["warnings"]
# ['open_interest binance/linear/BTCUSDT: 2 of 30 rows have usd=NaN (join missed; native is populated)']
```

What warns: open-interest rows with `NaN` `usd`, fewer rows than requested on a paginated route, a mark price with no index or no funding, an opaque long/short scope (the account columns are `NaN`), trades with null ids, a funding cycle that changed mid-window, a non-USD quote currency, and an empty response.

<br>
<br>

## Batch fetching

The `*_many` methods fetch many symbols concurrently (the sync client still runs them in parallel under the hood) and return a `BatchResult` that behaves like a dict for the common case where everything worked, with diagnostics there when you need them. Each value is a DataFrame.

```python
symbols = [m["symbol"] for m in client.get_markets("binance", "linear")["markets"]]

result = client.open_interest_many("binance", "linear", symbols, period="1h", limit=100)

for symbol, df in result.items():     # every symbol that returned data
    ...

result.ok          # {symbol: DataFrame}                 clean
result.degraded    # {symbol: (DataFrame, [warnings])}    returned with gaps
result.failed      # {symbol: exception}                  never returned
print(result.report())
```

```
100 requested: 97 ok, 2 degraded, 1 failed
  degraded
    APTUSDT    3 of 100 rows have usd=NaN (join missed)
    SEIUSDT   11 of 100 rows have usd=NaN (join missed)
  failed
    FOOUSDT   BadRequest: ...
```

A symbol is `degraded` when the result carries warnings (in `df.attrs["warnings"]`), `failed` when the request raised, `ok` otherwise. There is a `*_many` for every series route (`candles_many`, `trades_many`, `agg_trades_many`, `funding_rate_many`, `open_interest_many`, `liquidations_many`, `long_short_ratio_many`). `fetch_multi_candles` remains as a deprecated alias for `candles_many` that returns the plain `{symbol: DataFrame}` dict.

<br>
<br>

## Helpers for funding math

```python
from exchange_router_client import ExchangeRouterClient
from exchange_router_client.funding import funding_paid, per_hour_view

with ExchangeRouterClient("http://localhost:8040") as client:
    df = client.get_funding_rate("binance", "linear", "BTCUSDT", limit=100)

    print(per_hour_view(df.iloc[-1]))

    t_open  = int(df.index[0].value // 1_000_000)
    t_close = int(df.index[-1].value // 1_000_000)
    print(funding_paid(df, t_open, t_close, notional=1_000_000))

    mark = client.get_mark_price("kraken", "linear", "XBTUSD")
    print(per_hour_view(mark))
```

These are plain functions, not coroutines, so they are never awaited. `funding_paid` handles both discrete (settlement events) and continuous (sample integration) funding correctly. Both helpers accept every shape the client produces: the `get_funding_rate` DataFrame, a single row of it, the `get_mark_price` Row (rate read from its flat `funding_*` fields), and raw wire-format rows.

<br>
<br>

## Errors

The client raises a small typed tree, so failures are programmable without parsing message strings. Every error carries `.status` (the HTTP status, where applicable) and `.detail`.

| Exception | When |
|---|---|
| `BadRequest` | `400`, including an interval or period the route does not declare |
| `NotFound` | `404`, an unknown exchange |
| `RateLimited` | `429` |
| `UpstreamUnavailable` | `503`, carries `.retry_after` |
| `NotSupported` | the route is not exposed on this exchange and market, caught before the request, or a server `501` |
| `RouterUnreachable` | transport failure, or retries exhausted |

```python
from exchange_router_client import NotSupported, UpstreamUnavailable

try:
    df = client.get_funding_rate("binance", "spot", "BTCUSDT")
except NotSupported as e:
    ...
except UpstreamUnavailable as e:
    time.sleep(e.retry_after or 5)
```

All of these inherit from `RouterError`, so `except RouterError` catches everything. The client retries `429` and transient `5xx` with jittered backoff, honoring the `Retry-After` header the router sends on `503`; if the window outlasts the retry budget it raises `UpstreamUnavailable` carrying that `retry_after` so you can wait the rest.

Symbol validity is left to the server, which normalizes case and separators. A bad symbol surfaces as one of the errors above on the call, rather than a client-side guess that could falsely reject a valid symbol.

<br>
<br>

## Asynchronous client

For real concurrency, use `AsyncExchangeRouterClient`. It exposes the same methods and the same handle, with `await`.

```python
import asyncio
from exchange_router_client import AsyncExchangeRouterClient

async def main():
    async with AsyncExchangeRouterClient("http://localhost:8040") as client:
        df = await client.get_candles("binance", "spot", "BTCUSDT", "1m", 10000)

        m = client.market("binance", "linear", "BTCUSDT")
        funding = await m.funding_rate(limit=200)

        results = await client.candles_many("binance", "spot", ["BTCUSDT", "ETHUSDT"], interval="1d")

asyncio.run(main())
```

The batch methods on the sync client already run their fetches concurrently, so reach for the async client only when you are driving your own event loop.

<br>
<br>

## Real-time streams

`stream` yields messages as dicts and reconnects automatically when the upstream drops (the router closes the socket with code `1011`).

```python
for msg in client.stream("binance", "spot", "ticker", "BTCUSDT"):
    print(msg["symbol"], msg["price"])
```

Stream messages are the raw wire dicts (the same shapes as the REST response bodies), not `Row` objects; to get the flat Row shape on a ticker, book_ticker, or mark_price message, pass it through the matching builder, for example `from exchange_router_client.rows import ticker_row; ticker_row(msg)`. Pass `reconnect=False` to have the iterator raise `websockets.ConnectionClosed` on a drop instead. `subscribe` is the same without reconnect. One subscription per connection: to change channel or symbol, leave the loop and start a new one. On the async client these are `async for`.

<br>
<br>

## Symbols

The router exposes routing-facing symbols: the exchange-native symbol with any contract-type prefix stripped, so Kraken `PF_XBTUSD` (linear) and `PI_XBTUSD` (inverse) are both addressed as `XBTUSD`, told apart by `market_type`, and Binance COIN-M `BTCUSD_PERP` is `BTCUSD`. Base-currency codes are never translated: Kraken stays `XBT`, not `BTC`. The SDK passes symbols through unchanged. The canonical list for any venue and market type is `client.get_markets(...)`, and each row carries `base_asset` and `quote_asset`; build a per-venue map when you compare venues whose base codes differ.

<br>
<br>

## Method reference

Signatures are for the sync client. The async client is identical with `await`, and `markets()`, `stream()`, and `subscribe()` become `async`.

**Discovery.** `get_status()`, `get_version()`, `get_exchanges()`, `get_market_types(exchange)`, `get_capabilities(exchange)`, `get_markets(exchange, market_type)`. `get_version()` returns a version string; the others return `dict` or `list`.

**Snapshots (return `Row`).** `get_ticker(exchange, market_type, symbol)`, `get_book_ticker(...)`, `get_mark_price(...)`, `get_symbol_info(exchange, market_type, symbol)`.

**Order book (returns a `DataFrame`).** `get_orderbook(exchange, market_type, symbol, depth=20)`.

**Series (return DataFrames).**

```python
get_candles(exchange, market_type, symbol, interval="1h", limit=100, start=None)
get_trades(exchange, market_type, symbol, limit=100)
get_agg_trades(exchange, market_type, symbol, limit=500, start=None)
get_funding_rate(exchange, market_type, symbol, limit=100, start=None)
get_open_interest(exchange, market_type, symbol, period="1h", limit=30, start=None)
get_liquidations(exchange, market_type, symbol, limit=100, start=None)
get_long_short_ratio(exchange, market_type, symbol, period="5m", limit=30, start=None)
```

**Handles.** `market(exchange, market_type, symbol)`, `markets(exchange, market_type)`.

**Batch (return `BatchResult`).** `candles_many`, `trades_many`, `agg_trades_many`, `funding_rate_many`, `open_interest_many`, `liquidations_many`, `long_short_ratio_many`, each taking the same per-route parameters plus `max_concurrent=8`.

**Streams.** `stream(exchange, market_type, channel, symbol, reconnect=True)`, `subscribe(exchange, market_type, channel, symbol)`. Channels: `ticker`, `book_ticker`, `trades`, `agg_trades`, `orderbook`, `mark_price`, `liquidations`. Check `get_capabilities` to confirm a channel is supported.

**Provenance helper.** `with_provenance(df)` returns a copy with `exchange`, `symbol`, `quote`, and `unit` promoted to columns.

Every series and snapshot method also takes `verbose` to override the client default for that call.

<br>
<br>

## Migrating from 3.x

The wire schema is unchanged, so any code reading the raw response bodies (or stream messages) is unaffected. The client return shapes changed:

* **Snapshots are now a flat `Row`, not a nested dict.** `t["volume_24h"]["native"]` becomes `t.volume_24h` (or `t["volume_24h"]`); the nested object is on `t.raw`. Funding on `get_mark_price` is flat: `mp.funding_per_cycle`, `mp.funding_kind`, plus the derived `mp.funding_per_hour`.
* **`get_orderbook` returns one DataFrame, not `(bids, asks)`.** Split with `ob[ob.side == "bid"]` and `ob[ob.side == "ask"]`.
* **Series columns are stable.** `cycle_ms` is always a column on funding, and `long_account` / `short_account` are always columns on long/short ratio (NaN when opaque). Code that did `KeyError`-prone column access now works.
* **`df.attrs` keys are unchanged in spirit** but the conversion provenance stays in `attrs`; for per-row provenance across a `concat`, use `with_provenance`.
* **`get_symbol_info` returns a `Row`** with `funding_kind` instead of a nested `funding` block.
* **4xx errors** still raise the typed `RouterError` tree from 3.x.
