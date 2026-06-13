<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9+-blue.svg" alt="Python 3.9+" /></a>
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

The SDK (`exchange-router-client` 3.0) is a translation layer over the router's REST and WebSocket interfaces. It is synchronous by default: methods return their data directly, with no `await`. The sync client runs the async machinery on a private background event loop, so the same code works in a plain script and in a Jupyter cell without `asyncio.run` or top-level `await`. An `AsyncExchangeRouterClient` with the same surface is available when you want concurrency.

Return shapes follow one rule, so you never have to remember what a call hands back:

| Kind | Returns |
|---|---|
| Time series (candles, trades, agg_trades, funding_rate, open_interest, liquidations, long_short_ratio) | `pandas.DataFrame`, datetime index, sorted oldest-first |
| Point snapshot (ticker, book_ticker, mark_price, symbol_info) | `dict` |
| Order book | `(bids, asks)`, two `price, qty` DataFrames |
| Batch (`*_many`) | `BatchResult`, behaves like a dict over the symbols that returned |
| Discovery (exchanges, market_types, capabilities, markets) | `dict` / `list` |

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

Requires Python 3.9+.

<br>
<br>

## Quick Start

No `await`. This runs identically in a script and in a notebook.

```python
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

df = client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=100)
print(df.head())

client.close()
```

The client holds persistent connections, so release it when done. Use a `with` block for guaranteed cleanup:

```python
from exchange_router_client import ExchangeRouterClient

with ExchangeRouterClient("http://localhost:8040") as client:
    df = client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=100)
    print(df.head())
```

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

m.info()          # full SymbolInfo dict
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

## DataFrame output

Series methods return a `pandas.DataFrame` with:

* a `DatetimeIndex` built from the response `timestamp` (Unix milliseconds, UTC, timezone-naive), sorted oldest-first, so `df.iloc[-1]` is always the newest row,
* short, lowercased columns,
* per-query constants moved out of the rows into `df.attrs`,
* numeric columns coerced to numbers, text columns (`side`, `id`, the trade-id fields) left as objects,
* an empty `DataFrame` for an empty response, never `None`.

Columns that are null for the whole market type by design are dropped, not carried as a column of nulls. For example `contract_size` never appears on spot or linear (it is a constant in `df.attrs` on inverse). Values that are null only on some rows (a linear open-interest row whose candle-close join missed) stay as `NaN` and trigger a warning; the SDK never fills them.

`df.attrs` is a convenience, not a contract: pandas drops it across `merge`, `groupby`, and `concat`. Nothing you need per-row lives there, and the values it holds are constant for the query, so they are recoverable from the call you made.

Columns and attrs per route:

| Route | Columns | `df.attrs` |
|---|---|---|
| candles | `open, high, low, close, volume, volume_usd` | `interval, volume_unit, usd_basis` |
| trades | `price, qty, qty_usd, side, id` | `qty_unit` |
| agg_trades | `price, qty, qty_usd, side, agg_id, first_trade_id, last_trade_id` | `qty_unit` |
| funding_rate | `rate, funding_per_hour` | `funding_kind, cycle_ms` |
| open_interest | `open_interest, open_interest_usd` | `interval, oi_unit, usd_basis` |
| liquidations | `price, qty, qty_usd, side` | `qty_unit` |
| long_short_ratio | `ratio, long_account, short_account` | `interval, account_scope` |

Every frame also carries `exchange`, `market_type`, `symbol`, `quote`, `schema_version`, and `warnings` in `df.attrs`. On inverse markets the units become `contract` and `contract_size` is added to `df.attrs`; the `*_usd` columns are then exact (price-independent) rather than approximate.

`funding_per_hour` is a derived column: `per_cycle` normalized to a one-hour rate, so funding compares directly across venues with different cycle lengths. `cycle_ms` lives in `df.attrs` when it is constant across the window, and appears as a column (with a warning) only if the funding cycle changed mid-window.

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
{'exchange': 'binance', 'market_type': 'spot', 'symbol': 'BTCUSDT', 'quote': 'USDT',
 'interval': '1h', 'volume_unit': 'base', 'usd_basis': 'close', 'schema_version': 3, 'warnings': []}
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

Warnings fire only when a result deviates from the clean case you would expect, never for facts that are always true of a market type (so `volume_usd` being a close-based approximation on every linear candle is recorded in `df.attrs['usd_basis']`, not shouted on every call). They surface through Python's `warnings` module under the `RouterDataWarning` category, so a test can promote them to errors and a consumer can filter them. They are also attached to the result:

```python
df = client.get_open_interest("binance", "linear", "BTCUSDT", period="5m", limit=30)
df.attrs["warnings"]
# ['2 of 30 rows have usd=NaN (join missed; native is populated)']
```

What warns: open-interest rows with null `usd`, fewer rows than requested on a paginated route, a mark price with no index or no funding, an opaque long/short scope (the account columns are dropped), trades with null ids, a funding cycle that changed mid-window, a non-USD quote currency, and an empty response.

<br>
<br>

## Batch fetching

The `*_many` methods fetch many symbols concurrently (the sync client still runs them in parallel under the hood) and return a `BatchResult` that behaves like a dict for the common case where everything worked, with the diagnostics there when you need them.

```python
symbols = [m["symbol"] for m in client.get_markets("binance", "linear")["markets"]]

result = client.open_interest_many("binance", "linear", symbols, period="1h", limit=100)

for symbol, df in result.items():     # every symbol that returned data
    ...

result.ok          # {symbol: DataFrame}                clean
result.degraded    # {symbol: (DataFrame, [warnings])}   returned with gaps
result.failed      # {symbol: exception}                 never returned
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

A symbol is `degraded` when its response raised a `RouterDataWarning`, `failed` when the request raised, `ok` otherwise. In a batch the warnings collect into the manifest instead of flooding the output, so a 500-symbol fetch produces one summary line, not 500.

There is a `*_many` for every series route (`candles_many`, `trades_many`, `agg_trades_many`, `funding_rate_many`, `open_interest_many`, `liquidations_many`, `long_short_ratio_many`). `fetch_multi_candles` is kept as a deprecated alias for `candles_many` that returns the plain `{symbol: DataFrame}` dict of the 2.x release.

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

These are plain functions, not coroutines, so they are never awaited. `funding_paid` handles both discrete (settlement events) and continuous (sample integration) funding correctly. Use it instead of hand-rolling the math.

Both helpers accept every shape the client produces: the DataFrame returned by `get_funding_rate`, a single row of it, the `get_mark_price` dict (rate read from its nested `funding` block), and raw wire-format rows. `per_hour_view` reads the frame's own `funding_per_hour` column when given a funding row.

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

Pass `reconnect=False` to have the iterator raise `websockets.ConnectionClosed` on a drop instead. `subscribe` is the same without reconnect. One subscription per connection: to change channel or symbol, leave the loop and start a new one. On the async client these are `async for`.

<br>
<br>

## Symbols

The router exposes routing-facing symbols: the exchange-native symbol with any contract-type prefix stripped, so Kraken `PF_XBTUSD` (linear) and `PI_XBTUSD` (inverse) are both addressed as `XBTUSD`, told apart by `market_type`, and Binance COIN-M `BTCUSD_PERP` is `BTCUSD`. Base-currency codes are never translated: Kraken stays `XBT`, not `BTC`. The SDK passes symbols through unchanged. The canonical list for any venue and market type is `client.get_markets(...)`, and each row carries `base_asset` and `quote_asset`; build a per-venue map when you compare venues whose base codes differ.

<br>
<br>

## Method reference

Signatures are for the sync client. The async client is identical with `await`, and `markets()`, `stream()`, and `subscribe()` become `async`.

**Discovery.** `get_status()`, `get_version()`, `get_exchanges()`, `get_market_types(exchange)`, `get_capabilities(exchange)`, `get_markets(exchange, market_type)`, `get_symbol_info(exchange, market_type, symbol)`.

**Snapshots (return dicts).** `get_ticker(exchange, market_type, symbol)`, `get_book_ticker(...)`, `get_mark_price(...)`.

**Order book (returns `(bids, asks)`).** `get_orderbook(exchange, market_type, symbol, depth=20)`.

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

Every series and snapshot method also takes `verbose` to override the client default for that call.

<br>
<br>

## Migrating from 2.x

* The default client is now synchronous. Drop `await` and `async with`; use the client directly or with a plain `with`. For the old async behavior, switch the import to `AsyncExchangeRouterClient`, which keeps `await` and `async with`.
* `get_orderbook` now returns `(bids, asks)` as two DataFrames instead of a dict.
* DataFrame columns are shorter and the conversion provenance moved to `df.attrs`, so code that referenced the old flattened names such as `volume_usd_basis_close_ts` needs updating; the per-query constants are now in `df.attrs` and the rest are the short columns.
* `fetch_multi_candles` still works and returns the same `{symbol: DataFrame}` dict; new code should prefer `candles_many`, which returns a `BatchResult`.
* 4xx errors now raise typed exceptions (`BadRequest`, `NotFound`, and so on) rather than a single `ValueError`. All inherit from `RouterError`.
* The wire schema is unchanged, so any code reading the raw response shapes is unaffected.
