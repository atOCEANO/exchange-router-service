<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <b>Python SDK</b> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Python SDK

The Python SDK is a thin async client over the router's REST and WebSocket interfaces. Market data methods return `pandas.DataFrame` objects indexed by datetime, ready for use in research workflows. Discovery methods return plain dicts or lists.

---

### Installation

Install directly from the repository:

```bash
pip install git+https://github.com/atOCEANO/exchange-router-service.git#subdirectory=client
```

For local development:

```bash
cd client
pip install -e .
```

---

### Quick Start

The SDK works well in Jupyter environments using top-level `await`.

```python
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient(base_url="http://localhost:8040")

df = await client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=100)
print(df.head())
```

---

### Reference

The client holds persistent network connections. Always call `await client.close()` when done to release resources.

#### Errors

The client raises three exception types depending on where the failure happens:

* `ValueError` for any 4xx response from the router. The message includes the HTTP status and the router's `detail` field.
* `ConnectionError` when a request keeps failing after `max_retries` attempts. 5xx responses and connection-level errors are retried with linear backoff before this is raised.
* `RuntimeError` for anything unexpected that is not an HTTP or connection error.

`fetch_multi_candles` is the one exception to this rule. It logs failures per symbol and returns only the symbols that succeeded, so an unknown ticker in a batch will not break the whole call.

#### DataFrame output

Market data methods that return a `pd.DataFrame` follow the same conventions:

* The index is a `DatetimeIndex` built from the response `timestamp` (Unix milliseconds). The index is timezone-naive and represents UTC.
* Column names are lowercased.
* OHLCV columns (`open`, `high`, `low`, `close`, `volume`) are coerced to numeric. Rows that cannot be parsed become `NaN`.
* An empty response returns an empty `DataFrame`, not `None`.

#### Discovery Methods

##### `get_status`
Returns service health and the list of active exchange adapters.

```python
await client.get_status()
```

**Parameters:** None

---

##### `get_version`
Returns the running service version as a plain string.

```python
await client.get_version()
```

**Parameters:** None

---

##### `get_exchanges`
Lists every enabled exchange adapter registered in the router.

```python
await client.get_exchanges()
```

**Parameters:** None

---

##### `get_market_types`
Returns the market categories supported by a specific exchange.

```python
await client.get_market_types(exchange: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name (e.g., `"binance"`).

---

##### `get_capabilities`
Returns the full feature map for an adapter. For each market type, every feature is described with `{"rest": bool, "ws": bool}` flags indicating whether a REST endpoint and a WebSocket channel are available. Interval-based features (candles, open interest, long/short ratio) also include an `"intervals"` list.

```python
await client.get_capabilities(exchange: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.

---

##### `get_markets`
Returns all tradable symbols for a given market type.

```python
await client.get_markets(exchange: str, market_type: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.

---

##### `get_exchange_info`
Returns symbol specifications, filters, and precision constraints as a DataFrame.

```python
await client.get_exchange_info(exchange: str, market_type: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.

**Returns:** `pd.DataFrame`

#### Pricing

##### `get_ticker`
Returns the latest price and 24h rolling window statistics.

```python
await client.get_ticker(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"spot"`, `"linear"`, or `"inverse"`.
* `symbol` *(str)*: Trading pair (e.g., `"BTCUSDT"`).

---

##### `get_book_ticker`
Returns the current best bid and ask price/quantity.

```python
await client.get_book_ticker(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.

---

#### Trades

##### `get_trades`
Returns recent public trade executions.

```python
await client.get_trades(exchange: str, market_type: str, symbol: str, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.
* `limit` *(int)*: Number of trades to return.

---

##### `get_agg_trades`
Returns aggregated trade data.

```python
await client.get_agg_trades(exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 500)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.
* `start` *(int)*: Optional start timestamp.
* `limit` *(int)*: Number of results to return.

---

#### Orderbook

##### `get_orderbook`
Returns the current L2 orderbook snapshot.

```python
await client.get_orderbook(exchange: str, market_type: str, symbol: str, depth: int = 20)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbol` *(str)*: Trading pair.
* `depth` *(int)*: Number of price levels to return (default: 20).

---

#### Historical Data

##### `get_candles`
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
* `start` *(int)*: Optional Unix millisecond start timestamp.

---

##### `fetch_multi_candles`
Batch variant of `get_candles`. Fetches OHLCV for many symbols concurrently, with a semaphore capping parallel requests.

```python
results = await client.fetch_multi_candles(
    exchange: str,
    market_type: str,
    symbols: List[str],
    interval: str = "1h",
    limit: int = 1000,
    max_concurrent: int = 4
)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: Market category.
* `symbols` *(List[str])*: List of trading pairs to fetch.
* `interval` *(str)*: Timeframe (default: `"1h"`).
* `limit` *(int)*: Candles per symbol (default: 1000).
* `max_concurrent` *(int)*: Max simultaneous requests (default: 4).

**Returns:** `Dict[str, pd.DataFrame]` keyed by symbol. Symbols that fail are omitted, so always iterate `results.items()` instead of assuming all input symbols are present.

```python
results = await client.fetch_multi_candles("binance", "spot", ["BTCUSDT", "ETHUSDT", "SOLUSDT"], interval="1h", limit=500)

for symbol, df in results.items():
    print(symbol, df.shape)
```

#### Futures

##### `get_mark_price`
Returns the mark price, index price, and current funding rate. Linear and inverse only.

```python
await client.get_mark_price(exchange: str, market_type: str, symbol: str)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.

---

##### `get_funding_rate`
Returns funding rate history for perpetual contracts.

```python
await client.get_funding_rate(exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `start` *(int)*: Optional start timestamp.
* `limit` *(int)*: Number of results to return.

---

##### `get_open_interest`
Returns open interest history.

```python
await client.get_open_interest(exchange: str, market_type: str, symbol: str, period: str = "1h", start: Optional[int] = None, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `period` *(str)*: Accumulation interval (e.g., `"1h"`).
* `start` *(int)*: Optional start timestamp.
* `limit` *(int)*: Number of results to return.

---

##### `get_liquidations`
Returns recent forced liquidation events.

```python
await client.get_liquidations(exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 100)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `start` *(int)*: Optional start timestamp.
* `limit` *(int)*: Number of results to return.

---

##### `get_long_short_ratio`
Returns global long/short account ratio history.

```python
await client.get_long_short_ratio(exchange: str, market_type: str, symbol: str, period: str = "5m", start: Optional[int] = None, limit: int = 30)
```

**Parameters:**
* `exchange` *(str)*: Adapter name.
* `market_type` *(str)*: `"linear"` or `"inverse"`.
* `symbol` *(str)*: Trading pair.
* `period` *(str)*: Data interval (e.g., `"5m"`, `"1h"`).
* `limit` *(int)*: Number of data points (default: 30).

#### Real-time Streams

##### `subscribe`
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
