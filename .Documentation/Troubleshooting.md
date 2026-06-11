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
  <b>Troubleshooting</b> &nbsp;•&nbsp; 
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

## Troubleshooting

Symptoms and what they usually mean. For the canonical error-model design see [System Architecture](System_Architecture.md#error-handling); for the per-route response shapes see [API Reference](API_Reference.md#response-shapes).

<br>
<br>

## REST status codes

### `400 Bad Request`

The request reached an adapter, and the adapter rejected it. The `detail` field carries the underlying message.

Common causes:

* **Unknown symbol on this exchange or market type.** Spot symbols on Kraken use legacy codes (`XBTUSDT`, not `BTCUSDT`). Some venues' linear/inverse symbols differ from spot (Binance inverse needs a `_PERP`-stripped form; OKX uses dash-separated pairs internally). Check `GET /{exchange}/{market_type}/markets` for the canonical list.
* **Parameter out of range.** `limit` below 1, `depth` below 1, or an `interval` / `period` the venue does not declare for this route. The router validates `interval` and `period` against the capability map before dispatching, so an undeclared value returns `400` with a message like `Interval '2m' is not valid for kraken spot candles.` Check `GET /{exchange}/capabilities` for the accepted values.

### `404 Not Found`

The route layer rejected the request before it reached an adapter. The exchange name in the path is not registered.

Common causes:

* Typo in the exchange name (`binnance` for `binance`).
* The adapter exists in the codebase but failed to load at startup. Check the router's stdout logs for `Failed to load adapter '...'`.

### `422 Unprocessable Entity`

FastAPI rejected the request before the route handler ran. A path or query parameter failed pydantic validation, usually a `market_type` value that is not one of `spot`, `linear`, `inverse`.

### `500 Internal Server Error`

Something failed inside the adapter that was not converted into a clean `ValueError` or `NotImplementedError`. Usually means upstream HTTP retries were exhausted (5xx, connection errors, timeouts) and the adapter surfaced the raw `Exception`.

Common causes:

* Upstream exchange had an outage during the request.
* Adapter has a bug (an `IndexError`, `KeyError`, etc. that escaped the request loop). If you can reproduce, that is a bug report.

### `501 Not Implemented`

The route is not supported on this exchange/market combination. Capability map says so.

Common causes:

* Calling `funding_rate` or `mark_price` on a `spot` market.
* Calling `liquidations` on KuCoin or Kraken (neither exposes public liquidations).
* Calling `long_short_ratio` on Bybit spot, KuCoin, or any spot market.

Always check `GET /{exchange}/capabilities` before relying on a route. The capability map is the contract.

### `503 Service Unavailable`

The upstream exchange is throttling or has banned the router's IP, and the wait exceeds the adapter's fail-fast threshold (30s on Bybit and KuCoin, 60s on OKX, retry-budget exhaustion on Kraken). The request itself was valid; retry after the window clears. The response carries a `Retry-After` header (seconds) when the adapter knows the wait, and the `detail` field names the venue and the remaining time. Binance does not fail fast; on Binance a long upstream cooldown surfaces as a slow response instead.

<br>
<br>

## WebSocket close codes

### `1003`

The subscription payload was malformed. Either `channel` or `symbol` was missing, or `channel` is not one of `ticker`, `book_ticker`, `mark_price`, `agg_trades`, `trades`, `orderbook`, `liquidations`, or the requested channel is `ws: False` for this exchange/market in the capability map.

Fix: re-send a payload of the shape `{"channel": "ticker", "symbol": "BTCUSDT"}`. Check `/capabilities` before subscribing.

### `1008`

The exchange name is not registered, or the market type is not supported on this adapter.

Fix: same as REST 404 / 400. Verify the path with `GET /` and `/{exchange}/capabilities`.

### `1011`

The upstream WebSocket connection terminated (network drop, exchange-side reset, or an unhandled adapter error). Every attached client on that `(exchange, market_type, channel, symbol)` tuple is closed with this code.

Fix: reconnect and re-subscribe. The next subscriber to the same tuple opens a fresh upstream connection. Brief back-off (1-2 seconds) is enough; the upstream is usually back immediately.

<br>
<br>

## Common confusing responses

### Empty list when I expected history

The request reached past upstream retention. Each exchange documents its own retention window per route; KuCoin keeps 5m open-interest data for ~7 days, OKX keeps 5m for ~8 hours, Binance keeps 30 days, etc. The `retention_ms` field in `/capabilities` carries the value the router knows about.

If retention is correct and the list is still empty, the symbol may genuinely have had no events in the window. Check at a coarser period or against the upstream's own UI to confirm.

### Symbol comes back as `XBTUSDT`, not `BTCUSDT`

Kraken keeps legacy asset codes on its public API: Bitcoin is `XBT`, Dogecoin is `XDG`. The router preserves these literally (the [Exchange Notes → Kraken](Exchange_Notes.md#kraken) section covers the policy). The router does not translate `XBT` to `BTC` on user-facing fields; the modern code (`BTC`) only appears inside the v2 WebSocket handshake.

When querying Kraken, pass `XBTUSDT`. When querying any other venue for the same instrument, pass `BTCUSDT`.

### `open_interest.usd` is `null`

On linear markets, the `usd` field on `open_interest` comes from joining the same-period candle's `close`. When that candle does not exist (e.g. for a just-listed contract or during an upstream gap), `usd` returns `null`. The raw `native` count is always populated.

The `usd_basis` block carries the reason: `method = "candle_close"` with `close = null` and `close_ts = null` means the join missed. On inverse markets the conversion uses the fixed `contract_size` and never misses; `method = "contract_size"`.

### Trade `id` looks like a UUID, not an integer

The `id` field is an opaque string. Each venue uses its own format:

| Exchange | Format | Example |
| :--- | :--- | :--- |
| Binance, OKX | numeric integer string | `"6354281581"` |
| Bybit | UUID with hyphens | `"f960a6cf-2bcb-..."` |
| KuCoin | large integer string | `"1933627152931"` |
| Kraken spot | numeric integer string (upstream `trade_id`) | `"61044952"` |
| Kraken futures | UUID via upstream `uid` | `"ab32ca5f-..."` |

Treat `id` as identity-only; do not parse as integer. The full table lives in [Exchange Notes → Trade id formats](Exchange_Notes.md#trade-id-formats).

### Funding rate on Kraken looks 10× smaller than other exchanges

Kraken futures uses continuous funding sampled hourly (`cycle_ms: 3_600_000`), while the discrete-funding venues use 8-hour cycles (`cycle_ms: 28_800_000`). Cross-exchange comparison requires normalising to a common cadence. The conversion to per-hour-equivalent is `per_cycle / (cycle_ms / 3_600_000)`. To convert Kraken to a per-8h figure for direct comparison with discrete venues, multiply by 8.

The SDK's `per_hour_view(row)` helper handles this; see [Python SDK → Helpers](Python_SDK.md#helpers-for-funding-math). For the underlying discrete-vs-continuous semantics see [API Reference → Interpretation Fields](API_Reference.md#interpretation-fields).

### Ticker timestamp on Binance is 5-10 seconds old

Binance's `/ticker/24hr` endpoint returns a `closeTime` field with documented aggregation lag in their stats pipeline. The router exposes it as `Ticker.timestamp` faithfully. The price data is real-time; only the timestamp reflects stats-aggregation delay. Other venues return server time within ~1s of wall clock. Consumers using `ticker.timestamp` to gauge data freshness across exchanges should expect Binance to look older.

### `1000PEPEUSDT` on Binance/Bybit but `PEPEUSDT` on OKX/KuCoin

Binance and Bybit list sub-cent tokens as 1000-units to keep prices in a tradeable range. OKX and KuCoin list the bare token. The router faithfully exposes whichever the upstream uses. A consumer querying PEPE across venues must map symbols accordingly; the underlying USD math is internally consistent on each exchange.
