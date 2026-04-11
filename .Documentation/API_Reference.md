<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <a href="Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <b>API Reference</b> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## API Reference

The router exposes two interfaces. REST covers everything request/response (discovery, historical data, snapshots). WebSocket covers real-time streams. Both sit behind the same port.

*   **REST Base URI:** `http://localhost:8040`
*   **WebSocket Base URI:** `ws://localhost:8040/ws/{exchange}/{market_type}`

---

### Path Variables

*   `{exchange}`: Exchange identifier (e.g., `binance`, `bybit`).
*   `{market_type}`: Market category (`spot`, `linear`, `inverse`).
*   `{symbol}`: Trading pair (e.g., `BTCUSDT`).

---

## REST

The REST interface covers everything that is request/response in shape: service discovery, capability maps, historical data pulls, and point-in-time snapshots. Every endpoint returns a normalized JSON payload matching one of the schemas documented in Response Shapes below, regardless of which upstream exchange served the request.

---

### Endpoints

#### Discovery

| Method | Route | Description | Params |
| :--- | :--- | :--- | :--- |
| `GET` | `/status` | Service health check. | None |
| `GET` | `/version` | Service version. | None |
| `GET` | `/exchanges` | Lists all active adapters. | None |
| `GET` | `/{exchange}/status` | Connectivity status for a specific exchange. | None |
| `GET` | `/{exchange}/capabilities` | Supported REST routes and WebSocket channels for an adapter. | None |
| `GET` | `/{exchange}/market_types` | Market types available on the exchange. | None |
| `GET` | `/{exchange}/{market_type}/info` | Symbol specs, filters, and precision constraints. | None |
| `GET` | `/{exchange}/{market_type}/markets` | All tradable symbols for a market. | None |

#### Pricing

| Method | Route | Description | Params |
| :--- | :--- | :--- | :--- |
| `GET` | `/{exchange}/{market_type}/ticker/{symbol}` | 24h rolling window price and volume stats. | None |
| `GET` | `/{exchange}/{market_type}/book_ticker/{symbol}` | Best bid/ask price and quantity. | None |

#### Trades

| Method | Route | Description | Params |
| :--- | :--- | :--- | :--- |
| `GET` | `/{exchange}/{market_type}/trades/{symbol}` | Recent public trades. | `limit` |
| `GET` | `/{exchange}/{market_type}/agg_trades/{symbol}` | Aggregated trade history. | `start`, `limit` |

#### Orderbook

| Method | Route | Description | Params |
| :--- | :--- | :--- | :--- |
| `GET` | `/{exchange}/{market_type}/orderbook/{symbol}` | Current L2 orderbook snapshot. | `depth` |

#### Historical Data

| Method | Route | Description | Params |
| :--- | :--- | :--- | :--- |
| `GET` | `/{exchange}/{market_type}/candles/{symbol}` | OHLCV candles. | `interval`, `start`, `limit` |

#### Futures
*Linear and inverse market types only.*

| Method | Route | Description | Params |
| :--- | :--- | :--- | :--- |
| `GET` | `/{exchange}/{market_type}/mark_price/{symbol}` | Mark price, index price, and current funding rate. | None |
| `GET` | `/{exchange}/{market_type}/liquidations/{symbol}` | Recent forced liquidation events. | `start`, `limit` |
| `GET` | `/{exchange}/{market_type}/funding_rate/{symbol}` | Historical funding rates. | `start`, `limit` |
| `GET` | `/{exchange}/{market_type}/open_interest/{symbol}` | Open interest history. | `period`, `start`, `limit` |
| `GET` | `/{exchange}/{market_type}/long_short_ratio/{symbol}` | Long/short account distribution. | `period`, `start`, `limit` |

---

### Parameter Bounds

All `limit` and `depth` values are validated server-side. Requests outside these bounds return `400`.

| Parameter | Min | Max | Default |
| :--- | ---: | ---: | ---: |
| `depth` (orderbook) | 1 | 100 | 20 |
| `limit` (trades) | 1 | 1000 | 100 |
| `limit` (agg_trades) | 1 | 10000 | 500 |
| `limit` (candles) | 1 | 10000 | 100 |
| `limit` (open_interest) | 1 | 5000 | 30 |
| `limit` (funding_rate) | 1 | 5000 | 100 |
| `limit` (liquidations) | 1 | 1000 | 100 |
| `limit` (long_short_ratio) | 1 | 5000 | 30 |

---

### Response Shapes

All REST responses are normalized to the same schema regardless of the upstream exchange. Timestamps are Unix milliseconds. Prices and volumes are floats. WebSocket channels emit the same shapes, noted inline.

**Ticker** &nbsp;·&nbsp; `GET /ticker/{symbol}` &nbsp;·&nbsp; WS `ticker`
```json
{
  "symbol": "BTCUSDT",
  "price": 72154.30,
  "open_24h": 71800.00,
  "high_24h": 72500.00,
  "low_24h": 71200.00,
  "volume_24h": 12345.67,
  "quote_volume_24h": 890123456.78,
  "price_change_percent": 0.49,
  "timestamp": 1712847600000
}
```

**Book Ticker** &nbsp;·&nbsp; `GET /book_ticker/{symbol}` &nbsp;·&nbsp; WS `book_ticker`
```json
{
  "symbol": "BTCUSDT",
  "bid_price": 72154.10,
  "bid_qty": 0.512,
  "ask_price": 72154.30,
  "ask_qty": 1.042,
  "timestamp": 1712847600000
}
```

**Orderbook** &nbsp;·&nbsp; `GET /orderbook/{symbol}` &nbsp;·&nbsp; WS `orderbook`
```json
{
  "symbol": "BTCUSDT",
  "bids": [[72154.10, 0.512], [72154.00, 1.200]],
  "asks": [[72154.30, 1.042], [72154.40, 0.350]],
  "timestamp": 1712847600000
}
```

**Trade** &nbsp;·&nbsp; `GET /trades/{symbol}` (list) &nbsp;·&nbsp; WS `trades`
```json
{
  "id": "4851723",
  "price": 72154.30,
  "qty": 0.012,
  "side": "buy",
  "timestamp": 1712847600123
}
```

**Aggregated Trade** &nbsp;·&nbsp; `GET /agg_trades/{symbol}` (list) &nbsp;·&nbsp; WS `agg_trades`
```json
{
  "agg_id": "8192456",
  "price": 72154.30,
  "qty": 0.153,
  "first_trade_id": "4851720",
  "last_trade_id": "4851725",
  "side": "buy",
  "timestamp": 1712847600123
}
```

**Candle** &nbsp;·&nbsp; `GET /candles/{symbol}` (list)
```json
{
  "timestamp": 1712847600000,
  "open": 72100.00,
  "high": 72200.00,
  "low": 72050.00,
  "close": 72154.30,
  "volume": 48.21
}
```

**Mark Price** &nbsp;·&nbsp; `GET /mark_price/{symbol}` &nbsp;·&nbsp; WS `mark_price` &nbsp;·&nbsp; *linear/inverse only*
```json
{
  "symbol": "BTCUSDT",
  "mark_price": 72153.85,
  "index_price": 72150.10,
  "funding_rate": 0.0001,
  "next_funding_time": 1712851200000,
  "timestamp": 1712847600000
}
```

**Funding Rate** &nbsp;·&nbsp; `GET /funding_rate/{symbol}` (list)
```json
{
  "symbol": "BTCUSDT",
  "rate": 0.0001,
  "timestamp": 1712847600000
}
```

**Open Interest** &nbsp;·&nbsp; `GET /open_interest/{symbol}` (list)
```json
{
  "symbol": "BTCUSDT",
  "open_interest": 58234.12,
  "value_usd": 4203581234.56,
  "timestamp": 1712847600000
}
```

**Liquidation** &nbsp;·&nbsp; `GET /liquidations/{symbol}` (list) &nbsp;·&nbsp; WS `liquidations`
```json
{
  "symbol": "BTCUSDT",
  "side": "sell",
  "price": 71820.00,
  "qty": 2.35,
  "timestamp": 1712847598000
}
```

**Long/Short Ratio** &nbsp;·&nbsp; `GET /long_short_ratio/{symbol}` (list)
```json
{
  "symbol": "BTCUSDT",
  "ratio": 1.82,
  "long_account": 0.645,
  "short_account": 0.355,
  "timestamp": 1712847600000
}
```

---

### HTTP Status Codes

| Code | Status | Meaning |
| :--- | :--- | :--- |
| **200** | Success | Request succeeded and the payload was normalized. |
| **400** | Bad Request | Invalid parameter or upstream rejection. |
| **404** | Not Found | Exchange or resource not found. |
| **429** | Too Many Requests | Rate limit exceeded. |
| **500** | Internal Error | Upstream connection failure or unhandled error. |
| **501** | Not Implemented | Feature not supported by the target adapter. |

All error responses include a `detail` field describing what went wrong:

```json
{"detail": "Exchange 'foo' not found or not enabled."}
```

Parameter validation errors also carry an `error` field set to `"Invalid Request"`:

```json
{"error": "Invalid Request", "detail": "Interval '2m' is not valid for binance spot candles."}
```

The router never masks upstream error detail. If the underlying exchange returns a specific message, it is passed through in `detail`.

---

## WebSocket

The WebSocket interface covers everything REST does not, real-time streams from the exchange pushed through without polling. The two interfaces share a port and a schema, so a `ticker` WebSocket message looks identical to a `GET /ticker/{symbol}` response body, and you can pick whichever fits the workload.

Connect to `ws://localhost:8040/ws/{exchange}/{market_type}` and send a JSON subscription payload. The server starts streaming as soon as the upstream exchange connection is established. There is no acknowledgement message, just data.

---

### Protocol Rules

* **One subscription per connection.** A connection is bound to a single `(channel, symbol)` tuple. To subscribe to more than one, open additional connections. The internal `StreamManager` still shares a single upstream exchange connection between any clients that pick the same tuple, so this is not wasteful.
* **No unsubscribe message.** Close the connection to unsubscribe. The server tears down resources immediately.
* **No client heartbeat required.** Exchange-specific ping/pong handling is abstracted inside the adapter. The server does not close idle connections on its own.
* **Stream payloads match REST shapes.** A `ticker` subscription yields the same JSON object as `GET /ticker/{symbol}`. See the response shapes above.
* **Availability is not uniform.** Not every channel is available on every exchange or market type. Check `GET /{exchange}/capabilities` before subscribing.

---

### Channels

| Channel | Description | Payload |
| :--- | :--- | :--- |
| `ticker` | 24h rolling price and volume stats. | `{"channel": "ticker", "symbol": "BTCUSDT"}` |
| `book_ticker` | Best bid/ask updates. | `{"channel": "book_ticker", "symbol": "BTCUSDT"}` |
| `trades` | Individual public trade executions. | `{"channel": "trades", "symbol": "BTCUSDT"}` |
| `agg_trades` | Aggregated trade events. | `{"channel": "agg_trades", "symbol": "BTCUSDT"}` |
| `orderbook` | L2 orderbook depth updates. | `{"channel": "orderbook", "symbol": "BTCUSDT"}` |
| `mark_price` | Mark price and funding rate updates. | `{"channel": "mark_price", "symbol": "BTCUSDT"}` |
| `liquidations` | Real-time liquidation events. | `{"channel": "liquidations", "symbol": "BTCUSDT"}` |

---

### Close Codes

| Code | Meaning |
| :--- | :--- |
| **1003** | Missing `channel` or `symbol` in the subscription payload. |
| **1008** | Exchange not registered, or market type not supported by that adapter. |
| **1011** | Upstream exchange connection lost. Reconnect and re-subscribe. |

---

## Examples

REST, with `curl`:

```bash
# Service health
curl http://localhost:8040/status

# Active exchanges
curl http://localhost:8040/exchanges

# Feature map for an adapter (check this before building clients)
curl http://localhost:8040/binance/capabilities

# Binance spot BTCUSDT ticker
curl http://localhost:8040/binance/spot/ticker/BTCUSDT

# Last 500 1h candles for Bybit linear ETHUSDT
curl "http://localhost:8040/bybit/linear/candles/ETHUSDT?interval=1h&limit=500"
```

WebSocket, with `wscat`:

```bash
wscat -c ws://localhost:8040/ws/binance/spot
> {"channel": "trades", "symbol": "BTCUSDT"}
```

Trade events stream immediately after the payload is sent.
