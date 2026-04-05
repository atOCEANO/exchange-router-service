<img src="imgs/banners/banner_1.png" width="100%" />

<h1>OCEΛNO | Exchange Router Service</h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/docker-supported-blue.svg" alt="Docker" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
  <a href="https://github.com/atOCEANO"><img src="https://img.shields.io/badge/org-OCE%CE%9BNO-black.svg" alt="Organization: OCEΛNO" /></a>
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

The router exposes two interfaces: a REST API for request/response data, and a WebSocket API for real-time streams.

*   **REST Base URI:** `http://localhost:8040`
*   **WebSocket Base URI:** `ws://localhost:8040/ws/{exchange}/{market_type}`

<br>

### WebSocket Protocol

#### Connection Multiplexing

The internal `StreamManager` pools connections for identical `(exchange, market_type, channel, symbol)` combinations to avoid hitting upstream connection limits.

- The first subscriber triggers a single upstream connection to the exchange.
- Additional subscribers attach to the existing broadcast pool. No new external connections are opened.
- When the last subscriber disconnects, the upstream connection is cancelled and all resources are released.

#### Reconnection & Heartbeats

- If an upstream connection drops unexpectedly, the adapter retries with exponential backoff. Connected local clients receive a `1011` close code and must re-subscribe.
- Exchange-specific ping/pong handling is abstracted at the adapter level and invisible to the client.

<br>

### Endpoints & Channels

#### Path Variables (REST)
*   `{exchange}`: Exchange identifier (e.g., `binance`, `bybit`).
*   `{market_type}`: Market category (`spot`, `linear`, `inverse`).
*   `{symbol}`: Trading pair (e.g., `BTCUSDT`).

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

#### WebSocket Streams

Connect to `ws://localhost:8040/ws/{exchange}/{market_type}`, then send a JSON subscription payload. The server begins streaming immediately.

| Channel | Description | Payload |
| :--- | :--- | :--- |
| `ticker` | 24h rolling price and volume stats. | `{"channel": "ticker", "symbol": "BTCUSDT"}` |
| `book_ticker` | Best bid/ask updates. | `{"channel": "book_ticker", "symbol": "BTCUSDT"}` |
| `trades` | Individual public trade executions. | `{"channel": "trades", "symbol": "BTCUSDT"}` |
| `agg_trades` | Aggregated trade events. | `{"channel": "agg_trades", "symbol": "BTCUSDT"}` |
| `orderbook` | L2 orderbook depth updates. | `{"channel": "orderbook", "symbol": "BTCUSDT"}` |
| `mark_price` | Mark price and funding rate updates. | `{"channel": "mark_price", "symbol": "BTCUSDT"}` |
| `liquidations` | Real-time liquidation events. | `{"channel": "liquidations", "symbol": "BTCUSDT"}` |

> Not all channels are available on every exchange or market type. Check `GET /{exchange}/capabilities` before subscribing. Stream payloads match the corresponding REST response schemas (e.g., `Ticker`, `OrderBook`, `Trade`).

<br>

### Status & Error Codes

#### HTTP Status Codes

| Code | Status | Meaning |
| :--- | :--- | :--- |
| **200** | Success | Request succeeded and the payload was normalized. |
| **400** | Bad Request | Invalid parameter or upstream rejection. |
| **404** | Not Found | Exchange or resource not found. |
| **429** | Too Many Requests | Rate limit exceeded. |
| **500** | Internal Error | Upstream connection failure or unhandled error. |
| **501** | Not Implemented | Feature not supported by the target adapter. |

#### WebSocket Close Codes

| Code | Meaning |
| :--- | :--- |
| **1003** | Missing `channel` or `symbol` in the subscription payload. |
| **1008** | Exchange or market type not supported. |
| **1011** | Upstream connection dropped unexpectedly. |
