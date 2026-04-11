<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <b>Introduction</b> &nbsp;•&nbsp; 
  <a href=".Documentation/Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href=".Documentation/API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href=".Documentation/System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Introduction

The `exchange-router-service` is an async API gateway for public cryptocurrency market data. It sits in front of multiple exchanges and normalizes their REST and WebSocket APIs into a single consistent schema, so you write one client instead of many.

The service handles the boring parts that every exchange integration hits eventually: pagination for large historical pulls, request-weight throttling to prevent IP bans, and persistent connection management for WebSocket streams. Adding a new exchange means dropping in a new adapter, no changes to the routing core.

**Stateless and keyless.** The service handles public market data only. It places no orders, holds no API keys, manages no accounts, and persists nothing to disk. If you need authentication or private endpoints, this is not it.


<div align="center">
  <img src=".Documentation/imgs/204644.png" alt="Exchange Router Architecture" width="90%" />
  <p style="margin: 0;"><i>Architecture: Client -> Router -> Multiple Exchanges</i></p>
</div>

Clients talk to one endpoint, the router fans requests out to the right exchange adapter, and the adapter normalizes the response into a schema that is identical across exchanges. REST and WebSocket both sit on the same port, and the same adapter instance serves both, so a client written against Binance spot works against Bybit linear with a single path change.

---

### Supported Exchanges

<table style="width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 0.9em;">
  <thead>
    <tr style="border-bottom: 2px solid #30363d; background: transparent;">
      <th style="padding: 12px; text-align: left;">Exchange</th>
      <th style="padding: 12px; text-align: left;">Market</th>
      <th style="padding: 12px; text-align: center;">OHLCV</th>
      <th style="padding: 12px; text-align: center;">Ticker</th>
      <th style="padding: 12px; text-align: center;">DOM</th>
      <th style="padding: 12px; text-align: center;">Trades</th>
      <th style="padding: 12px; text-align: center;">OI / FR</th>
      <th style="padding: 12px; text-align: center;">L/S Ratio</th>
      <th style="padding: 12px; text-align: left;">WebSocket Streams</th>
    </tr>
  </thead>
  <tbody>
    <!-- Binance -->
    <tr style="border-bottom: 1px solid #30363d;">
      <td rowspan="3" style="padding: 12px; vertical-align: middle; text-align: left; border-right: 1px solid #30363d;">
        <img src=".Documentation/imgs/exchanges/binance_logo.png" height="24" />
      </td>
      <td style="padding: 8px 12px; opacity: 0.8;">Spot</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center; opacity: 0.3;">[ ]</td>
      <td style="padding: 8px; text-align: center; opacity: 0.3;">[ ]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Agg Trades, Orderbook</td>
    </tr>
    <tr style="border-bottom: 1px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Linear</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Agg Trades, Orderbook, Mark Price, Liquidations</td>
    </tr>
    <tr style="border-bottom: 2px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Inverse</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Agg Trades, Orderbook, Mark Price, Liquidations</td>
    </tr>
    <!-- Bybit -->
    <tr style="border-bottom: 1px solid #30363d;">
      <td rowspan="3" style="padding: 12px; vertical-align: middle; text-align: left; border-right: 1px solid #30363d;">
        <img src=".Documentation/imgs/exchanges/bybit_logo.png" height="24" />
      </td>
      <td style="padding: 8px 12px; opacity: 0.8;">Spot</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center; opacity: 0.3;">[ ]</td>
      <td style="padding: 8px; text-align: center; opacity: 0.3;">[ ]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook</td>
    </tr>
    <tr style="border-bottom: 1px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Linear</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Liquidations</td>
    </tr>
    <tr style="border-bottom: 1px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Inverse</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Liquidations</td>
    </tr>
  </tbody>
</table>

<p style="font-size: 0.9em; opacity: 0.8; margin-top: 12px;"><i>*WebSocket streams provide real-time updates for the listed channels. See the <a href=".Documentation/API_Reference.md#websocket-streams" style="color: #8b949e;">API Reference</a> for full channel specs and subscription payloads.</i></p>

---

### Quick Start

The service runs as a stateless Docker container.

```bash
# Clone and enter the directory
git clone https://github.com/atOCEANO/exchange-router-service.git
cd exchange-router-service

# Configure environment
cp .env.example .env

# Launch the service
docker-compose up -d --build

# Verify the service is running (if this fails, check logs with: docker-compose logs -f)
curl http://localhost:8040/status

# Fetch Binance Spot Ticker
curl http://localhost:8040/binance/spot/ticker/BTCUSDT
```

---

### Configuration

The service is deliberately thin on configuration. Everything it needs at runtime is baked into the adapters themselves, and the only knob a typical user will touch is the host port.

| Variable | Required | Default | Purpose |
| :--- | :--- | :--- | :--- |
| `EXCHANGE_ROUTER_SERVICE_PORT` | Yes | `8040` | Host port that Docker publishes. The container always binds `8040` internally. Change this if `8040` is already taken on the host, or if you run multiple router instances on the same machine. |

The variable lives in `.env` and is consumed by `docker-compose.yml` in the `ports` mapping. It is not read by the Python service at all, so running `uvicorn src.main:app` directly during development ignores it, use `--port` on the uvicorn command line instead.

There is no configuration file for adapters, upstream URLs, timeouts, or rate limit thresholds. Those are defined in code, per adapter, and changing them means editing the adapter and rebuilding the container. This is intentional, the router ships as a fixed artifact and should behave identically across deployments.

---

### Python SDK

A thin async client over the router's REST and WebSocket interfaces. Market data methods return `pandas.DataFrame` objects indexed by datetime.

```bash
pip install git+https://github.com/atOCEANO/exchange-router-service.git#subdirectory=client
```

```python
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

df = await client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=500)
print(df.tail())

await client.close()
```

Full method reference in the [Python SDK](.Documentation/Python_SDK.md) doc.
