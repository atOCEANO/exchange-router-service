<img src=".Documentation/imgs/banners/banner_1.png" width="100%" />

<h1>OCEΛNO | Exchange Router Service</h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/docker-supported-blue.svg" alt="Docker" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
  <a href="https://github.com/atOCEANO"><img src="https://img.shields.io/badge/org-OCE%CE%9BNO-black.svg" alt="Organization: OCEΛNO" /></a>
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

The Exchange Router Service is an async API gateway for public cryptocurrency market data. It normalizes the APIs of multiple exchanges (REST for historical data, WebSocket for live feeds) into a single consistent schema.

Instead of writing separate clients for each exchange, everything goes through one interface. The service handles pagination for large historical pulls, request-weight throttling to prevent IP bans, and persistent connection management for WebSocket streams.

Adding a new exchange requires no changes to the routing core. The service is **stateless and keyless**: it executes no trades, manages no accounts, and requires no persistent storage, so it drops into any research or trading setup without extra configuration.


<div align="center">
  <img src=".Documentation/imgs/204644.png" alt="Exchange Router Architecture" width="100%" />
  <p style="margin: 0;"><i>Architecture: Client -> Router -> Multiple Exchanges</i></p>
</div>


<br>

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

<br>

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

# Verify the service is running
curl http://localhost:8040/status

# Fetch Binance Spot Ticker
curl http://localhost:8040/binance/spot/ticker/BTCUSDT
```

<br>

### Python SDK

The Python SDK is a thin async client over the router's REST and WebSocket interfaces. It returns data as `pandas.DataFrame` objects for immediate use in research workflows.

#### Installation

Install directly from the repository:

```bash
pip install git+https://github.com/atOCEANO/exchange-router-service.git#subdirectory=client
```

For local development:

```bash
cd client
pip install -e .
```

#### Usage Example

```python
from exchange_router_client import ExchangeRouterClient

client = ExchangeRouterClient("http://localhost:8040")

# Fetch normalized Binance Spot OHLCV
df_binance = await client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=500)

# Fetch normalized Bybit Inverse Futures using the same interface
df_bybit = await client.get_candles("bybit", "inverse", "BTCUSD", interval="1h", limit=500)

print(df_binance.tail())
print(df_bybit.tail())

await client.close()
```
