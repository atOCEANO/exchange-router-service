<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9+-blue.svg" alt="Python 3.9+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <b>Introduction</b> &nbsp;•&nbsp; 
  <a href=".Documentation/Scope.md">Scope</a> &nbsp;•&nbsp; 
  <a href=".Documentation/API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Troubleshooting.md">Troubleshooting</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href=".Documentation/System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Capabilities_Contract.md">Capabilities Contract</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <a href=".Documentation/Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Introduction

The `exchange-router-service` is **a drop-in container that normalizes public crypto exchange market data behind one async API**. REST and WebSocket are served on the same port, and every registered adapter speaks the same schema, so a client written once works across all of them. The currently supported set is listed in [Supported Exchanges](#supported-exchanges) below.

Field names, units, funding conventions, and pagination semantics differ from one exchange to the next. The router does that translation in the adapter layer, so callers see the same `Ticker`, `Candle`, `OrderBook`, `MarkPrice`, `FundingRate` shape regardless of which exchange served the request. It also handles the parts every integration needs: pagination for large historical pulls, request-weight throttling to prevent IP bans, and persistent connection management for WebSocket streams. Adding a new exchange means dropping in a new adapter, with no changes to the routing core.

**Stateless and keyless.** The service handles public market data only. It places no orders, holds no API keys, manages no accounts, and persists nothing to disk. If you need authentication or private endpoints, this is not it. See [Scope](.Documentation/Scope.md) for the full list of non-goals and deployment assumptions before pointing real traffic at it.

<br>

<div align="center">
  <img src=".Documentation/imgs/204644.png" alt="Exchange Router Architecture" width="90%" />
  <p style="margin: 0;"><i>Architecture: Client -> Router -> Multiple Exchanges</i></p>
</div>

<br>

Clients talk to one endpoint, the router routes requests to the right exchange adapter, and the adapter normalizes the response into a schema that is identical across exchanges. REST and WebSocket both sit on the same port, and the same adapter instance serves both, so a client written against Binance spot works against Bybit linear with a single path change.

The wire format carries nested value objects on every quantitative record: each `qty`, `volume`, or `open_interest` field comes back as `{native, unit, contract_size?, usd, usd_basis?}` so a single record contains both the raw upstream value and a quote-currency notional with the conversion basis spelled out. Funding uses a `kind: "discrete" | "continuous"` discriminator so a position-PnL calculation branches on the funding model itself, with no exchange-specific code. See [API Reference](.Documentation/API_Reference.md) for the wire-format spec with worked response examples, and [Exchange Notes](.Documentation/Exchange_Notes.md) for per-exchange semantic quirks.

<br>
<br>

## Supported Exchanges

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
    <tr style="border-bottom: 2px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Inverse</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Liquidations</td>
    </tr>
    <!-- Kraken -->
    <tr style="border-bottom: 1px solid #30363d;">
      <td rowspan="3" style="padding: 12px; vertical-align: middle; text-align: left; border-right: 1px solid #30363d;">
        <img src=".Documentation/imgs/exchanges/kraken_logo.png" height="24" />
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
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Mark Price</td>
    </tr>
    <tr style="border-bottom: 2px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Inverse</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Mark Price</td>
    </tr>
    <!-- OKX -->
    <tr style="border-bottom: 1px solid #30363d;">
      <td rowspan="3" style="padding: 12px; vertical-align: middle; text-align: left; border-right: 1px solid #30363d;">
        <img src=".Documentation/imgs/exchanges/okx_logo.png" height="24" />
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
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Mark Price, Liquidations</td>
    </tr>
    <tr style="border-bottom: 2px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Inverse</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Mark Price, Liquidations</td>
    </tr>
    <!-- KuCoin -->
    <tr style="border-bottom: 1px solid #30363d;">
      <td rowspan="3" style="padding: 12px; vertical-align: middle; text-align: left; border-right: 1px solid #30363d;">
        <img src=".Documentation/imgs/exchanges/kucoin_logo.png" height="24" />
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
      <td style="padding: 8px; text-align: center; opacity: 0.3;">[ ]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Mark Price</td>
    </tr>
    <tr style="border-bottom: 1px solid #30363d;">
      <td style="padding: 8px 12px; opacity: 0.8;">Inverse</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center;">[x]</td>
      <td style="padding: 8px; text-align: center; opacity: 0.3;">[ ]</td>
      <td style="padding: 8px; text-align: left; font-size: 0.85em; max-width: 250px;">Ticker, Book Ticker, Trades, Orderbook, Mark Price</td>
    </tr>
  </tbody>
</table>

<small><i>*WebSocket streams provide real-time updates for the listed channels. See the <a href=".Documentation/API_Reference.md#channels">API Reference</a> for full channel specs and subscription payloads. Per-venue semantic quirks (symbol formats, retention windows, funding shape, rate-limit behaviour) are in <a href=".Documentation/Exchange_Notes.md">Exchange Notes</a>.</i></small>

<br>
<br>

## Quick Start

The service runs as a stateless Docker container. Clone the repo, configure the environment, launch, then verify with curl:

```bash
git clone https://github.com/atOCEANO/exchange-router-service.git
cd exchange-router-service
cp .env.example .env
docker-compose up -d --build
curl http://localhost:8040/status
curl http://localhost:8040/binance/spot/ticker/BTCUSDT
```

If the status check fails, inspect logs with `docker-compose logs -f`.

<br>

The only knob exposed at deploy time is the host port:

| Variable | Required | Default | Purpose |
| :--- | :--- | :--- | :--- |
| `EXCHANGE_ROUTER_SERVICE_PORT` | Yes | `8040` | Host port that Docker publishes. The container always binds `8040` internally. Change this if `8040` is already taken on the host, or if you run multiple router instances on the same machine. |

<br>

This variable lives in `.env` and is consumed by `docker-compose.yml` in the `ports` mapping. There is no configuration file for adapters, upstream URLs, timeouts, or rate-limit thresholds. Those are defined in code, per adapter, and changing them means editing the adapter and rebuilding the container. This is intentional: the router ships as a single immutable image and behaves identically across deployments.

<br>
<br>

## Python SDK

A synchronous client (`exchange-router-client`) over the router's REST and WebSocket interfaces, with an async client for concurrency. Time-series methods return `pandas.DataFrame` objects indexed by datetime; point-in-time snapshots (ticker, book ticker, mark price) return dicts; the order book returns two `price, qty` frames. It is sync by default, so the same code runs in a script and in a Jupyter cell with no `await`. See [Exchange Notes](.Documentation/Exchange_Notes.md) for fields whose units vary across exchanges.

```bash
pip install git+https://github.com/atOCEANO/exchange-router-service.git#subdirectory=client
```

```python
from exchange_router_client import ExchangeRouterClient

with ExchangeRouterClient("http://localhost:8040") as client:
    df = client.get_candles("binance", "spot", "BTCUSDT", interval="1h", limit=500)
    print(df.tail())
```

**Fetch candles for every symbol on an exchange, with an integrity manifest:**

```python
from exchange_router_client import ExchangeRouterClient

with ExchangeRouterClient("http://localhost:8040") as client:
    symbols = [m["symbol"] for m in client.get_markets("binance", "spot")["markets"]]

    result = client.candles_many("binance", "spot", symbols, interval="1d", limit=1000)

    print(result.report())
    print(result["BTCUSDT"].tail())
```

`result` behaves like a dict over the symbols that returned, with `.ok`, `.degraded`, and `.failed` for triage.

**Full method reference, DataFrame column layout, warnings, and end-to-end recipes are in the [Python SDK](.Documentation/Python_SDK.md) docs.**
