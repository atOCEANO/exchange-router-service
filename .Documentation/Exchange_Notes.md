<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <b>Exchange Notes</b> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Exchange Notes

Per-adapter implementation details. This document covers behaviors that are specific to an exchange and not derivable from the normalized API contract: API version differences, symbol translation quirks, rate limit tiers, and filter logic. It is intended for contributors implementing or maintaining adapters.

<br>
<br>

---

### Symbol Conventions

All adapters normalize to bare trading pairs (`BTCUSDT`, `ETHUSDT`) with no exchange-specific suffixes. The market type in the URL path conveys that context, so a symbol like `BTCUSDT` means different things depending on whether the path is `/binance/spot/`, `/binance/linear/`, or `/binance/inverse/`.

`GET /{exchange}/{market_type}/markets` returns a flat list of symbol strings. `GET /{exchange}/{market_type}/info` returns the full specs for each symbol, including `native_symbol`, the raw symbol as it appears on the exchange's own API:

```json
{
  "symbol": "BTCUSDT",
  "native_symbol": "BTCUSD_PERP",
  ...
}
```

For Binance inverse, `symbol` is `BTCUSDT` and `native_symbol` is `BTCUSD_PERP`. For Kraken linear, `symbol` is `XBTUSD` and `native_symbol` is `PF_XBTUSD`. Use `native_symbol` when you need to cross-reference a normalized symbol back to the exchange's documentation or raw API.

Both endpoints return perpetual contracts only. Quarterly and dated futures are excluded at the adapter level (Bybit `LinearFutures`/`InverseFutures`, Kraken `FI_*`/`FF_*`).

<br>
<br>

---

### Binance

**Symbol format.** Binance COIN-M (inverse) perpetuals carry a `_PERP` suffix in the exchange API (`BTCUSD_PERP`, `ETHUSD_PERP`). The adapter strips this suffix to produce the normalized symbol. `get_api_symbol` restores the suffix when constructing requests back to Binance, so the upstream always receives the format it expects.

**Perpetuals filter.** Binance returns all contract types from `/dapi/v1/exchangeInfo`. The adapter filters on `contractType == "PERPETUAL"` to exclude quarterly and dated contracts.

**Rate limiting.** Standard HTTP 429 with a `Retry-After` header. Request weight is tracked via the `x-mbx-used-weight-1m` response header.

**Liquidations REST.** The public REST endpoints for forced liquidation history (`/fapi/v1/allForceOrders` on USDM, `/dapi/v1/allForceOrders` on COINM) were deprecated by Binance in April 2021 and no longer return data without authentication. The adapter marks `liquidations.rest: False` for both LINEAR and INVERSE. The WebSocket stream (`forceOrder@arr`) remains publicly available and is still exposed as `liquidations.ws: True`.

<br>
<br>

---

### Bybit

**Rate limit tiers.** Bybit uses two distinct mechanisms:

- **HTTP 403** signals an IP ban. This is not a transient limit; bans last on the order of 10 minutes. The adapter sets a 10-minute backoff and raises immediately. Requests arriving while the ban is active are rejected with a clear error if the remaining backoff exceeds 30 seconds.
- **`retCode: 10006`** inside an HTTP 200 response signals a soft per-endpoint limit. The adapter backs off 2 seconds and retries rather than failing the request.
- **HTTP 429** is a standard rate limit. The adapter backs off 5 seconds and retries.

**Perpetuals filter.** Bybit returns all contract types from `/v5/market/instruments-info`. The adapter passes `contractType=LinearPerpetual` or `contractType=InversePerpetual` as a query parameter and also applies a client-side guard on the response to ensure no dated contracts (`LinearFutures`, `InverseFutures`) slip through.

**Open interest `value_usd`.** The Bybit `/v5/market/open-interest` endpoint returns only `openInterest` and `timestamp`. There is no USD notional field. The unit of `open_interest` depends on the contract type: USD for inverse, base asset (e.g. BTC) for linear. `value_usd` is always `0.0` for Bybit; this is an API limitation, not a bug.

<br>
<br>

---

### Kraken

**Two separate WebSocket APIs.** Kraken runs distinct WebSocket services for spot and futures:

- Spot: `wss://ws.kraken.com/v2` (Kraken v2 protocol)
- Futures: `wss://futures.kraken.com/ws/v1` (Kraken v1 protocol)

The message shapes, subscription formats, and acknowledgement patterns differ between the two.

**XBT → BTC symbol translation.** Kraken's spot REST API (`/AssetPairs`) returns ticker symbols using legacy asset names: `XBT/USDT`, `XDG/USDT`. The v2 WebSocket API requires the modern names: `BTC/USDT`, `DOGE/USDT`. The adapter translates via `_to_v2_wsname` when building the spot WebSocket symbol map and when constructing subscription payloads. Normalized model symbols always use the modern form.

**Perpetuals filter.** Kraken futures instruments share the same `type` field value (`futures_inverse`, `flexible_futures`) for both perpetuals and dated contracts. The adapter distinguishes them by instrument prefix: `PI_` and `PF_` are perpetuals; `FI_` and `FF_` are dated. Both a type check and a prefix check are required; the type check alone is not sufficient.

**Book ticker on spot.** The v2 WS `ticker` channel does not emit BBO updates by default; it waits for a trade. The adapter passes `event_trigger: "bbo"` in the subscription payload to receive best-bid/offer updates independently of trade activity.

**Trades WS on spot.** The Kraken v2 WebSocket `trade` channel does not send a historical snapshot by default; it only pushes trades as they occur. The adapter includes `"snapshot": true` in the subscription payload so Kraken immediately sends the most-recent trades on connect, ensuring at least one message arrives regardless of market activity.

**Open interest and long/short ratio.** OI and L/S ratio data is served from the Kraken Futures chart API at `https://futures.kraken.com/api/charts/v1/analytics/{symbol}/{type}`. The endpoint accepts `interval` (seconds, e.g. `3600` for 1h) and `since` (Unix seconds) as query parameters. Each response bucket is an OHLC-style array; the adapter uses the close value as the representative.

**Open interest `value_usd`.** The Kraken Futures chart analytics endpoint returns a raw OI value only, with no USD notional. `value_usd` is always `0.0` for Kraken.

**Candle history limits.** The Kraken spot OHLC endpoint (`/0/public/OHLC`) has a hard ceiling of 720 candles per request with no pagination support. Requests above this limit silently return only the most recent 720. Linear and inverse candles are served from the Futures chart API, which has no such restriction; the adapter paginates those endpoints normally by advancing the `from` parameter across successive requests.

**Inverse `/info` metadata.** The `/derivatives/api/v3/instruments` endpoint does not include `baseCurrency` or `quoteCurrency` for inverse perpetuals (`PI_*`). The `underlying` field present in those responses is a reference-rate identifier (e.g. `rr_xbtusd`), not a currency code. As a result, `base_asset` and `quote_asset` are empty strings for Kraken inverse symbols in `/info` responses. Linear perpetuals (`PF_*`) include these fields and return them correctly.
