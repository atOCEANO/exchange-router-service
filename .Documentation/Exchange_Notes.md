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
  <b>Exchange Notes</b> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Validator_Guide.md">Validator Guide</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Exchange Notes

Quirks per exchange that callers can observe: symbol formats, fields the upstream does not expose, retention windows that bound historical queries, and unit semantics that survive normalisation. The schema is uniform across exchanges; the data behind it is not always. The router never invents data, so missing fields come back as `0` or an empty string and queries past retention come back as an empty list.

<br>
<br>

## Shared Conventions

### Symbol normalization

All adapters normalize to bare trading pairs (`BTCUSDT`, `ETHUSDT`) with no exchange-specific suffixes. The market type in the URL path conveys that context, so a symbol like `BTCUSDT` means different things depending on whether the path is `/binance/spot/`, `/binance/linear/`, or `/binance/inverse/`.

`GET /{exchange}/{market_type}/markets` returns a flat list of symbol strings. `GET /{exchange}/{market_type}/info` returns the full specs for each symbol, including `native_symbol`, the raw symbol as it appears on the exchange's own API:

```json
{
  "symbol": "BTCUSDT",
  "native_symbol": "BTCUSD_PERP",
  ...
}
```

For Binance inverse, `symbol` is `BTCUSDT` and `native_symbol` is `BTCUSD_PERP`. For Kraken linear, `symbol` is `XBTUSD` and `native_symbol` is `PF_XBTUSD`. For OKX inverse, `symbol` is `BTCUSD` and `native_symbol` is `BTC-USD-SWAP`. Use `native_symbol` when you need to cross-reference a normalized symbol back to the exchange's documentation or raw API.

### Perpetuals only on contract markets

For `linear` and `inverse` market types, both `/markets` and `/info` return perpetual contracts only. Quarterly and dated futures are excluded by every adapter. Spot markets are unaffected and return every active pair.

### Empty fields mean "upstream did not provide"

`SymbolInfo` fields like `min_notional`, `max_qty`, `base_asset`, `quote_asset` default to `0` or `""` when the upstream does not return them. They are never invented or computed. Each exchange's section below documents which fields are missing on that exchange.

### Pagination is always backward-walking

Every endpoint that accepts `start` treats it as an inclusive backward-walking upper bound: `?start=X&limit=N` returns up to N records with `ts <= X`, sorted ascending. Omit `start` for the most recent N. The response body is always sorted ascending regardless of which adapter served it. For the full pagination contract, see [API Reference](API_Reference.md#pagination-semantics).

The router maps this universal contract to each upstream's native parameters. Where the upstream walks forward only (Kraken), the adapter computes a synthetic forward start, fetches a window, and truncates. The user-visible behavior is identical; backward-anchored requests on Kraken are slightly slower than on the other exchanges.

<br>

### Rate-limit and ban protection

Every adapter handles rate-limit and ban behaviour transparently. You can call any route at full speed; a steady stream runs as fast as upstream allows, a burst slows down on its own, and a hard ban window surfaces as an immediate fail-fast error until it clears. No per-call sleep loops or client retry logic needed.

Per-exchange retry and ban specifics live in each exchange's section below. The implementation patterns (header-driven proactive backoff, reactive on rejection, proactive minimum-spacing, the 30-second fail-fast cutoff) live in [Contributor Guide → Rate limit headers and proactive backoff](Contributor_Guide.md#rate-limit-headers-and-proactive-backoff).

<br>

### Trade and timestamp conventions

Trade `side` is always normalized to `"buy"` or `"sell"` (taker side). Timestamps are always Unix milliseconds. Orderbook levels are `[price, size]` tuples; `size` follows the same unit as `qty` for the same market, see each exchange's section for the per-market specifics.

<br>
<br>

## Binance

### Symbol format

Binance COIN-M (inverse) perpetuals carry a `_PERP` suffix in the exchange API (`BTCUSD_PERP`, `ETHUSD_PERP`). The router normalizes them to bare pairs (`BTCUSD`, `ETHUSD`). Use the `native_symbol` field on `/info` responses if you need to cross-reference back to Binance's docs.

### Liquidations REST is disabled

Forced liquidation history is not available via public REST. The endpoints (`/fapi/v1/allForceOrders` on USDM, `/dapi/v1/allForceOrders` on COINM) require authentication, so `liquidations.rest` is `False` for both linear and inverse. The WebSocket stream (`forceOrder@arr`) is public, so `liquidations.ws` is `True`.

### COIN-M `qty` and `volume` are in contracts

For COIN-M (inverse) perpetuals, `qty` and `volume` are reported in contracts, not the base asset. For `BTCUSD_PERP`, 1 contract represents 100 USD of notional. The router does not convert. Multiply by the contract multiplier (and divide by `price`) if you need a base-asset volume; consult Binance's COIN-M contract spec for each instrument's multiplier. USD-M (linear) and SPOT report `qty` and `volume` in the base asset directly.

<br>
<br>

## Bybit

Bybit uses bare-pair symbols natively (`BTCUSDT`, `ETHUSDT`) on both spot and contracts; `native_symbol` matches `symbol` everywhere.

### IP bans

Bybit returns HTTP 403 (not 429) on persistent rate-limit violations. These are IP bans on the order of 10 minutes. The router fails the triggering request immediately and rejects subsequent requests until the ban window has passed. Soft per-endpoint limits are handled internally with brief backoff and retry.

### Open interest unit and `value_usd`

The Bybit open-interest endpoint returns only a raw `openInterest` number. The unit depends on contract type: USD for inverse, base asset for linear. Bybit does not provide a USD notional, so `value_usd` is always `0` on Bybit.

### Long/short ratio derivation

The Bybit account-ratio endpoint returns `buyRatio` and `sellRatio` directly. The router maps `long_account = buyRatio` and `short_account = sellRatio`, and computes `ratio = buyRatio / sellRatio`. This is the inverse of the OKX convention (where the upstream sends a single `ratio` and the router derives the accounts).

### Long/short ratio ignores `start_time`

Bybit's `/v5/market/account-ratio` endpoint does not accept a start timestamp. When `start_time` is provided, the router logs a warning and returns the most recent data. The capability map declares this honestly with `long_short_ratio.start_param = False`, so the verification harness skips both anchor probes on this route and emits one informational row in their place. This is an upstream limitation, not an adapter bug; be aware when comparing time-anchored requests across exchanges.

<br>
<br>

## Kraken

### Legacy asset codes stay literal

Kraken uses legacy asset codes on its REST API: Bitcoin is `XBT`, Dogecoin is `XDG`. The router preserves these in normalized symbols, so Bitcoin on Kraken is `XBTUSD` / `XBTUSDT`, not `BTCUSD` / `BTCUSDT`. The v2 WebSocket API requires the modern names (`BTC`, `DOGE`); the adapter translates internally for the WS handshake only, never on the user-facing model. Pass `XBT...` / `XDG...` symbols in router requests that target Kraken; pass `BTC...` / `DOGE...` for the other four exchanges.

### Open interest unit and `value_usd`

Kraken's futures OI endpoint returns a raw numeric value with no USD notional. `value_usd` is always `0` on Kraken.

### Candle history limits

The Kraken spot OHLC endpoint has a hard ceiling of 720 candles per request with no pagination support. Requests above this limit silently return only the most recent 720. Linear and inverse candles use the Futures chart API and paginate normally.

### Inverse `/info` metadata

The Kraken Futures instruments endpoint does not include `baseCurrency` or `quoteCurrency` for inverse perpetuals (`PI_*`). The `underlying` field present in those responses is a reference-rate identifier (e.g. `rr_xbtusd`), not a currency code. As a result, `base_asset` and `quote_asset` are empty strings for Kraken inverse symbols in `/info` responses. Linear perpetuals (`PF_*`) include these fields and return them correctly.

<br>
<br>

## KuCoin

### Symbol format

KuCoin spot uses dash-separated pairs (`BTC-USDT`). Futures use a contract suffix `M` on the concatenated form: `XBTUSDTM` for USDT-margined linear, `XBTUSDM` for USD-margined inverse. The router strips the dash on spot and the trailing `M` on futures, so normalized symbols are bare pairs (`BTCUSDT`, `XBTUSDT`, `XBTUSD`). Base-currency codes are passed through literally, so KuCoin's `XBT` (Bitcoin) stays as `XBT` in normalized symbols. Use `native_symbol` on `/info` to round-trip back to the upstream form.

### No public liquidations or long/short ratio

KuCoin does not expose a public liquidation history endpoint (only a private `LiquidationWarning` channel for the authenticated account). It also does not expose a public long/short account ratio. Both `liquidations` and `long_short_ratio` are `False` for REST and WS across every market type on the capability map. Calling those routes returns HTTP 501.

### Open interest retention and `value_usd`

KuCoin's open-interest history endpoint accepts `5min`, `15min`, `30min`, `1hour`, `4hour`, `1day` periods. Sub-hour data has roughly 7 days of retention; daily data extends to roughly 70 days. Queries with `start` older than the retention return an empty list. KuCoin does not publish a USD notional, so `value_usd` is always `0`.

### Mark price funding fields

The `/api/v1/mark-price/{symbol}/current` endpoint only returns mark and index. The router fetches `/api/v1/contracts/{symbol}` as a side request to populate `funding_rate` and `next_funding_time` on the `MarkPrice` model. If that side request fails, both fields default to `0`.

### Futures `qty` and `volume` are in contracts

For futures (linear and inverse), `qty` and `volume` are reported in contracts, not the base asset. Each instrument has a `multiplier` field on the upstream contract spec; the router does not convert. For `XBTUSDTM`, 1 contract represents 0.001 XBT; for `XBTUSDM`, 1 contract represents 1 USD. Consult KuCoin's contracts endpoint for per-instrument multipliers. Spot reports `qty` and `volume` in the base asset directly.

<br>
<br>

## OKX

### Symbol format

OKX uses dash-separated symbols upstream: `BTC-USDT` for spot, `BTC-USDT-SWAP` for linear perpetuals, `BTC-USD-SWAP` for inverse perpetuals. The router normalizes these to bare pairs (`BTCUSDT`, `BTCUSD`). Use `native_symbol` on `/info` responses if you need the raw upstream form.

### Recency vs. deep history on candles

OKX exposes a "live" candles endpoint with shallow recency (~1440 most-recent bars) and a "history" endpoint with deeper history but a smaller per-request page size. The router selects between them automatically based on the requested `start` and `limit`. Behavior is uniform from the caller's side; the only effect is on how many round trips a large historical pull takes.

### Open interest

`open_interest` is in coin units, with `value_usd` populated from upstream.

Supported periods: `5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D`. **`8H` is not supported** by the upstream and is excluded from the capability map; sending it returns an OKX error.

Retention is finite even within supported periods: roughly ~8 hours of 5m data, ~4 days of 1h data, lengthening for coarser periods. Queries with `start` older than the retention return an empty list.

### Long/short ratio is per currency

OKX's long/short ratio endpoint accepts a currency (`ccy`) parameter, not an instrument id. The router extracts the base currency from the requested symbol (`BTCUSDT` becomes `BTC`). The returned ratio aggregates across all OKX contracts denominated in that currency, so `BTCUSDT` and `BTCUSD` queries return the same numbers.

Supported periods are `5m, 1H, 1D` only. 5m data retains roughly the most recent two days; querying further back returns an empty list. The `ratio` field is the authoritative upstream value. `long_account = ratio / (ratio + 1)` and `short_account = 1 / (ratio + 1)` are derived under the assumption that the two halves sum to 1; treat them as convenience fields, not as separate upstream measurements.

### Liquidations stream

The OKX liquidations WebSocket channel broadcasts events for every SWAP on the exchange. The router filters server-side messages down to your subscribed symbol. Low-activity contracts may have quiet windows where no events arrive.
