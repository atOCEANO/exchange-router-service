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

Behaviors that are specific to one exchange and that callers should be aware of: symbol translation rules the router applies on your behalf, fields the upstream does not expose, retention windows that affect what historical queries can return, and unit semantics that survive normalization. The shared schema is uniform; the data behind it is not always.

The router never silently substitutes data. If a field is unavailable upstream it returns `0` or an empty string; if a historical window is past the upstream's retention, it returns an empty list. Every quirk that could surprise a caller is documented below.

<br>
<br>

---

### Shared Conventions

#### Symbol normalization

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

#### Perpetuals only on contract markets

For `linear` and `inverse` market types, both `/markets` and `/info` return perpetual contracts only. Quarterly and dated futures are excluded by every adapter. Spot markets are unaffected and return every active pair.

#### Empty fields mean "upstream did not provide"

`SymbolInfo` fields like `min_notional`, `max_qty`, `base_asset`, `quote_asset` default to `0` or `""` when the upstream does not return them. They are never invented or computed. See [Field coverage](#field-coverage) below for which fields are missing on which exchange.

#### Pagination is always backward-walking

Every endpoint that accepts `start` treats it as an inclusive backward-walking upper bound: `?start=X&limit=N` returns up to N records with `ts <= X`, sorted ascending. Omit `start` for the most recent N. The response body is always sorted ascending regardless of which adapter served it. For the full pagination contract, see [API Reference](API_Reference.md#pagination-semantics).

The router maps this universal contract to each upstream's native parameters. Where the upstream walks forward only (Kraken), the adapter computes a synthetic forward start, fetches a window, and truncates. The user-visible behavior is identical; backward-anchored requests on Kraken are slightly slower than on the other exchanges.

<br>

#### Unit semantics

The schema names match across exchanges, but the units behind certain fields do not. The same numeric value of `qty`, `volume`, or `open_interest` can mean different things depending on the exchange and market type. Before comparing values across markets, consult the upstream contract specification or normalize through `price`.

The pitfalls that bite most often:

- **Binance COIN-M (inverse) `qty` and `volume` are reported in contracts, not the base asset.** For `BTCUSD_PERP`, 1 contract represents 100 USD of notional. The router does not convert. Multiply by the contract multiplier (and divide by `price`) if you need a base-asset volume; consult Binance's COIN-M contract spec for each instrument's multiplier.
- **Bybit `open_interest` units depend on contract type.** For linear contracts the field is in the base asset; for inverse contracts it is in USD. Bybit does not provide a USD notional, so `value_usd` is always `0` on Bybit.
- **Kraken futures `open_interest` is a raw numeric value with no USD notional.** `value_usd` is always `0` on Kraken.
- **OKX `open_interest`** is in coin units, with `value_usd` populated from upstream.

Trade `side` is always normalized to `"buy"` or `"sell"` (taker side). Timestamps are always Unix milliseconds. Orderbook levels are always `[price, size]` tuples in their native units.

<br>

#### Field coverage

A reference for which `SymbolInfo` and `OpenInterest` fields are populated per exchange. Empty cells mean the field defaults to `0` or `""`.

| Field | Binance | Bybit | Kraken | OKX |
| :--- | :---: | :---: | :---: | :---: |
| `OpenInterest.value_usd` | yes | no | no | yes |
| `SymbolInfo.min_notional` (spot) | yes | yes | yes | no |
| `SymbolInfo.min_notional` (linear/inverse) | yes | yes | no | no |
| `SymbolInfo.max_qty` (spot) | yes | yes | no | yes |
| `SymbolInfo.max_qty` (linear/inverse) | yes | yes | yes | yes |
| `SymbolInfo.base_asset` / `quote_asset` (inverse) | yes | yes | no (`PI_*`) | yes (if `uly` set) |

<br>
<br>

---

### Binance

#### Symbol format

Binance COIN-M (inverse) perpetuals carry a `_PERP` suffix in the exchange API (`BTCUSD_PERP`, `ETHUSD_PERP`). The router normalizes them to bare pairs (`BTCUSD`, `ETHUSD`). Use the `native_symbol` field on `/info` responses if you need to cross-reference back to Binance's docs.

#### Liquidations REST is disabled

The public REST endpoints for forced liquidation history (`/fapi/v1/allForceOrders` on USDM, `/dapi/v1/allForceOrders` on COINM) were deprecated by Binance in April 2021 and no longer return data without authentication. The capability map reports `liquidations.rest: False` for both linear and inverse. The WebSocket stream (`forceOrder@arr`) remains publicly available and is exposed as `liquidations.ws: True`.

<br>
<br>

---

### Bybit

#### Symbol format

Bybit uses bare-pair symbols natively (`BTCUSDT`, `ETHUSDT`) on both spot and contracts. No translation is applied; `native_symbol` and `symbol` are identical.

#### IP bans

Bybit returns HTTP 403 (not 429) on persistent rate-limit violations. These are IP bans on the order of 10 minutes. The router fails the triggering request immediately and rejects subsequent requests until the ban window has passed. Soft per-endpoint limits are handled internally with brief backoff and retry.

#### Open interest unit and `value_usd`

The Bybit open-interest endpoint returns only a raw `openInterest` number. The unit depends on contract type: USD for inverse, base asset for linear. Bybit does not provide a USD notional, so `value_usd` is always `0` on Bybit. See [Unit semantics](#unit-semantics).

#### Long/short ratio derivation

The Bybit account-ratio endpoint returns `buyRatio` and `sellRatio` directly. The router maps `long_account = buyRatio` and `short_account = sellRatio`, and computes `ratio = buyRatio / sellRatio`. This is the inverse of the OKX convention (where the upstream sends a single `ratio` and the router derives the accounts).

#### Long/short ratio ignores `start_time`

Bybit's account-ratio endpoint does not accept a start timestamp. When `start_time` is provided, the router logs a warning and returns the most recent data. This is a known limitation; be aware when comparing time-anchored requests across exchanges.

<br>
<br>

---

### Kraken

#### XBT to BTC translation

Kraken's spot REST API returns ticker symbols using legacy asset names (`XBT/USDT`, `XDG/USDT`). The v2 WebSocket API requires the modern names (`BTC/USDT`, `DOGE/USDT`). The router translates internally; normalized symbols always use the modern form. You do not need to think about this when using the router.

#### Open interest `value_usd`

Kraken does not provide a USD notional for OI. `value_usd` is always `0`. See [Unit semantics](#unit-semantics).

#### Candle history limits

The Kraken spot OHLC endpoint has a hard ceiling of 720 candles per request with no pagination support. Requests above this limit silently return only the most recent 720. Linear and inverse candles use the Futures chart API and paginate normally.

#### Backward-anchored requests

Kraken's upstream parameters are forward-only. The router emulates the universal `?start=X` contract by computing a synthetic forward start, fetching a window, then truncating. User-visible behavior is identical to other exchanges; backward-anchored requests on Kraken are slightly slower than the native backward walks the other adapters use.

#### Inverse `/info` metadata

The Kraken Futures instruments endpoint does not include `baseCurrency` or `quoteCurrency` for inverse perpetuals (`PI_*`). The `underlying` field present in those responses is a reference-rate identifier (e.g. `rr_xbtusd`), not a currency code. As a result, `base_asset` and `quote_asset` are empty strings for Kraken inverse symbols in `/info` responses. Linear perpetuals (`PF_*`) include these fields and return them correctly.

<br>
<br>

---

### OKX

#### Symbol format

OKX uses dash-separated symbols upstream: `BTC-USDT` for spot, `BTC-USDT-SWAP` for linear perpetuals, `BTC-USD-SWAP` for inverse perpetuals. The router normalizes these to bare pairs (`BTCUSDT`, `BTCUSD`). Use `native_symbol` on `/info` responses if you need the raw upstream form.

#### Inverse base/quote derivation

For SWAP instruments (linear and inverse), OKX returns empty `baseCcy`/`quoteCcy`. The router derives `base_asset` and `quote_asset` from the upstream `uly` field. If `uly` is missing, both fields are returned as empty strings.

#### Recency vs. deep history on candles

OKX exposes a "live" candles endpoint with shallow recency (~1440 most-recent bars) and a "history" endpoint with deeper history but a smaller per-request page size. The router selects between them automatically based on the requested `start` and `limit`. Behavior is uniform from the caller's side; the only effect is on how many round trips a large historical pull takes.

#### Open interest

`open_interest` is in coin units, with `value_usd` populated. Unlike Bybit and Kraken, OKX provides both fields directly.

Supported periods: `5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D`. **`8H` is not supported** by the upstream and is excluded from the capability map; sending it returns an OKX error.

Retention is finite even within supported periods: roughly ~8 hours of 5m data, ~4 days of 1h data, lengthening for coarser periods. Queries with `start` older than the retention return an empty list.

#### Long/short ratio is per currency

OKX's long/short ratio endpoint accepts a currency (`ccy`) parameter, not an instrument id. The router extracts the base currency from the requested symbol (`BTCUSDT` becomes `BTC`). The returned ratio aggregates across all OKX contracts denominated in that currency, so `BTCUSDT` and `BTCUSD` queries return the same numbers.

Supported periods are `5m, 1H, 1D` only. 5m data retains roughly the most recent two days; querying further back returns an empty list. The `ratio` field is the authoritative upstream value. `long_account = ratio / (ratio + 1)` and `short_account = 1 / (ratio + 1)` are derived under the assumption that the two halves sum to 1; treat them as convenience fields, not as separate upstream measurements.

#### Liquidations stream

The OKX liquidations WebSocket channel broadcasts events for every SWAP on the exchange. The router filters server-side messages down to your subscribed symbol. Low-activity contracts may have quiet windows where no events arrive.
