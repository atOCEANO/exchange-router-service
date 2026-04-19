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
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <b>Contributor Guide</b>
</sub>

<br>
<br>
<br>
<br>

## Contributor Guide

The router is extended through isolated exchange adapters. Almost every contribution lives in `src/exchanges/<name>/` and leaves the routing core untouched. This guide walks through the work in roughly the order you will actually do it: set up a local loop, build the adapter, satisfy the contract, follow the code standards, and run the test suite before opening a PR.

<br>

### Local Development

The service ships as a Docker container, but iterating on an adapter through `docker-compose up --build` on every change is slow. For day-to-day work, run the service directly:

```bash
# Create and activate a venv (once)
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the service with autoreload
uvicorn src.main:app --reload --port 8040
```

The `--reload` flag restarts the worker whenever a file under `src/` changes. Pair it with the test suite running in another terminal for a tight edit-test loop.

When something breaks, it helps to bypass FastAPI entirely. Instantiate the adapter in a Python REPL and call its methods directly. The stack trace is cleaner, and you can poke at intermediate state without routing a request end to end.

<br>

### Adapter Implementation

All adapters must inherit from `BaseExchange` in `src/exchanges/base.py`. The following checklist covers everything a compliant adapter needs to satisfy before it can be merged:

- [ ] **Capability Mapping:** Implement `get_capabilities()` returning the full list of supported REST routes and WebSocket channels.
- [ ] **Data Normalization:** Map all raw upstream JSON payloads to the Pydantic models in `src/models.py`.
- [ ] **Market Routing:** Handle `spot`, `linear`, and `inverse` market types, including any subdomain or parameter differences between them.
- [ ] **Symbol Normalization:** Implement `get_model_symbol(api_symbol, market_type)` to translate raw exchange symbols to normalized form (e.g. `BTCUSD_PERP` → `BTCUSDT`), and `get_api_symbol(symbol, market_type)` to reverse the translation when constructing upstream requests. Normalized symbols must be bare pairs with no suffixes.
- [ ] **Perpetuals Filter:** For `linear` and `inverse` markets, `get_exchange_info` must exclude dated and quarterly contracts. Only perpetual instruments should appear in `/markets` and `/info`.
- [ ] **`native_symbol` Field:** Populate `native_symbol` on every `SymbolInfo` object with the raw exchange symbol before normalization.
- [ ] **Registry Registration:** Add an `__init__.py` that imports the adapter class (e.g., `from .adapter import KrakenAdapter`). The auto-loader relies on this import to discover subclasses of `BaseExchange`.

<br>

### Capabilities Contract

Every adapter must implement `get_capabilities()`, which returns a dict describing what the adapter supports. The router exposes this at `GET /{exchange}/capabilities`, and clients use it to know what they can call before making requests.

The structure follows this schema:

```python
{
    "name": "exchange_name",
    "markets": {
        MarketType.SPOT: {
            "candles":          {"rest": True,  "ws": False, "intervals": ["1m", "5m", "1h", ...]},
            "ticker":           {"rest": True,  "ws": True},
            "book_ticker":      {"rest": True,  "ws": True},
            "trades":           {"rest": True,  "ws": True},
            "agg_trades":       {"rest": True,  "ws": True},
            "orderbook":        {"rest": True,  "ws": True},
            "mark_price":       {"rest": False, "ws": False},
            "open_interest":    {"rest": False, "ws": False},
            "funding_rate":     {"rest": False, "ws": False},
            "long_short_ratio": {"rest": False, "ws": False},
            "liquidations":     {"rest": False, "ws": False},
        },
        MarketType.LINEAR: {
            "candles":          {"rest": True, "ws": False, "intervals": ["1m", "5m", "1h", ...]},
            "ticker":           {"rest": True, "ws": True},
            "book_ticker":      {"rest": True, "ws": True},
            "trades":           {"rest": True, "ws": True},
            "agg_trades":       {"rest": True, "ws": True},
            "orderbook":        {"rest": True, "ws": True},
            "mark_price":       {"rest": True, "ws": True},
            "open_interest":    {"rest": True, "ws": False, "intervals": ["5m", "1h", ...]},
            "funding_rate":     {"rest": True, "ws": False},
            "long_short_ratio": {"rest": True, "ws": False, "intervals": ["5m", "1h", ...]},
            "liquidations":     {"rest": True, "ws": True},
        },
        # inverse follows the same structure as linear
    }
}
```

Every feature uses `{"rest": bool, "ws": bool}`. Features with interval-based queries (candles, open interest, long/short ratio) include an additional `"intervals"` list. Unsupported market types should be omitted from the `markets` dict entirely.

This split matters because REST and WS support do not always line up. For example, an exchange may stream liquidations over WebSocket but not expose a REST history endpoint. The test suite uses these flags independently to decide which tests to run.

The canonical implementation lives in `src/exchanges/binance/adapter.py` (`BinanceAdapter.get_capabilities`). When adding a new adapter, copy it as a starting point rather than reconstructing this schema by hand.

<br>

### Data Normalization

All data returned by an adapter must go through the Pydantic models in `src/models.py` before reaching the routing layer. Raw dictionaries are not accepted.

- **Validation:** Unmapped or malformed fields cause an immediate validation error.
- **Schema stability:** The router and downstream clients depend on these models being consistent across adapters.
- **Type safety:** This keeps price, volume, and quantity precision consistent between exchanges.

Before integrating with the router, validate normalization locally by instantiating your adapter directly in a Python REPL and confirming that all network responses parse through the relevant models without errors.

#### When the Models Don't Fit

If an upstream response carries a field that no existing model captures, you have three options, in order of preference:

1. **Drop the field.** Most of the time the extra data is not relevant to the router's contract. If it is noise, leave it out of the normalized output.
2. **Add an optional field to an existing model.** If the field is conceptually shared across exchanges and just happens to be missing from yours, extend the model with an `Optional[...]` field and default it to `None`. Other adapters can start populating it later without breaking the schema.
3. **Add a new model.** If the data type is genuinely new (new market type, new derivative), propose a new Pydantic model in `src/models.py` as part of the PR.

Do not add adapter-specific fields under a generic name, and do not return raw dicts as an escape hatch. The normalization contract is the whole point of the router.

<br>

### Code Standards

* **Async I/O.** All network calls must use `httpx` or `websockets` in an async context. A blocking call inside an adapter stalls the entire event loop.
* **No raw dicts across the boundary.** Adapter methods must return Pydantic models from `src/models.py`. If you find yourself wanting to return a `dict`, add a model instead.
* **Fail with `ValueError` or `NotImplementedError`.** These are the two exception types the route layer knows how to translate into HTTP responses. Use `ValueError` for bad input and upstream validation failures, `NotImplementedError` for features the adapter does not support on a given market type.
* **No hand-rolled retry logic at the call site.** `_make_request` (or the adapter's equivalent) handles retries and backoff. Per-call retry loops fight the rate limiter.
* **Logging via `logging.getLogger("<adapter>_adapter")`.** Keep each adapter's logs isolated so they can be filtered independently.

For exchange-specific behaviors (rate limit tiers, symbol translation quirks, API version notes) see [Exchange Notes](Exchange_Notes.md).

<br>

### Testing

Adapter compliance is validated via `tests/verify_routes.py`. The suite reads `/{exchange}/capabilities` and runs only the features the adapter claims to support, so an honest capabilities map is the difference between a passing and a skipped test. All declared endpoints must pass before submitting a Pull Request.

Dishonesty in either direction fails loudly. Claim `True` for a feature the adapter does not implement and the 501 response surfaces as a test failure, not a skip. Claim `False` for a feature that actually works and the test simply never runs, which shows up as suspiciously thin coverage for your adapter in the results table. Neither flavor passes review.

The suite also runs a pagination sanity check against candles: it fetches 1500 bars and asserts timestamps are strictly ascending. This is the one place where normalization correctness is tested beyond "did the response parse," and it tends to catch adapters that mix up `start` and `end` semantics between exchanges.

WebSocket tests open a connection and listen for the declared duration (default 60 seconds). Channels like `ticker` and `trades` produce messages immediately on most pairs, but low-frequency channels such as `liquidations` may produce nothing in a quiet 60-second window. This is expected and is not a test failure. When running a thorough validation — for example, before a release or after a significant adapter change — increase the duration by setting `WS_TEST_DURATION` before running the suite. The organization's own validation runs use a longer window specifically to exercise these channels.

```bash
# Point the test suite at the running router (default port 8040)
export API_URL=http://localhost:8040

# Run the suite
python tests/verify_routes.py

# Run with extended WebSocket observation window (seconds)
WS_TEST_DURATION=300 python tests/verify_routes.py
```

<br>

### Service Lifecycle

For reference, the router uses a FastAPI lifespan manager (`@asynccontextmanager`) to handle startup and shutdown. On startup, the auto-loader walks `src/exchanges/`, instantiates every `BaseExchange` subclass it finds, and registers it in `EXCHANGE_REGISTRY`. On shutdown, the manager closes all active WebSocket tasks cleanly and calls `shutdown()` on each adapter to release connection pools and any other resources the adapter holds.

Contributors rarely need to touch this layer. The only thing an adapter owes the lifecycle is a correct `shutdown()` implementation that closes every client it opened in `__init__`.
