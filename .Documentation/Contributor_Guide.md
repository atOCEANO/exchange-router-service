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
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <b>Contributor Guide</b>
</sub>

<br>
<br>
<br>
<br>

## Contributor Guide

The router is extended through isolated exchange adapters. Contributions should focus on adding or improving adapters. The core routing logic should not need to change.

<br>

### Service Lifecycle

The router uses a FastAPI lifespan manager (`@asynccontextmanager`) to handle startup and shutdown.

- **Startup:** The loader iterates over the `EXCHANGE_REGISTRY`, initializes connection pools, and registers adapters.
- **Shutdown:** The manager intercepts termination signals, closes all active WebSocket tasks cleanly, and releases adapter-level resources.

<br>

### Adapter Implementation

All adapters must inherit from `BaseExchange` in `src/exchanges/base.py`. The following checklist covers everything a compliant adapter needs to satisfy before it can be merged:

- [ ] **Capability Mapping:** Implement `get_capabilities()` returning the full list of supported REST routes and WebSocket channels.
- [ ] **Data Normalization:** Map all raw upstream JSON payloads to the Pydantic models in `src/models.py`.
- [ ] **Market Routing:** Handle `spot`, `linear`, and `inverse` market types, including any subdomain or parameter differences between them.
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

<br>

### Data Normalization

All data returned by an adapter must go through the Pydantic models in `src/models.py` before reaching the routing layer. Raw dictionaries are not accepted.

- **Validation:** Unmapped or malformed fields cause an immediate validation error.
- **Schema stability:** The router and downstream clients depend on these models being consistent across adapters.
- **Type safety:** This keeps price, volume, and quantity precision consistent between exchanges.

Before integrating with the router, validate normalization locally by instantiating your adapter directly in a Python REPL and confirming that all network responses parse through the relevant models without errors.

<br>

### Testing

Adapter compliance is validated via `tests/verify_routes.py`. All endpoints must pass before submitting a Pull Request.

```bash
# Point the test suite at the running router (default port 8040)
export API_URL=http://localhost:8040

# Run the suite
python tests/verify_routes.py
```

<br>

### Code Standards

* **Async I/O:** All network calls must use `httpx` or `websockets` in an async context.
* **No raw dicts:** Every response must be validated through a Pydantic model before being returned to the router. Returning a raw dictionary will cause a 500 error in the core service.
* **Lock hygiene:** Respect the `asyncio.Lock` pattern on each adapter to ensure rate limit state is managed correctly for the host IP.
