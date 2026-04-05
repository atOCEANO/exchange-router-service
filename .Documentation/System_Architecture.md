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
  <b>System Architecture</b> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## System Architecture

### Core Design

The router is built around an abstract base contract (`BaseExchange`). Each exchange is an isolated adapter that implements this contract. The core routing logic never needs to know which exchange it is talking to.

Adapters are discovered automatically. On startup, the loader in `src/exchanges/__init__.py` scans the `src/exchanges/` directory, finds every valid `BaseExchange` subclass, instantiates it, and registers it in the `EXCHANGE_REGISTRY`. Adding a new exchange is a matter of dropping a compliant adapter into that directory and restarting the service.

<br>

### Request Lifecycle

Every request follows the same path, regardless of exchange or data type:

`Client -> FastAPI Route -> Adapter -> Upstream Exchange -> Normalization -> Response`

1. **Client request:** Inbound REST or WebSocket call hits a FastAPI route.
2. **Adapter lookup:** The router resolves the exchange name to the registered adapter instance.
3. **Upstream call:** The adapter makes an async request to the exchange API.
4. **Normalization:** The raw JSON response is mapped to a Pydantic model defined in `src/models.py`.
5. **Response:** The validated model is returned to the client. Raw dicts are never passed through.

<br>

### Adapter Integration

Adding a new exchange is simple:

1. Create a directory under `src/exchanges/` named after the exchange (e.g., `src/exchanges/kraken/`).
2. Add an `adapter.py` file with a class that inherits from `src.exchanges.base.BaseExchange`.
3. Add an `__init__.py` that imports the adapter class (e.g., `from .adapter import KrakenAdapter`). The auto-loader uses this import to discover the class.
4. Implement the required abstract methods for REST endpoints and WebSocket streams.
5. Restart the service. The auto-loader picks up the new adapter and registers it automatically.

No changes to routes, configuration, or any other part of the codebase are needed.

<br>

### Rate Limiting

The router manages request weights per adapter to avoid upstream IP bans caused by concurrent local processes hitting the same exchange.

Each adapter holds a dedicated `asyncio.Lock()`. When multiple async tasks target the same exchange simultaneously, the lock forces them to queue if a rate limit threshold is approaching.

#### Proactive and Reactive Protection

The router uses a two-layer approach via shared `_backoff_until` timestamps:

- Every response is inspected for rate limit headers (e.g., `X-Bapi-Limit-Status`, `x-mbx-used-weight`). If the remaining API weight drops below a threshold, the adapter sets a backoff timestamp and stalls pending tasks before any upstream rejection occurs.
- If the exchange returns a `429 Too Many Requests` or `418 IP Ban`, the adapter reads the `Retry-After` header, acquires the lock, sets the backoff timestamp, and puts all pending tasks to sleep until the window clears.

<br>

### Deployment Notes

Each router instance manages rate limits in memory, scoped to its own process. This works correctly when a single container handles all traffic to a given exchange IP.

If you deploy multiple router containers behind a shared load balancer, each container has its own independent rate limit state. Because exchanges enforce limits per IP, and the load balancer masks individual container IPs, the containers cannot coordinate and will collectively exceed the limit. For multi-instance deployments, use IP-sticky routing so each exchange is always handled by the same container.
