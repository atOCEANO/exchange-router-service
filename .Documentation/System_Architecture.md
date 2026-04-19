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
  <b>System Architecture</b> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## System Architecture

The sections below cover the router's core contract, the two request paths (REST and WebSocket), how rate limiting and failures are handled, and the deployment and security constraints that shape how the service should be operated in practice.

<br>

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

### WebSocket Lifecycle

WebSocket streams do not follow the same path as REST. The `StreamManager` in `src/stream_manager.py` sits between clients and the adapter's streaming methods.

When a client subscribes to a `(channel, symbol)` tuple on an exchange, the manager builds a key of the form `{exchange}:{market_type}:{channel}:{symbol}` and checks whether an upstream task already exists for it.

1. **First subscriber.** The manager starts an async task that opens a single upstream WebSocket via `adapter.stream_*(market_type, symbol)`. Every message produced by the adapter is fanned out to all local clients registered under the same key.
2. **Additional subscribers.** New clients attach to the existing task. No new upstream connection is opened, which keeps the router well under per-IP connection limits on exchanges that enforce them.
3. **Last subscriber leaves.** The manager cancels the upstream task and releases its resources. The next subscriber to the same tuple starts a fresh connection.
4. **Upstream failure.** If the adapter's stream terminates (network drop, exchange-side reset), the manager closes every attached client with code `1011` and clears the entry. Clients are expected to reconnect and re-subscribe.

This is also why re-subscribing on the same connection is not supported. Each connection is bound to one stream task, and the router has no mechanism for moving a client between tasks.

<br>

### Rate Limiting

The router manages request weight per adapter to avoid upstream IP bans. Each adapter holds a dedicated `asyncio.Lock()` and a shared `_backoff_until` timestamp, combining them in two layers:

- **Proactive.** Every response is inspected for rate limit headers (`X-Bapi-Limit-Status`, `x-mbx-used-weight`, and similar). If remaining weight drops below a threshold, the adapter sets a backoff timestamp and stalls pending tasks before the upstream starts rejecting anything.
- **Reactive.** Exchange-specific: Bybit uses HTTP 403 for IP bans (not 429), which sets a 10-minute backoff and fails the triggering request immediately. A `retCode: 10006` inside a 200 response signals a soft per-endpoint limit and triggers a short backoff with a retry. Binance and Kraken use standard HTTP 429. All cases update the shared `_backoff_until` timestamp.

If the remaining backoff exceeds 30 seconds when a request arrives, the adapter fails it immediately with a clear error rather than making the caller wait. All requests to the same exchange share a single budget and serialize behind the same lock.

For detailed per-exchange behavior, see the [Exchange Notes](Exchange_Notes.md).

<br>

### Error Handling

The router normalizes failures into a small set of HTTP responses. It helps to separate what adapters raise from what the route layer emits, because adapter authors only need to worry about the first list.

Adapters raise exactly two exception types:

* **`ValueError`** for bad input or upstream validation failures (unknown symbol, out-of-range limit, malformed interval, adapter-side parameter rejections). A global exception handler in `main.py` converts these into `400 Bad Request`, preserving the message in `detail`.
* **`NotImplementedError`** when the adapter does not implement a method for a given market type. The base class raises this by default, and the route layer catches it and returns `501 Not Implemented`.

The route layer adds two more responses that adapters never raise themselves:

* **`404 Not Found`** when the exchange name is not in `EXCHANGE_REGISTRY`. Raised directly by `validate_request` before the adapter is ever called.
* **`400 Bad Request`** when the market type is known but the adapter does not declare support for it. Also raised by `validate_request`, checked against each adapter's `supported_market_types` list.

Two more conditions do not correspond to exception types at all:

* **Upstream HTTP failures** (5xx from the exchange, connection errors, timeouts) are retried with backoff inside `_make_request`. If retries are exhausted, the request surfaces as `500 Internal Server Error`.
* **Upstream rate limiting** (429 or 418, or proactive detection via response headers) causes the adapter to pause all in-flight tasks and wait for the declared backoff window. The client-facing request is not failed, only delayed.

The `detail` field in error responses always carries the underlying exception message, whether it came from the adapter or the upstream exchange. Nothing is rewritten or swallowed.

<br>

### Deployment Notes

The router is designed to run as a single container per exchange IP. A few consequences follow from that.

* **Rate limit state is in-memory.** Each router instance tracks its own backoff timestamps and per-adapter locks. Running two router instances behind a load balancer that both hit the same exchange from the same IP will double-count request weight and risk an IP ban. If you need horizontal scaling, shard exchanges across instances rather than replicating them.
* **No persistence.** The service holds no database, no cache, no on-disk state. Historical data is always pulled from upstream on demand. A restart loses only in-flight requests and WebSocket sessions.
* **No built-in observability.** The router logs to stdout via Python's `logging` module. Metrics and traces are not emitted. If you need them, wrap the service at the edge (reverse proxy, sidecar) or add them directly to the adapter layer.
* **CORS is open.** `allow_origins=["*"]` is set for local research convenience. If you expose the router beyond localhost, put it behind a proxy that enforces origin checks.

None of these are bugs. They are intentional tradeoffs that keep the service stateless and simple to restart. They should be understood before pointing real traffic at it.

<br>

### Security and Exposure

The router is designed to run on localhost or inside a trusted network. It does not ship with any of the pieces you would need to safely expose it to the public internet, and the intro's "stateless and keyless" framing is meant to narrow the threat model, not eliminate it.

Concretely, the service has no authentication, no rate limiting on the inbound side (only on upstream calls), no request signing, no TLS termination, and no origin enforcement. Anyone who can reach the port can issue any request the adapters support. Because every response is public market data pulled from public APIs, a compromised router cannot leak private information or move funds, but an exposed instance can still be abused as an open proxy to hammer upstream exchanges from your IP, which is how IP bans happen.

If you need to run the router outside a trusted network, put it behind a reverse proxy that adds the pieces the service deliberately omits: TLS, an allowlist or origin check, and an inbound rate limit tight enough that a single misbehaving client cannot exhaust the upstream budget the router is carefully managing on your behalf. A minimal nginx or Caddy config in front of the container is usually enough. Anything more ambitious (per-client quotas, audit logging, API keys) belongs in that proxy layer, not in the router itself.

One deployment pattern worth calling out: if you are running multiple Oceano services behind the same proxy, give the router its own subpath or hostname and keep the port private. The router's CORS wildcard means a browser app served from any origin can talk to it, which is useful during local research and dangerous in any shared environment.
