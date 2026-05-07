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
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <b>System Architecture</b> &nbsp;•&nbsp; 
  <a href="Validator_Guide.md">Validator Guide</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## System Architecture

The router's core contract, the REST and WebSocket request paths, rate-limit handling, error handling, and the deployment and security constraints that shape how the service should be operated.

<br>
<br>

## Core Design

The router is built around an abstract base contract (`BaseExchange`). Each exchange is an isolated adapter that implements it; the routing core never needs to know which exchange it is talking to. The loader in `src/exchanges/__init__.py` scans the directory at startup, instantiates every `BaseExchange` subclass it finds, and registers them in `EXCHANGE_REGISTRY`. Adding a new exchange is a matter of dropping a compliant adapter into that directory and restarting.

Before deploying outside localhost, see [Security and Exposure](#security-and-exposure).

<br>
<br>

## Request Lifecycle

Every REST request follows the same path:

`Client -> FastAPI Route -> Adapter -> Upstream Exchange -> Normalisation -> Response`

The route resolves the exchange name to its registered adapter, the adapter makes an async upstream call, and the raw JSON is mapped to a Pydantic model from `src/models.py` before returning. Raw dicts never cross the boundary.

<br>
<br>

## WebSocket Lifecycle

WebSocket streams do not follow the same path as REST. The `StreamManager` in `src/stream_manager.py` sits between clients and the adapter's streaming methods.

When a client subscribes to a `(channel, symbol)` tuple on an exchange, the manager builds a key of the form `{exchange}:{market_type}:{channel}:{symbol}` and checks whether an upstream task already exists for it.

1. **First subscriber.** The manager starts an async task that opens a single upstream WebSocket via `adapter.stream_*(market_type, symbol)`. Every message produced by the adapter is fanned out to all local clients registered under the same key.
2. **Additional subscribers.** New clients attach to the existing task. No new upstream connection is opened, which keeps the router well under per-IP connection limits on exchanges that enforce them.
3. **Last subscriber leaves.** The manager cancels the upstream task and releases its resources. The next subscriber to the same tuple starts a fresh connection.
4. **Upstream failure.** If the adapter's stream terminates (network drop, exchange-side reset), the manager closes every attached client with code `1011` and clears the entry. Clients are expected to reconnect and re-subscribe.

This is also why re-subscribing on the same connection is not supported. Each connection is bound to one stream task, and the router has no mechanism for moving a client between tasks.

<br>
<br>

## Rate Limiting

Each adapter holds a dedicated `asyncio.Lock()` and a shared `_backoff_until` timestamp. All requests to the same exchange share a single budget and serialise behind that lock. If the remaining backoff exceeds 30 seconds when a request arrives, the adapter fails the request immediately with a clear error rather than making the caller wait.

The user-facing patterns (header-driven proactive backoff, reactive backoff on rejection, proactive minimum-spacing) and per-exchange specifics live in [Exchange Notes → Rate-limit and ban protection](Exchange_Notes.md#rate-limit-and-ban-protection). The implementation details (header names, code paths, Kraken's layered retry strategy) live in [Contributor Guide → Rate limit headers and proactive backoff](Contributor_Guide.md#rate-limit-headers-and-proactive-backoff).

<br>
<br>

## Error Handling

The router normalizes failures into a small set of HTTP responses. It helps to separate what adapters raise from what the route layer emits, because adapter authors only need to worry about the first list.

Adapters raise three exception types:

* **`ValueError`** for bad input or upstream validation failures (unknown symbol, out-of-range limit, malformed interval, adapter-side parameter rejections, requests arriving while an active backoff window has more than 30 seconds remaining). A global exception handler in `main.py` converts these into `400 Bad Request`, preserving the message in `detail`. Kraken also uses `ValueError` for exhausted retries on throttling, so callers see `400` with an actionable message rather than an opaque 500.
* **`NotImplementedError`** when the adapter does not implement a method for a given market type. The base class raises this by default, and the route layer catches it and returns `501 Not Implemented`.
* **plain `Exception`** when retries on upstream HTTP failures (5xx, connection errors, timeouts) are exhausted on Binance, Bybit, KuCoin, or OKX. The message is `"Max retries exceeded for {url}"`. Not caught explicitly, so FastAPI's default handler returns `500 Internal Server Error`.

The route layer adds three more responses that adapters never raise themselves:

* **`404 Not Found`** when the exchange name is not in `EXCHANGE_REGISTRY`. Raised directly by `validate_request` before the adapter is ever called.
* **`400 Bad Request`** when the market type is known but the adapter does not declare support for it. Also raised by `validate_request`, checked against each adapter's `supported_market_types` list.
* **`422 Unprocessable Entity`** when a path or query parameter fails FastAPI's pydantic validation (for example, an unrecognised `market_type` enum value). Raised by FastAPI before the route handler runs.

One more condition does not correspond to an exception type at all:

* **Upstream rate limiting** (429 or 418, or proactive detection via response headers) causes the adapter to pause all in-flight tasks and wait for the declared backoff window. The client request is delayed, not rejected. When the wait exceeds 30 seconds, the adapter raises `ValueError` instead, which falls under the first bullet above.

The `detail` field in error responses always carries the underlying exception message, whether it came from the adapter or the upstream exchange. Nothing is rewritten or swallowed.

<br>
<br>

## Deployment Notes

The router is designed to run as a single container per exchange IP. A few consequences follow from that.

* **Rate limit state is in-memory.** Each router instance tracks its own backoff timestamps and per-adapter locks. Running two router instances behind a load balancer that both hit the same exchange from the same IP will double-count request weight and risk an IP ban. If you need horizontal scaling, shard exchanges across instances rather than replicating them.
* **No persistence.** The service holds no database, no cache, no on-disk state. Historical data is always pulled from upstream on demand. A restart loses only in-flight requests and WebSocket sessions.
* **No built-in observability.** The router logs to stdout via Python's `logging` module. Metrics and traces are not emitted. If you need them, wrap the service at the edge (reverse proxy, sidecar) or add them directly to the adapter layer.
* **CORS is open.** `allow_origins=["*"]` is set so any local client can talk to the router during development. If you expose the router beyond localhost, put it behind a proxy that enforces origin checks.

These are intentional constraints that keep the service stateless and straightforward to restart. Understand them before pointing real traffic at the service.

<br>
<br>

## Security and Exposure

The router is designed for localhost or a trusted network. It ships without authentication, inbound rate limiting (only outbound), request signing, TLS termination, or origin enforcement. Anyone who can reach the port can issue any request the adapters support.

Every response is public market data, so a compromised router cannot leak private information or move funds. The real risk is being abused as an open proxy: a misbehaving caller hammering upstream exchanges from your IP. That is how IP bans happen.

If you run the router outside a trusted network, put it behind a reverse proxy that adds TLS, an allowlist or origin check, and an inbound rate limit tight enough that one misbehaving client cannot exhaust the upstream budget. A minimal nginx or Caddy config is enough. Per-client quotas, audit logging, and API keys belong in that proxy layer, not in the router. If you run multiple Oceano services behind the same proxy, give the router its own subpath or hostname and keep the port private; CORS is wildcard-open by design for local development.
