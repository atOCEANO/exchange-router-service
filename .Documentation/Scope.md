<h1>OCEΛNO <small><code>exchange-router-service</code></small></h1>


<div style="padding-top: 0px;">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9+-blue.svg" alt="Python 3.9+" /></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.123.0-05998b.svg?logo=fastapi&logoColor=white" alt="FastAPI" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
</div>

<sub>
  <a href="../README.md">Introduction</a> &nbsp;•&nbsp; 
  <b>Scope</b> &nbsp;•&nbsp; 
  <a href="API_Reference.md">API Reference</a> &nbsp;•&nbsp; 
  <a href="Python_SDK.md">Python SDK</a> &nbsp;•&nbsp; 
  <a href="Troubleshooting.md">Troubleshooting</a> &nbsp;•&nbsp; 
  <a href="Exchange_Notes.md">Exchange Notes</a> &nbsp;•&nbsp; 
  <a href="System_Architecture.md">System Architecture</a> &nbsp;•&nbsp; 
  <a href="Capabilities_Contract.md">Capabilities Contract</a> &nbsp;•&nbsp; 
  <a href="Auditor_Guide.md">Auditor Guide</a> &nbsp;•&nbsp; 
  <a href="Contributor_Guide.md">Contributor Guide</a>
</sub>

<br>
<br>
<br>
<br>

## Scope

Public market data from multiple crypto exchanges through one async API, normalized to a shared schema. No orders, no auth, no persistence.

<br>
<br>

## What the router does

Normalize public market data from every registered exchange adapter into one async API. The currently shipped adapters live under `src/exchanges/` (see [Supported Exchanges](../README.md#supported-exchanges) in the README for the canonical list). REST and WebSocket are served on the same port and use the same wire schema, so a client written against one exchange works against the others with a single path change. The wire format is documented in the [API Reference](API_Reference.md).

The router translates between each upstream's native field names, units, funding conventions, and pagination semantics, and surfaces a uniform record shape regardless of which exchange served the request. It also handles the parts every integration needs: pagination for large historical pulls, request-weight throttling and back-off to prevent IP bans, and persistent connection management for WebSocket streams.

<br>
<br>

## What the router does not do

* **No orders.** The router is read-only. There are no order-placement, order-cancellation, position-management, or transfer endpoints.
* **No account access.** No authentication, no API keys, no account-state queries. Every endpoint is public market data.
* **No persistence.** No database, no cache, no on-disk state. Every request hits upstream. A restart loses only in-flight requests and WebSocket sessions.
* **No historical archive.** Historical data is always fetched from upstream on demand, bounded by each venue's own retention. The router never stores rows past the response.
* **No inbound rate limiting.** Outbound rate-limit respect (to keep the router from being banned by upstreams) is enforced per adapter. Inbound limits, quotas, or per-client throttling are not provided; that belongs in a reverse proxy.
* **No observability.** Logs go to stdout via Python `logging`. Metrics and traces are not emitted. Wrap the service at the edge if you need them.
* **No invented data.** When an upstream omits a field, the router surfaces the gap (typically `null`, `0`, or an empty list), never a derived or fabricated value. See [Data fidelity rule](#data-fidelity-rule) below.

<br>
<br>

## Deployment assumptions

* **One router instance per upstream IP.** Rate-limit state is in-memory per adapter. Running two router instances behind a load balancer that share an outbound IP doubles the apparent request weight and risks an IP ban. If you need horizontal scaling, shard exchanges across instances rather than replicating them.
* **Localhost or trusted network.** No TLS termination, no authentication, no inbound rate limiting, no origin enforcement. `allow_origins=["*"]` is set so any local client works during development. For external exposure, put the router behind a reverse proxy that adds TLS, an allowlist or origin check, and an inbound rate limit. Per-client quotas and audit logging belong in that proxy layer, not in the router.
* **Single immutable image.** Configuration is in code, per adapter. The only deploy-time knob is the host port mapped via `EXCHANGE_ROUTER_SERVICE_PORT` in `.env`. Changes to adapter behaviour, timeouts, or upstream URLs are code edits and a container rebuild, not a config switch.
* **Public market data only.** A compromised router cannot leak private information or move funds. The realistic abuse surface is being driven as an open proxy that hammers upstream APIs from your IP. The same proxy layer that adds inbound limits handles this.

<br>
<br>

## Data fidelity rule

The router never invents data. When upstream omits a field, the router surfaces the gap honestly:

* Missing string fields come back as `""`.
* Missing numeric fields come back as `0` or `null` depending on the model.
* Missing list responses come back as `[]`, not as a synthesized "best guess".
* Queries past upstream retention return an empty list.

Consumers reading a `0` on a rarely-populated field should not interpret it as "the exchange said zero". It usually means the exchange did not return the field at all. Per-exchange specifics for which fields tend to be missing are in [Exchange Notes](Exchange_Notes.md).

The same rule applies to derived values such as `usd` on `open_interest`: on linear markets the value is joined from a same-period candle's close, and if the candle join misses, `usd` comes back `null` with `native` still populated. The router does not substitute a stale close, a ticker price, or a back-of-envelope estimate.

<br>
<br>

## When the router is not the right fit

* You need authenticated endpoints (orders, balances, deposits). Use the upstream SDK directly.
* You need long-term historical storage. Build a separate ingestion pipeline; the router fetches on demand but does not retain.
* You need sub-millisecond latency. The router adds a normalization layer and a process boundary; co-locate with the exchange or use a direct client if microseconds matter.
* You need to expose a public service to untrusted clients. The router has no auth and no inbound throttle. Either wrap in a proxy that supplies both, or pick a service designed for that role.
