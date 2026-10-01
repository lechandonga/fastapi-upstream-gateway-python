# FastAPI Upstream Gateway

A local-only request gateway built on FastAPI. Incoming requests to
`/gateway/...` are forwarded to a dynamically registered set of upstream
nodes. Nodes are health-checked periodically, load-balanced by a
configurable strategy, and forwarded with timeout, bounded retries and
standard forwarding headers. Everything runs in local processes on
`127.0.0.1`; no external service or credential is required.

The original service interface is unchanged: `GET /` still returns
`{"Hello": "World"}` and the service still starts with `python main.py`
(uvicorn, ephemeral port by default; override with `GW_HOST`/`GW_PORT`).

## HTTP API

### Admin

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/admin/upstreams` | Register `{"id", "base_url"}`; `409 upstream_already_registered` on duplicate |
| `DELETE` | `/admin/upstreams/{id}` | Deregister; `404 upstream_not_found` when unknown |
| `GET` | `/admin/upstreams` | State of every node |
| `GET` | `/admin/upstreams/{id}` | One node: health, counters, inflight, last reason |
| `GET` | `/admin/strategy` | Current strategy |
| `PUT` | `/admin/strategy` | `{"strategy": "round_robin" \| "least_inflight"}` |

Node state fields: `id`, `base_url`, `healthy`, `inflight`,
`consecutive_failures`, `consecutive_successes`, `last_reason`,
`last_check_at`.

### Forwarding

Any method (`GET`/`POST`/`PUT`/`DELETE`/`PATCH`/`HEAD`/`OPTIONS`) to
`/gateway/{path}` is forwarded to `<base_url>/<path><query>`. The
upstream status code, body and safe response headers are passed through.

Normalized error responses all use `{"error": <code>, "detail": ...}`:

| HTTP | `error` code | Meaning |
| --- | --- | --- |
| 409 | `upstream_already_registered` | Duplicate registration |
| 404 | `upstream_not_found` | Unknown node on deregister/lookup |
| 503 | `no_healthy_upstream` | No candidate is healthy |
| 504 | `upstream_timeout` | Upstream did not answer within `GW_REQUEST_TIMEOUT` |
| 502 | `upstream_connect_error` | Connection refused / reset / DNS failure |
| 502 | `upstream_5xx` | Upstream responded with 5xx |

## Health detection and recovery

- Each registered node is assumed healthy and probed with
  `GET <base_url>/health` every `GW_HEALTH_INTERVAL` seconds (probe
  timeout `GW_HEALTH_TIMEOUT`). `2xx–4xx` counts as success; a timeout,
  connection failure or `5xx` counts as failure.
- A node is marked unhealthy only after `GW_UNHEALTHY_THRESHOLD`
  **consecutive** failures; it recovers only after
  `GW_HEALTHY_THRESHOLD` **consecutive** successful probes. Counters
  reset on any opposite result, so intermittent results below the
  threshold never flip state.
- Real forwarded traffic also feeds the failure counter (a node can be
  ejected by traffic failures); recovery happens only through probes.
- The current state and the last decision reason are visible per node.

## Load balancing

- `round_robin` (default): cycles over the currently healthy nodes.
- `least_inflight`: picks the healthy node with the fewest in-flight
  requests; ties break round-robin. Selection and release are guarded
  by an asyncio lock, so inflight counts are exact under concurrency
  and always return to zero.
- Unhealthy nodes and already-tried nodes (`exclude` set during
  retries) are never selected.

## Timeout and retry semantics

- Every forwarded call has a hard timeout (`GW_REQUEST_TIMEOUT`).
- Retries are bounded (`GW_MAX_RETRIES`, default 1 extra attempt) and
  are allowed **only for idempotent methods** (`GET`, `HEAD`, `PUT`,
  `DELETE`, `OPTIONS`). Non-idempotent methods (`POST`, `PATCH`) are
  delivered at most once, even on failure.
- Each retry selects a different healthy node. Retryable outcomes:
  timeout, connection error, upstream 5xx. If every candidate fails,
  the last typed error is returned; if no candidate remains,
  `503 no_healthy_upstream` is returned.
- Hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-*`, `TE`,
  `Trailer(s)`, `Transfer-Encoding`, `Upgrade`), every token named in
  the incoming `Connection` header, and the incoming `Host` header are
  stripped. Outgoing requests get `X-Forwarded-For`,
  `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Real-IP` and `Via`.

## Configuration

All settings have defaults and can be overridden via environment
variables (`app/config.py`):

| Env var | Default | Meaning |
| --- | --- | --- |
| `GW_STRATEGY` | `round_robin` | Initial LB strategy (`round_robin` / `least_inflight`) |
| `GW_HEALTH_INTERVAL` | `1.0` | Seconds between health probes |
| `GW_HEALTH_TIMEOUT` | `1.0` | Health probe timeout (seconds) |
| `GW_UNHEALTHY_THRESHOLD` | `3` | Consecutive failures to eject |
| `GW_HEALTHY_THRESHOLD` | `2` | Consecutive successes to recover |
| `GW_REQUEST_TIMEOUT` | `5.0` | Forwarded-request timeout (seconds) |
| `GW_MAX_RETRIES` | `1` | Extra attempts, idempotent methods only |
| `GW_HOST` / `GW_PORT` | `127.0.0.1` / `0` | Bind address for `python main.py` |

Strategy can also be switched at runtime via `PUT /admin/strategy`.

## Local verification

```bash
uv sync --group dev
uv run pytest -q
```

Tests (`tests/`) start real uvicorn servers on ephemeral local ports
(fake upstreams in `tests/fakes.py`, gateway in `tests/conftest.py`)
and cover:

- health threshold flip to unhealthy, threshold-gated recovery and no
  flapping while interleaving probe results;
- probe timeout ejection;
- no-healthy-upstream error (no request reaches the upstream);
- connection failure classification;
- round-robin distribution and least-inflight preference;
- exact inflight accounting under 9 concurrent requests (counts return
  to zero, no node over-dispatched);
- timeout-driven retry on a different healthy node for idempotent GET;
- POST is never retried;
- upstream 5xx is retried then classified;
- hop-by-hop stripping and forwarding-header injection.

Node selection and every health state change are logged
(`gateway.registry` / `gateway.proxy`, e.g.
`select ok strategy=... id=... inflight=...` and
`state change id=... healthy->unhealthy reason=...`); pytest prints
them live (`log_cli` enabled).

Manual run:

```bash
uv run python main.py                 # picks a free local port
# in another shell:
curl -X POST localhost:<port>/admin/upstreams \
  -H 'content-type: application/json' \
  -d '{"id":"n1","base_url":"http://127.0.0.1:8123"}'
curl localhost:<port>/gateway/hi
```
