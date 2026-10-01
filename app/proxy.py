"""Request forwarding with timeout, classification and bounded retries."""
from __future__ import annotations

import httpx

from .config import Settings
from .errors import ErrorCode, GatewayError
from .registry import UpstreamRegistry

import logging

log = logging.getLogger("gateway.proxy")

# RFC 7230 §6.1 hop-by-hop headers; must not be forwarded by proxies.
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

# Methods for which a repeated delivery has the same semantics.
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})

RETRYABLE = frozenset({
    ErrorCode.UPSTREAM_TIMEOUT,
    ErrorCode.UPSTREAM_CONNECT_ERROR,
    ErrorCode.UPSTREAM_5XX,
})


def _build_forward_headers(
    incoming: dict[str, str],
    *,
    peer_host: str,
    scheme: str,
) -> dict[str, str]:
    connection_header = incoming.get("connection", "")
    connection_tokens = {
        token.strip().lower()
        for token in connection_header.split(",")
        if token.strip()
    }
    blocked = HOP_BY_HOP | connection_tokens | {"host"}

    headers = {
        key: value
        for key, value in incoming.items()
        if key.lower() not in blocked
    }

    client_host = peer_host or "127.0.0.1"
    prior_xff = incoming.get("x-forwarded-for")
    headers["X-Forwarded-For"] = (
        f"{prior_xff}, {client_host}" if prior_xff else client_host
    )
    headers["X-Forwarded-Proto"] = scheme
    headers["X-Forwarded-Host"] = incoming.get("host", "")
    headers["X-Real-IP"] = client_host
    headers["Via"] = "1.1 fastapi-upstream-gateway"
    return headers


async def forward(
    registry: UpstreamRegistry,
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    method: str,
    path: str,
    query: bytes,
    headers: dict[str, str],
    body: bytes,
    peer_host: str,
    scheme: str,
) -> httpx.Response:
    idempotent = method.upper() in IDEMPOTENT_METHODS
    attempts_allowed = 1 + (settings.max_retries if idempotent else 0)
    excluded: set[str] = set()
    last_error: GatewayError | None = None

    forward_headers = _build_forward_headers(
        headers, peer_host=peer_host, scheme=scheme
    )

    for attempt in range(1, attempts_allowed + 1):
        try:
            node = await registry.select(exclude=excluded)
        except GatewayError:
            if last_error is not None:
                log.info(
                    "retry exhausted method=%s attempts=%d error=%s",
                    method, attempt - 1, last_error.code.value,
                )
                raise last_error
            raise

        url = f"{node.base_url}/{path.lstrip('/')}"
        if query:
            url = f"{url}?{query.decode('ascii')}"
        log.info(
            "forward attempt=%d/%d method=%s id=%s url=%s idempotent=%s",
            attempt, attempts_allowed, method, node.node_id, url, idempotent,
        )
        try:
            resp = await client.request(
                method,
                url,
                content=body,
                headers=forward_headers,
                timeout=settings.request_timeout,
            )
        except httpx.TimeoutException as exc:
            error = GatewayError(
                ErrorCode.UPSTREAM_TIMEOUT,
                f"upstream {node.node_id} timed out: {exc.__class__.__name__}",
                status_code=504,
            )
        except httpx.TransportError as exc:
            error = GatewayError(
                ErrorCode.UPSTREAM_CONNECT_ERROR,
                f"upstream {node.node_id} connection failed: "
                f"{exc.__class__.__name__}",
                status_code=502,
            )
        else:
            if 500 <= resp.status_code < 600:
                error = GatewayError(
                    ErrorCode.UPSTREAM_5XX,
                    f"upstream {node.node_id} returned {resp.status_code}",
                    status_code=502,
                )
            else:
                await registry.record_request_result(
                    node, True, f"upstream {resp.status_code}"
                )
                await registry.release(node)
                log.info(
                    "forward ok attempt=%d id=%s status=%d",
                    attempt, node.node_id, resp.status_code,
                )
                return resp

        await registry.record_request_result(node, False, error.message)
        await registry.release(node)
        excluded.add(node.node_id)
        last_error = error
        log.info(
            "forward failed attempt=%d id=%s error=%s retryable=%s",
            attempt, node.node_id, error.code.value,
            error.code in RETRYABLE,
        )

    assert last_error is not None
    log.info(
        "giving up method=%s attempts=%d error=%s",
        method, attempts_allowed, last_error.code.value,
    )
    raise last_error
