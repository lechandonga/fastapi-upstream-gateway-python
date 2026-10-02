import logging

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .balancer import Balancer
from .config import GatewayConfig
from .errors import NoHealthyUpstreamError
from .registry import UpstreamNode

logger = logging.getLogger("gateway.proxy")

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

IDEMPOTENT_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}


def _error_response(status_code: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": error, "detail": detail})


class ProxyHandler:
    def __init__(self, balancer: Balancer, config: GatewayConfig) -> None:
        self._balancer = balancer
        self._config = config
        self._request_block = HOP_BY_HOP_HEADERS | set(config.block_request_headers)
        self._response_block = HOP_BY_HOP_HEADERS | set(config.block_response_headers)
        # Per-request timeouts are applied at send time. The read phase is
        # an *idle* timeout: as long as data keeps arriving a stream may
        # run far longer than request_timeout in total.
        self._client = httpx.AsyncClient(timeout=None)

    async def close(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _connection_nominations(headers) -> set[str]:
        """Header names nominated by a Connection header (RFC 7230 6.1).

        Every nominated name is hop-by-hop for this exchange and must be
        stripped before forwarding in either direction.
        """
        raw = headers.get("connection")
        if not raw:
            return set()
        return {part.strip().lower() for part in raw.split(",") if part.strip()}

    def _build_headers(self, request: Request, node: UpstreamNode) -> dict[str, str]:
        banned = self._request_block | self._connection_nominations(request.headers)
        headers = {
            k: v for k, v in request.headers.items() if k.lower() not in banned
        }
        headers["host"] = httpx.URL(node.url).netloc.decode()
        client_ip = request.client.host if request.client else ""
        prior = request.headers.get("x-forwarded-for")
        headers["x-forwarded-for"] = f"{prior}, {client_ip}" if prior else client_ip
        headers["x-forwarded-host"] = request.headers.get("host", "")
        headers["x-forwarded-proto"] = request.url.scheme
        return headers

    def _filter_response_headers(self, upstream_resp: httpx.Response) -> dict[str, str]:
        banned = self._response_block | self._connection_nominations(upstream_resp.headers)
        return {
            k: v for k, v in upstream_resp.headers.items() if k.lower() not in banned
        }

    async def forward(self, request: Request, path: str) -> Response:
        method = request.method.upper()
        idempotent = method in IDEMPOTENT_METHODS
        attempts = 1 + (self._config.max_retries if idempotent else 0)
        body = await request.body()
        tried: set[str] = set()
        last_error: JSONResponse | None = None

        for attempt in range(attempts):
            try:
                node = await self._balancer.acquire(exclude=tried)
            except NoHealthyUpstreamError:
                if last_error is not None:
                    return last_error
                return _error_response(
                    503, "no_healthy_upstream", "no healthy upstream node available"
                )
            tried.add(node.name)
            try:
                url = node.url + "/" + path
                upstream_request = self._client.build_request(
                    method,
                    url,
                    params=request.query_params,
                    content=body,
                    headers=self._build_headers(request, node),
                    timeout=httpx.Timeout(self._config.request_timeout),
                )
                # The read timeout is an *idle* timeout (gap between
                # chunks), not a cap on total transfer duration.
                upstream_resp = await self._client.send(
                    upstream_request, stream=True
                )
            except httpx.TimeoutException:
                logger.warning("attempt=%d node=%s timeout", attempt, node.name)
                last_error = _error_response(
                    504, "upstream_timeout", f"upstream {node.name} timed out"
                )
                await self._balancer.release(node)
                continue
            except httpx.HTTPError as exc:
                logger.warning("attempt=%d node=%s connect error=%s", attempt, node.name, exc)
                last_error = _error_response(
                    502,
                    "upstream_connect_error",
                    f"failed to connect to upstream {node.name}",
                )
                await self._balancer.release(node)
                continue

            if upstream_resp.status_code >= 500:
                logger.warning(
                    "attempt=%d node=%s upstream 5xx status=%d",
                    attempt,
                    node.name,
                    upstream_resp.status_code,
                )
                await upstream_resp.aclose()
                await self._balancer.release(node)
                last_error = _error_response(
                    502,
                    "upstream_server_error",
                    f"upstream {node.name} returned {upstream_resp.status_code}",
                )
                continue

            # Status/headers look good. Wait for the first body chunk
            # before committing a response: an upstream that stalls
            # before any data still trips the idle timeout and can be
            # retried on another node. The in-flight counter stays held
            # for the whole stream and is released in body_iter()'s
            # finally block -- including when the client disconnects.
            stream = upstream_resp.aiter_raw()
            try:
                first_chunk = await anext(stream, b"")
            except httpx.TimeoutException:
                logger.warning(
                    "attempt=%d node=%s timeout waiting for first chunk",
                    attempt,
                    node.name,
                )
                await upstream_resp.aclose()
                await self._balancer.release(node)
                last_error = _error_response(
                    504, "upstream_timeout", f"upstream {node.name} timed out"
                )
                continue
            except httpx.HTTPError as exc:
                logger.warning(
                    "attempt=%d node=%s stream error=%s", attempt, node.name, exc
                )
                await upstream_resp.aclose()
                await self._balancer.release(node)
                last_error = _error_response(
                    502,
                    "upstream_connect_error",
                    f"upstream {node.name} broke the response stream",
                )
                continue

            resp_headers = self._filter_response_headers(upstream_resp)
            balancer = self._balancer

            async def body_iter():
                try:
                    if first_chunk:
                        yield first_chunk
                    async for chunk in stream:
                        if chunk:
                            yield chunk
                except httpx.TimeoutException:
                    logger.warning(
                        "node=%s idle timeout while streaming; terminating response",
                        node.name,
                    )
                except httpx.HTTPError as exc:
                    logger.warning(
                        "node=%s stream broken: %s; terminating response",
                        node.name,
                        exc.__class__.__name__,
                    )
                finally:
                    await upstream_resp.aclose()
                    await balancer.release(node)

            logger.info(
                "stream start node=%s status=%d response_headers=%d",
                node.name,
                upstream_resp.status_code,
                len(resp_headers),
            )
            return StreamingResponse(
                content=body_iter(),
                status_code=upstream_resp.status_code,
                headers=resp_headers,
            )

        return last_error or _error_response(
            503, "no_healthy_upstream", "no healthy upstream node available"
        )
