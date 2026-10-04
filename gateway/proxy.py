import asyncio
import logging

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .balancer import Balancer
from .config import GatewayConfig
from .errors import NoHealthyUpstreamError
from .headers import filter_headers
from .registry import UpstreamNode

logger = logging.getLogger("gateway.proxy")

IDEMPOTENT_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}


def _error_response(status_code: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": error, "detail": detail})


def _raw_headers(headers) -> list[tuple[bytes, bytes]]:
    return [
        (
            name if isinstance(name, bytes) else name.encode("latin-1"),
            value if isinstance(value, bytes) else value.encode("latin-1"),
        )
        for name, value in headers
    ]


class ProxyHandler:
    def __init__(self, balancer: Balancer, config: GatewayConfig) -> None:
        self._balancer = balancer
        self._config = config
        # There is deliberately *no total deadline* on an upstream response:
        # a stream that keeps producing data may run arbitrarily long. The
        # read timeout bounds only the gap between two consecutive reads
        # (connect/write/pool use the ordinary per-request timeout).
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=config.request_timeout,
                read=config.effective_stream_idle_timeout,
                write=config.request_timeout,
                pool=config.request_timeout,
            )
        )

    async def close(self) -> None:
        await self._client.aclose()

    def _build_headers(self, request: Request, node: UpstreamNode) -> dict[str, str]:
        headers = {
            name: value
            for name, value in filter_headers(
                request.headers.items(),
                blocked=self._config.blocked_request_headers,
            )
        }
        headers["host"] = httpx.URL(node.url).netloc.decode()
        client_ip = request.client.host if request.client else ""
        prior = request.headers.get("x-forwarded-for")
        headers["x-forwarded-for"] = f"{prior}, {client_ip}" if prior else client_ip
        headers["x-forwarded-host"] = request.headers.get("host", "")
        headers["x-forwarded-proto"] = request.url.scheme
        return headers

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
                req = self._client.build_request(
                    method,
                    url,
                    params=request.query_params,
                    content=body,
                    headers=self._build_headers(request, node),
                )
                upstream_resp = await self._client.send(req, stream=True)
            except httpx.TimeoutException:
                logger.warning("attempt=%d node=%s timeout", attempt, node.name)
                last_error = _error_response(
                    504, "upstream_timeout", f"upstream {node.name} timed out"
                )
                await self._balancer.release(node)
                continue
            except httpx.HTTPError as exc:
                logger.warning(
                    "attempt=%d node=%s connect error=%s", attempt, node.name, exc
                )
                last_error = _error_response(
                    502,
                    "upstream_connect_error",
                    f"failed to connect to upstream {node.name}",
                )
                await self._balancer.release(node)
                continue

            if upstream_resp.status_code >= 500:
                await upstream_resp.aclose()
                logger.warning(
                    "attempt=%d node=%s upstream 5xx status=%d",
                    attempt,
                    node.name,
                    upstream_resp.status_code,
                )
                last_error = _error_response(
                    502,
                    "upstream_server_error",
                    f"upstream {node.name} returned {upstream_resp.status_code}",
                )
                await self._balancer.release(node)
                continue

            resp_headers = filter_headers(
                upstream_resp.headers.raw,
                blocked=self._config.blocked_response_headers,
            )

            # Commit point: nothing is sent to the client until we either have
            # the first real body byte or a clean end-of-body. If the upstream
            # stalls after headers, dies, or truncates the body before the
            # first byte, we can still release this node and retry another
            # one -- the client never sees a misleading "200 then nothing".
            body_iter = upstream_resp.aiter_raw().__aiter__()
            try:
                first_chunk = await body_iter.__anext__()
            except StopAsyncIteration:
                await upstream_resp.aclose()
                await self._balancer.release(node)
                logger.info(
                    "empty body node=%s status=%d attempt=%d",
                    node.name,
                    upstream_resp.status_code,
                    attempt,
                )
                response = Response(status_code=upstream_resp.status_code, content=b"")
                response.raw_headers = _raw_headers(resp_headers)
                return response
            except httpx.TimeoutException:
                await upstream_resp.aclose()
                await self._balancer.release(node)
                logger.warning(
                    "attempt=%d node=%s stalled after response headers, retrying",
                    attempt,
                    node.name,
                )
                last_error = _error_response(
                    504,
                    "upstream_timeout",
                    f"upstream {node.name} sent headers but no body in time",
                )
                continue
            except httpx.HTTPError as exc:
                await upstream_resp.aclose()
                await self._balancer.release(node)
                logger.warning(
                    "attempt=%d node=%s broken before first body byte: %s, retrying",
                    attempt,
                    node.name,
                    exc,
                )
                last_error = _error_response(
                    502,
                    "upstream_broken_response",
                    f"upstream {node.name} failed before sending any body",
                )
                continue

            logger.info(
                "streaming node=%s status=%d attempt=%d",
                node.name,
                upstream_resp.status_code,
                attempt,
            )
            response = StreamingResponse(
                content=self._stream_body(
                    node, upstream_resp, body_iter, first_chunk
                ),
                status_code=upstream_resp.status_code,
            )
            # Raw headers keep repeated headers (e.g. several Set-Cookie) and
            # drop hop-by-hop/denied headers; Content-Encoding passes through.
            response.raw_headers = _raw_headers(resp_headers)
            return response

        return last_error or _error_response(
            503, "no_healthy_upstream", "no healthy upstream node available"
        )

    async def _stream_body(
        self,
        node: UpstreamNode,
        upstream_resp: httpx.Response,
        body_iter,
        first_chunk: bytes,
    ):
        """Pump the encoded upstream body to the client byte-for-byte.

        Semantics once the response has been committed (first byte sent):

        * Only the *idle gap* between upstream reads is bounded by the read
          timeout; continuous data resets the deadline forever, so total
          duration is unlimited and big/compressed/chunked bodies stream
          unchanged.
        * A client disconnect cancels this generator at its current suspend
          point (Starlette aborts the response task group); the finally
          block then closes the upstream connection and releases the
          in-flight counter immediately, instead of waiting for an upstream
          timeout. We deliberately do not poll ``request.is_disconnected``
          ourselves: that would create a second consumer of the ASGI receive
          channel racing with StreamingResponse's own disconnect watcher.
        * An upstream error or truncation after commit aborts the client
          response at the transport level (the exception propagates and the
          connection is closed without a clean terminator), so the client
          can tell the body is incomplete instead of receiving a response
          that merely *looks* successfully finished.
        * The node is released exactly once on every exit path.
        """
        released = False

        async def release_once() -> None:
            nonlocal released
            if released:
                return
            released = True
            await self._balancer.release(node)

        completed = False
        try:
            yield first_chunk

            # A plain async-for: end-of-body is handled natively, while a
            # client disconnect cancels this generator at the current await
            # point, still running the finally block below.
            async for chunk in body_iter:
                if chunk:
                    yield chunk
            completed = True
        except asyncio.CancelledError:
            # The ASGI server cancelled the response task group because the
            # client went away. Closing the upstream connection below breaks
            # any in-flight upstream read promptly.
            logger.info("node=%s client disconnected, aborting upstream", node.name)
            raise
        except (httpx.ReadTimeout, httpx.HTTPError) as exc:
            # Committed response, so a synthetic status code is impossible:
            # propagate to tear the transport down. The client then sees a
            # transport error instead of a truncated but "successful" 200.
            logger.warning(
                "node=%s upstream stream failed mid-body (%s); aborting client response",
                node.name,
                exc.__class__.__name__,
            )
            raise
        finally:
            await upstream_resp.aclose()
            await release_once()
            if completed:
                logger.info(
                    "node=%s body fully streamed, counter released", node.name
                )
