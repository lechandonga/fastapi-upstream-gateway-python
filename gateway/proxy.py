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


class ProxyHandler:
    def __init__(self, balancer: Balancer, config: GatewayConfig) -> None:
        self._balancer = balancer
        self._config = config
        # The request timeout covers connect / headers / read gaps; streamed
        # bodies enforce their own idle timeout in the forwarding loop so a
        # stream that keeps producing data is never cut off by total duration.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                config.request_timeout,
                connect=config.request_timeout,
                read=config.effective_stream_idle_timeout,
                write=config.request_timeout,
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
                return _error_response(503, "no_healthy_upstream", "no healthy upstream node available")
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
                last_error = _error_response(504, "upstream_timeout", f"upstream {node.name} timed out")
                await self._balancer.release(node)
                continue
            except httpx.HTTPError as exc:
                logger.warning("attempt=%d node=%s connect error=%s", attempt, node.name, exc)
                last_error = _error_response(
                    502, "upstream_connect_error", f"failed to connect to upstream {node.name}"
                )
                await self._balancer.release(node)
                continue

            if upstream_resp.status_code >= 500:
                # Drain/close the error response before releasing and retrying.
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
            logger.info(
                "streaming node=%s status=%d attempt=%d",
                node.name,
                upstream_resp.status_code,
                attempt,
            )
            response = StreamingResponse(
                content=self._stream_body(node, upstream_resp),
                status_code=upstream_resp.status_code,
            )
            # Set raw headers directly so repeated headers (e.g. multiple
            # Set-Cookie) survive, and hop-by-hop/denied headers are gone.
            response.raw_headers = [
                (
                    name if isinstance(name, bytes) else name.encode("latin-1"),
                    value if isinstance(value, bytes) else value.encode("latin-1"),
                )
                for name, value in resp_headers
            ]
            return response

        return last_error or _error_response(
            503, "no_healthy_upstream", "no healthy upstream node available"
        )

    async def _stream_body(
        self, node: UpstreamNode, upstream_resp: httpx.Response
    ):
        """Pump upstream bytes to the client as soon as they arrive.

        The node's in-flight counter stays held for the whole body transfer
        and is released exactly once:

        * after the upstream body is fully forwarded, or
        * when the client disconnects early, or
        * when an upstream error/idle timeout interrupts the transfer.

        ``aiter_raw`` yields the *encoded* body, so upstream gzip/br/zstd
        payloads pass through byte-for-byte (Content-Encoding retained), and
        chunked framing is decoded by httpx and re-framed by the ASGI server.
        A read-gap that exceeds the stream idle timeout raises
        ``ReadTimeout`` and aborts the stream; continuously arriving data
        resets that deadline no matter the total elapsed time.
        """
        try:
            async for chunk in upstream_resp.aiter_raw():
                if chunk:
                    yield chunk
        except httpx.ReadTimeout:
            logger.warning(
                "node=%s stream aborted: no upstream data for %.1fs",
                node.name,
                self._config.effective_stream_idle_timeout,
            )
        except httpx.HTTPError as exc:
            logger.warning("node=%s stream error=%s", node.name, exc)
        except GeneratorExit:  # client went away before the body finished
            logger.info("node=%s client disconnected mid-stream", node.name)
            raise
        finally:
            await upstream_resp.aclose()
            drained = await self._balancer.release(node)
            if drained:
                logger.info("node=%s fully drained and removed", node.name)
