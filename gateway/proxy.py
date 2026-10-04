import asyncio
import logging
from typing import Any

import anyio
import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response
from starlette._utils import create_collapsing_task_group
from starlette.background import BackgroundTask
from starlette.types import Receive, Scope, Send

from .balancer import Balancer
from .config import GatewayConfig
from .errors import NoHealthyUpstreamError
from .headers import filter_headers
from .registry import UpstreamNode

logger = logging.getLogger("gateway.proxy")

IDEMPOTENT_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}


def _error_response(status_code: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": error, "detail": detail})


def _raw(headers: Any) -> list[tuple[bytes, bytes]]:
    return [
        (
            name if isinstance(name, bytes) else name.encode("latin-1"),
            value if isinstance(value, bytes) else value.encode("latin-1"),
        )
        for name, value in headers
    ]


class UpstreamBrokenError(Exception):
    """Raised after the response was committed: the upstream stopped sending
    data (idle timeout, mid-body error or dropped connection).

    Once response bytes have gone out on the wire we cannot swap in a JSON
    error status anymore; raising aborts the ASGI response without a terminal
    zero-length chunk, so the server closes the connection and the client
    sees an interrupted transfer instead of a short-but-"successful" 200.
    """


class ProxyResponse(Response):
    """Drives the upstream forwarding loop as a retry-aware ASGI response.

    Nothing is sent to the client until the first upstream body bytes are in
    hand.  If an upstream stalls after headers, errors mid-way or drops the
    connection *before* that first chunk, the whole attempt is discarded and
    retried on a different healthy node (idempotent methods only).  After the
    first chunk is forwarded the response is committed; a later upstream
    failure then aborts the connection rather than ending the body cleanly.

    A dedicated disconnect watcher task cancels the forwarding task as soon as
    the client goes away.  All per-attempt cleanup (closing the upstream
    response, releasing the in-flight counter) runs in a shielded scope so
    cancellation can never leave a counter or connection behind.
    """

    media_type = None

    def __init__(self, handler: "ProxyHandler", request: Request, path: str) -> None:
        self._handler = handler
        self._request = request
        self._path = path
        self._forwarding_complete = False
        self.status_code = 200
        self.raw_headers: list[tuple[bytes, bytes]] = []
        self.background: BackgroundTask | None = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async with create_collapsing_task_group() as task_group:

            async def watch_disconnect() -> None:
                while True:
                    message = await receive()
                    # Important: uvicorn's receive() also synthesises
                    # "http.disconnect" once the *response* is complete.  The
                    # watcher must only react to a real client abort and must
                    # not keep polling after forwarding finished, otherwise a
                    # normal completion would cancel an in-progress stream.
                    if self._forwarding_complete:
                        return
                    if message["type"] == "http.disconnect":
                        logger.info("client disconnected mid-stream, cancelling forward")
                        task_group.cancel_scope.cancel()
                        return

            task_group.start_soon(watch_disconnect)
            await self._drive(scope, send)
            # Forwarding finished cleanly; stop the disconnect watcher.
            task_group.cancel_scope.cancel()

    async def _drive(self, scope: Scope, send: Send) -> None:
        request = self._request
        method = request.method.upper()
        idempotent = method in IDEMPOTENT_METHODS
        attempts = 1 + (self._handler.config.max_retries if idempotent else 0)
        body = await request.body()
        tried: set[str] = set()
        last_error: JSONResponse | None = None

        for attempt in range(attempts):
            try:
                node = await self._handler.balancer.acquire(exclude=tried)
            except NoHealthyUpstreamError:
                await self._send_json(send, request.scope, last_error or _error_response(
                    503, "no_healthy_upstream", "no healthy upstream node available"
                ))
                return
            tried.add(node.name)

            outcome = await self._attempt(
                send=send,
                scope=scope,
                attempt=attempt,
                node=node,
                method=method,
                body=body,
            )
            if outcome == "committed":
                # Body fully forwarded (or the client went away); nothing more
                # to do, the disconnect watcher/task group tears down below.
                return
            if isinstance(outcome, JSONResponse):
                last_error = outcome
                continue
            if isinstance(outcome, UpstreamBrokenError):
                raise outcome

        await self._send_json(
            send,
            request.scope,
            last_error
            or _error_response(503, "no_healthy_upstream", "no healthy upstream node available"),
        )

    async def _attempt(
        self,
        *,
        send: Send,
        scope: Scope,
        attempt: int,
        node: UpstreamNode,
        method: str,
        body: bytes,
    ) -> Any:
        """Run one upstream attempt.

        Returns ``"committed"`` when the full body was forwarded (or the
        client disconnected after commit), a ``JSONResponse`` describing a
        pre-commit failure (caller may retry on another node), or raises
        ``UpstreamBrokenError`` for a failure after bytes were committed.
        """
        config = self._handler.config
        request = self._request
        req = self._handler.client.build_request(
            method,
            node.url + "/" + self._path,
            params=request.query_params,
            content=body,
            headers=self._handler.build_headers(request, node),
        )

        upstream_resp: httpx.Response | None = None
        released = False

        async def cleanup() -> None:
            # Runs shielded: neither client-disconnect cancellation nor our
            # own raise may skip closing the upstream connection / releasing
            # the in-flight counter exactly once.
            nonlocal released
            if released:
                return
            released = True

            async def _do_cleanup() -> None:
                if upstream_resp is not None:
                    await upstream_resp.aclose()
                await self._handler.balancer.release(node)

            # A shielded scope keeps cleanup alive even when the outer task
            # is being cancelled (client disconnect), so the upstream
            # connection always closes and the counter always drops.
            try:
                with anyio.CancelScope(shield=True):
                    await _do_cleanup()
            except Exception:
                logger.exception("node=%s cleanup failed", node.name)

        try:
            try:
                # The connect/write phases use the regular request timeout;
                # waiting for response headers is bounded by it as well.
                upstream_resp = await asyncio.wait_for(
                    self._handler.client.send(req, stream=True),
                    timeout=config.request_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "scenario=upstream_header_timeout attempt=%d node=%s", attempt, node.name
                )
                return _error_response(
                    504, "upstream_timeout", f"upstream {node.name} timed out"
                )
            except httpx.TimeoutException:
                logger.warning(
                    "scenario=upstream_connect_timeout attempt=%d node=%s", attempt, node.name
                )
                return _error_response(
                    504, "upstream_timeout", f"upstream {node.name} timed out"
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    "scenario=upstream_connect_error attempt=%d node=%s error=%s",
                    attempt,
                    node.name,
                    exc,
                )
                return _error_response(
                    502,
                    "upstream_connect_error",
                    f"failed to connect to upstream {node.name}",
                )

            if upstream_resp.status_code >= 500:
                logger.warning(
                    "scenario=upstream_5xx attempt=%d node=%s status=%d",
                    attempt,
                    node.name,
                    upstream_resp.status_code,
                )
                return _error_response(
                    502,
                    "upstream_server_error",
                    f"upstream {node.name} returned {upstream_resp.status_code}",
                )

            resp_headers = filter_headers(
                upstream_resp.headers.raw,
                blocked=config.blocked_response_headers,
            )

            committed = False
            try:
                async for chunk in upstream_resp.aiter_raw():
                    if not chunk:
                        continue
                    if not committed:
                        # Commit point: first real body bytes.  Until here a
                        # failure is still invisible to the client and the
                        # attempt may be transparently retried elsewhere.
                        self.status_code = upstream_resp.status_code
                        self.raw_headers = _raw(resp_headers)
                        logger.info(
                            "scenario=stream_commit node=%s status=%d attempt=%d",
                            node.name,
                            upstream_resp.status_code,
                            attempt,
                        )
                        await send(
                            {
                                "type": "http.response.start",
                                "status": upstream_resp.status_code,
                                "headers": self.raw_headers,
                            }
                        )
                        committed = True
                    await send(
                        {"type": "http.response.body", "body": chunk, "more_body": True}
                    )
            except OSError:
                # Sending to the client failed: the connection is gone.
                logger.info("scenario=client_send_error node=%s", node.name)
                raise
            except asyncio.CancelledError:
                # Client disconnect (watcher cancelled us) or server shutdown.
                logger.info(
                    "scenario=client_abort node=%s committed=%s", node.name, committed
                )
                raise
            except httpx.ReadTimeout:
                logger.warning(
                    "scenario=upstream_idle_timeout node=%s committed=%s gap=%.1fs",
                    node.name,
                    committed,
                    config.effective_stream_idle_timeout,
                )
                if not committed:
                    return _error_response(
                        504,
                        "upstream_timeout",
                        f"upstream {node.name} stalled before sending body data",
                    )
                raise UpstreamBrokenError(f"upstream {node.name} stalled mid-body")
            except httpx.HTTPError as exc:
                logger.warning(
                    "scenario=upstream_stream_error node=%s committed=%s error=%s",
                    node.name,
                    committed,
                    exc,
                )
                if not committed:
                    return _error_response(
                        502,
                        "upstream_stream_error",
                        f"upstream {node.name} failed before sending body data",
                    )
                raise UpstreamBrokenError(f"upstream {node.name} broke mid-body: {exc}")

            if not committed:
                # Complete header-only / empty body response (e.g. 204, 304).
                self.status_code = upstream_resp.status_code
                self.raw_headers = _raw(resp_headers)
                logger.info(
                    "scenario=stream_commit_empty node=%s status=%d attempt=%d",
                    node.name,
                    upstream_resp.status_code,
                    attempt,
                )
                await send(
                    {
                        "type": "http.response.start",
                        "status": upstream_resp.status_code,
                        "headers": self.raw_headers,
                    }
                )

            # Clean end of upstream body: mark forwarding complete *before*
            # the terminal chunk makes uvicorn's receive() synthesise
            # http.disconnect, so the watcher cannot mistake it for an abort.
            self._forwarding_complete = True
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            logger.info("scenario=stream_complete node=%s attempt=%d", node.name, attempt)
            await cleanup()
            return "committed"
        finally:
            await cleanup()

    @staticmethod
    async def _send_json(send: Send, scope: Scope, response: JSONResponse) -> None:
        async def receive() -> dict[str, str]:  # pragma: no cover - error bodies are small
            return {"type": "http.disconnect"}

        await response(scope=scope, receive=receive, send=send)


class ProxyHandler:
    def __init__(self, balancer: Balancer, config: GatewayConfig) -> None:
        self._balancer = balancer
        self._config = config
        # ``total`` is deliberately disabled: a stream that keeps producing
        # data must not be cut off just because the *overall* duration passes
        # the ordinary request timeout.  The read timeout is a per-gap idle
        # timeout enforced between chunks (connect/write still use
        # request_timeout), and waiting for response headers is additionally
        # bounded by request_timeout in the forwarding loop.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                timeout=config.effective_stream_idle_timeout,
                connect=config.request_timeout,
                read=config.effective_stream_idle_timeout,
                write=config.request_timeout,
                pool=config.request_timeout,
            )
        )

    @property
    def config(self) -> GatewayConfig:
        return self._config

    @property
    def client(self) -> httpx.AsyncClient:
        return self._client

    @property
    def balancer(self) -> Balancer:
        return self._balancer

    async def close(self) -> None:
        await self._client.aclose()

    def build_headers(self, request: Request, node: UpstreamNode) -> dict[str, str]:
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
        return ProxyResponse(self, request, path)
