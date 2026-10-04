import asyncio
import socket
import threading
import time

import anyio
import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, Response, StreamingResponse

from gateway.app import create_app
from gateway.config import GatewayConfig


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    def __init__(self, app, port: int):
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self):
        self._thread.start()
        deadline = time.time() + 10
        while not self._server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.02)
        return self

    def stop(self):
        self._server.should_exit = True
        self._thread.join(timeout=10)


def make_fake_upstream(name: str, delay: float = 0.0) -> FastAPI:
    app = FastAPI()
    app.state.name = name
    app.state.healthy = True
    app.state.delay = delay
    app.state.hits = {}
    app.state.slow_stream_gate = anyio.Event()
    app.state.header_stall_gate = anyio.Event()
    app.state.header_stalls = False
    app.state.half_errors = False
    app.state.fail_before_byte = False

    def count(path: str):
        app.state.hits[path] = app.state.hits.get(path, 0) + 1

    @app.get("/health")
    async def health():
        count("/health")
        if app.state.healthy:
            return {"status": "ok"}
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=503, content={"status": "down"})

    @app.get("/echo")
    async def echo(request: Request):
        count("/echo")
        return {
            "node": app.state.name,
            "x_forwarded_for": request.headers.get("x-forwarded-for"),
            "x_forwarded_host": request.headers.get("x-forwarded-host"),
            "x_forwarded_proto": request.headers.get("x-forwarded-proto"),
            "connection": request.headers.get("connection"),
        }

    @app.api_route("/work", methods=["GET", "POST"])
    async def work():
        count("/work")
        if app.state.delay:
            import asyncio

            await asyncio.sleep(app.state.delay)
        return {"node": app.state.name}

    @app.get("/boom")
    async def boom():
        count("/boom")
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=500, content={"detail": "boom"})

    @app.get("/big")
    async def big():
        count("/big")
        total = 512 * 1024

        def gen():
            chunk = b"x" * 64 * 1024
            sent = 0
            while sent < total:
                yield chunk
                sent += len(chunk)

        return StreamingResponse(gen(), media_type="application/octet-stream")

    @app.get("/gzip-me")
    async def gzip_me():
        count("/gzip-me")
        import gzip

        payload = ("gateway-gzip-payload-" * 4096).encode()
        return Response(
            content=gzip.compress(payload),
            media_type="text/plain",
            headers={"content-encoding": "gzip", "x-upstream-tag": "kept"},
        )

    @app.get("/chunked")
    async def chunked():
        count("/chunked")

        async def gen():
            for i in range(10):
                yield f"chunk-{i}\n".encode()

        return StreamingResponse(gen(), media_type="text/plain")

    @app.get("/slow-stream")
    async def slow_stream():
        count("/slow-stream")

        async def gen():
            yield b"first\n"
            await app.state.slow_stream_gate.wait()
            yield b"second\n"

        return StreamingResponse(gen(), media_type="text/plain")

    @app.get("/slow-start")
    async def slow_start():
        count("/slow-start")
        await app.state.slow_stream_gate.wait()
        return PlainTextResponse("done")

    @app.get("/stall-headers")
    async def stall_headers():
        # Nodes flagged header_stalls flush response headers and never produce
        # body bytes until the gate is released; other nodes answer normally
        # so the gateway can retry and serve the complete content.
        count("/stall-headers")
        if not app.state.header_stalls:
            return PlainTextResponse("complete-body\n")

        async def gen():
            await app.state.header_stall_gate.wait()
            yield b"finally\n"

        return StreamingResponse(gen(), media_type="text/plain")

    @app.post("/release-header-stall")
    async def release_header_stall():
        app.state.header_stall_gate.set()
        return {"released": True}

    @app.get("/half-then-error")
    async def half_then_error():
        # Flagged nodes send a few real chunks then die mid-body: the
        # connection ends without a terminating chunk, so the client must see
        # a broken stream instead of a clean (truncated) 200.
        count("/half-then-error")
        if not app.state.half_errors:
            return PlainTextResponse("complete-body\n")

        async def gen():
            if app.state.fail_before_byte:
                await asyncio.sleep(0.05)
                raise RuntimeError("upstream died before first byte")
            yield b"part-0\n"
            await asyncio.sleep(0.1)
            yield b"part-1\n"
            await asyncio.sleep(0.1)
            raise RuntimeError("upstream exploded mid-body")

        return StreamingResponse(gen(), media_type="text/plain")

    @app.get("/full")
    async def full():
        count("/full")
        return PlainTextResponse("complete-body\n")

    @app.post("/release-slow-stream")
    async def release_slow_stream():
        app.state.slow_stream_gate.set()
        return {"released": True}

    @app.get("/headers-debug")
    async def headers_debug(request: Request):
        count("/headers-debug")
        return {"headers": dict(request.headers)}

    @app.get("/internal-response")
    async def internal_response():
        count("/internal-response")
        return PlainTextResponse(
            "ok",
            headers={
                "x-internal-only": "secret",
                "x-public-tag": "visible",
                "set-cookie": "a=1",
            },
        )

    @app.get("/hop-response")
    async def hop_response():
        count("/hop-response")
        return PlainTextResponse(
            "hop-body",
            headers={
                "connection": "x-up-hop, keep-alive",
                "x-up-hop": "should-be-stripped",
                "x-safe-resp": "kept",
            },
        )

    @app.get("/steady-stream")
    async def steady_stream(request: Request):
        count("/steady-stream")
        duration = float(request.query_params.get("duration", "1"))
        interval = float(request.query_params.get("interval", "0.05"))

        async def gen():
            import asyncio

            elapsed = 0.0
            i = 0
            while elapsed < duration:
                await asyncio.sleep(interval)
                yield f"{i}\n".encode()
                elapsed += interval
                i += 1

        return StreamingResponse(gen(), media_type="text/plain")

    return app


@pytest.fixture
def upstream_factory():
    servers = []

    def factory(name: str, delay: float = 0.0) -> RunningServer:
        srv = RunningServer(make_fake_upstream(name, delay), free_port()).start()
        servers.append(srv)
        return srv

    yield factory
    for srv in servers:
        srv.stop()


@pytest.fixture
def gateway():
    servers = []

    def factory(config: GatewayConfig | None = None) -> RunningServer:
        cfg = config or GatewayConfig(
            health_interval=3600,  # tests trigger checks explicitly
            failure_threshold=2,
            success_threshold=2,
            request_timeout=0.5,
            max_retries=2,
        )
        srv = RunningServer(create_app(cfg), free_port()).start()
        servers.append(srv)
        return srv

    yield factory
    for srv in servers:
        srv.stop()


@pytest.fixture
def client():
    with httpx.Client(timeout=10) as c:
        yield c
