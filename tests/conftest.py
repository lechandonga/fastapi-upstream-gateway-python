import socket
import threading
import time
import gzip
import json
import asyncio

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

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
    app.state.stream_started = 0
    app.state.stream_completed = 0
    app.state.slow_first = True

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
            await asyncio.sleep(app.state.delay)
        return {"node": app.state.name}

    @app.get("/boom")
    async def boom():
        count("/boom")
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=500, content={"detail": "boom"})

    @app.get("/large")
    async def large(request: Request):
        count("/large")
        size = int(request.query_params.get("size", "1024"))
        payload = bytes((i * 7 + 3) % 251 for i in range(size))
        return Response(content=payload, media_type="application/octet-stream")

    @app.get("/gzip")
    async def gzipped():
        count("/gzip")
        payload = json.dumps(
            {"node": app.state.name, "blob": "x" * 8192}, sort_keys=True
        ).encode()
        return Response(
            content=gzip.compress(payload),
            media_type="application/json",
            headers={"content-encoding": "gzip"},
        )

    @app.api_route("/headers-echo", methods=["GET", "POST"])
    async def headers_echo(request: Request):
        count("/headers-echo")
        body = {
            "node": app.state.name,
            "headers": {k.lower(): v for k, v in request.headers.items()},
        }
        return Response(
            content=json.dumps(body).encode(),
            media_type="application/json",
            headers={
                "x-upstream-internal": "secret-marker",
                "x-upstream-shared": "ok",
            },
        )

    @app.get("/stream")
    async def stream(request: Request):
        count("/stream")
        chunks = int(request.query_params.get("chunks", "10"))
        interval = float(request.query_params.get("interval", "0.1"))
        stall_after = int(request.query_params.get("stall_after", "-1"))

        async def gen():
            app.state.stream_started += 1
            try:
                for i in range(chunks):
                    if stall_after >= 0 and i == stall_after:
                        await asyncio.sleep(30)
                    yield f"chunk-{i:04d}\n".encode()
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            finally:
                app.state.stream_completed += 1

        return StreamingResponse(gen(), media_type="text/plain")

    @app.get("/slow-first")
    async def slow_first():
        count("/slow-first")

        async def gen():
            if app.state.slow_first:
                await asyncio.sleep(30)
            yield b"late"

        return StreamingResponse(gen(), media_type="text/plain")

    return app


class RawHttpUpstream:
    """Minimal HTTP/1.1 server written on raw sockets.

    Used to test forwarding of response framing that real ASGI servers
    refuse to emit (e.g. custom hop-by-hop Connection nominations).
    """

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(5)
        self.url = f"http://127.0.0.1:{self._sock.getsockname()[1]}"
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            conn.settimeout(5)
            data = b""
            while b"\r\n\r\n" not in data:
                more = conn.recv(4096)
                if not more:
                    return
                data += more
            if b"GET /health" in data:
                body = b'{"status":"ok"}'
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Length: "
                    + str(len(body)).encode()
                    + b"\r\nConnection: close\r\n\r\n"
                    + body
                )
                return
            body = b"raw-body-ok"
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Connection: close, x-secret\r\n"
                b"X-Secret: should-be-stripped\r\n"
                b"X-Shared: kept\r\n"
                b"Content-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body
            )
        finally:
            conn.close()

    def stop(self):
        self._sock.close()


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
