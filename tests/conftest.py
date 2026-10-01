import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request

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
