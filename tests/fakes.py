"""Fake upstream servers bound to real local ports."""
from __future__ import annotations

import threading
import time
import asyncio
from dataclasses import dataclass, field

import uvicorn
from fastapi import FastAPI, Request


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class FakeUpstream:
    node_id: str
    healthy: bool = True
    health_status: int = 200
    health_delay: float = 0.0
    request_count: int = 0
    inflight: int = 0
    max_inflight: int = 0
    slow_delay: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)
    port: int = 0
    received_methods: list[str] = field(default_factory=list)
    received_headers: dict[str, str] = field(default_factory=dict)
    _server: uvicorn.Server | None = None
    _thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def set_health(self, *, status: int = 200, delay: float = 0.0) -> None:
        with self.lock:
            self.health_status = status
            self.health_delay = delay

    def _build_app(self) -> FastAPI:
        fake = self
        upstream = FastAPI()

        @upstream.get("/health")
        async def health():
            from fastapi.responses import Response

            if fake.health_delay:
                await asyncio.sleep(fake.health_delay)
            return Response(status_code=fake.health_status)

        @upstream.api_route(
            "/echo/{path:path}",
            methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        )
        async def echo(path: str, request: Request):
            body = await request.body()
            with fake.lock:
                fake.request_count += 1
                fake.inflight += 1
                fake.max_inflight = max(fake.max_inflight, fake.inflight)
                fake.received_methods.append(request.method)
                fake.received_headers = dict(request.headers)
            try:
                if fake.slow_delay:
                    await asyncio.sleep(fake.slow_delay)
                return {
                    "node": fake.node_id,
                    "path": path,
                    "query": request.url.query,
                    "method": request.method,
                    "body": body.decode(),
                    "headers": dict(request.headers),
                }
            finally:
                with fake.lock:
                    fake.inflight -= 1

        @upstream.get("/fail500")
        async def fail500():
            from fastapi.responses import JSONResponse

            with fake.lock:
                fake.request_count += 1
            return JSONResponse(status_code=500, content={"bad": True})

        @upstream.get("/slow")
        async def slow():
            with fake.lock:
                fake.request_count += 1
                fake.inflight += 1
            try:
                await asyncio.sleep(fake.slow_delay)
                return {"node": fake.node_id}
            finally:
                with fake.lock:
                    fake.inflight -= 1

        return upstream

    def start(self) -> "FakeUpstream":
        self.port = _free_port()
        config = uvicorn.Config(
            self._build_app(),
            host="127.0.0.1",
            port=self.port,
            log_level="error",
            lifespan="off",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run, daemon=True
        )
        self._thread.start()
        self._wait_ready()
        return self

    def _wait_ready(self, timeout: float = 5.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._server is not None and self._server.started:
                return
            time.sleep(0.01)
        raise RuntimeError(f"fake upstream {self.node_id} did not start")

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)
