from __future__ import annotations

import logging
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from app import Settings, create_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class GatewayServer:
    def __init__(self, settings: Settings):
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            create_app(settings),
            host="127.0.0.1",
            port=self.port,
            log_level="error",
            lifespan="on",
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> "GatewayServer":
        self.thread.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.server.started:
                return self
            time.sleep(0.01)
        raise RuntimeError("gateway did not start")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=10)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        strategy="round_robin",
        health_interval=0.2,
        health_timeout=0.5,
        unhealthy_threshold=3,
        healthy_threshold=2,
        request_timeout=0.8,
        max_retries=1,
    )


@pytest.fixture
def gateway(settings):
    server = GatewayServer(settings).start()
    try:
        yield server
    finally:
        server.stop()


def pytest_configure(config):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
