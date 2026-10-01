"""FastAPI application: admin API plus the catch-all forwarding route."""
from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from typing import Literal

from pydantic import BaseModel

from .config import Settings
from .errors import GatewayError
from .proxy import HOP_BY_HOP, forward
from .registry import UpstreamRegistry


class RegisterIn(BaseModel):
    id: str
    base_url: str


class StrategyIn(BaseModel):
    strategy: Literal["round_robin", "least_inflight"]


# Response headers that must be regenerated for the returned body.
_DROP_RESPONSE_HEADERS = HOP_BY_HOP | {"content-length", "content-encoding"}


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.registry = registry = UpstreamRegistry(settings)
        await registry.start()
        app.state.http_client = httpx.AsyncClient()
        try:
            yield
        finally:
            await registry.close()
            await app.state.http_client.aclose()

    app = FastAPI(title="fastapi-upstream-gateway", lifespan=lifespan)

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, exc: GatewayError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code.value, "detail": exc.message},
        )

    # Original service interface, kept intact.
    @app.get("/", include_in_schema=False)
    def read_root():
        return {"Hello": "World"}

    # -- admin API --------------------------------------------------------

    @app.post("/admin/upstreams", status_code=201)
    async def register_upstream(payload: RegisterIn):
        node = await app.state.registry.register(payload.id, payload.base_url)
        return node.snapshot()

    @app.delete("/admin/upstreams/{node_id}", status_code=204)
    async def deregister_upstream(node_id: str):
        await app.state.registry.deregister(node_id)
        return Response(status_code=204)

    @app.get("/admin/upstreams")
    async def list_upstreams():
        return await app.state.registry.status()

    @app.get("/admin/upstreams/{node_id}")
    async def get_upstream(node_id: str):
        node = await app.state.registry.get(node_id)
        return node.snapshot()

    @app.get("/admin/strategy")
    async def get_strategy():
        return {"strategy": app.state.registry.strategy}

    @app.put("/admin/strategy")
    async def put_strategy(payload: StrategyIn):
        app.state.registry.set_strategy(payload.strategy)
        return {"strategy": app.state.registry.strategy}

    # -- gateway ----------------------------------------------------------

    @app.api_route(
        "/gateway/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
        include_in_schema=False,
    )
    async def gateway_proxy(path: str, request: Request):
        body = await request.body()
        resp = await forward(
            app.state.registry,
            settings,
            app.state.http_client,
            method=request.method,
            path=path,
            query=request.url.query.encode("ascii"),
            headers=dict(request.headers),
            body=body,
            peer_host=request.client.host if request.client else "127.0.0.1",
            scheme=request.url.scheme,
        )
        passthrough = {
            key: value
            for key, value in resp.headers.items()
            if key.lower() not in _DROP_RESPONSE_HEADERS
        }
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=passthrough,
        )

    return app
