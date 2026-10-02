import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .balancer import Balancer
from .config import GatewayConfig
from .errors import NodeAlreadyExistsError, NodeDrainingError, NodeNotFoundError
from .health import HealthChecker
from .proxy import ProxyHandler
from .registry import NodeRegistry

logger = logging.getLogger("gateway.app")


class RegisterRequest(BaseModel):
    name: str
    url: str


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    config = config or GatewayConfig.from_env()
    registry = NodeRegistry()
    balancer = Balancer(registry, config.strategy)
    checker = HealthChecker(registry, config)
    proxy = ProxyHandler(balancer, config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await checker.start()
        yield
        await checker.stop()
        await proxy.close()

    app = FastAPI(lifespan=lifespan)
    app.state.config = config
    app.state.registry = registry
    app.state.checker = checker

    @app.exception_handler(NodeAlreadyExistsError)
    async def already_exists_handler(request: Request, exc: NodeAlreadyExistsError):
        return JSONResponse(
            status_code=409, content={"error": "node_already_exists", "detail": str(exc)}
        )

    @app.exception_handler(NodeNotFoundError)
    async def not_found_handler(request: Request, exc: NodeNotFoundError):
        return JSONResponse(
            status_code=404, content={"error": "node_not_found", "detail": str(exc)}
        )

    @app.exception_handler(NodeDrainingError)
    async def draining_handler(request: Request, exc: NodeDrainingError):
        return JSONResponse(
            status_code=409, content={"error": "node_draining", "detail": str(exc)}
        )

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/admin/upstreams", status_code=201)
    async def register_upstream(req: RegisterRequest):
        node = await registry.register(req.name, req.url)
        return node.snapshot()

    @app.delete("/admin/upstreams/{name}")
    async def unregister_upstream(name: str):
        node, already_draining = await registry.begin_drain(name)
        if already_draining:
            raise NodeDrainingError(
                f"upstream node {name!r} is already draining "
                f"({node.in_flight} in-flight request(s) remaining)"
            )

        async def drain_and_remove() -> None:
            await node.drained_event.wait()
            await registry.finish_drain(name)

        if node.in_flight == 0:
            # No traffic: preserve the previous immediate-removal behaviour,
            # including the guarantee that a follow-up DELETE already 404s.
            await registry.finish_drain(name)
            return JSONResponse(status_code=200, content=node.snapshot())

        # In-flight requests exist: the node is already out of the
        # selectable pool and gets removed in the background once they
        # finish. Reaching this line without awaiting anything means no
        # request can complete between begin_drain and this branch.
        asyncio.create_task(drain_and_remove())
        return JSONResponse(status_code=202, content=node.snapshot())

    @app.get("/admin/upstreams")
    async def list_upstreams():
        return {
            "strategy": balancer.strategy_name,
            "nodes": [n.snapshot() for n in registry.all()],
        }

    @app.post("/admin/upstreams/check")
    async def run_health_check():
        await checker.check_once()
        return {"nodes": [n.snapshot() for n in registry.all()]}

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    )
    async def gateway_proxy(request: Request, full_path: str):
        return await proxy.forward(request, full_path)

    return app
