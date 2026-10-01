import asyncio
import logging
from dataclasses import dataclass, field

from .errors import NodeAlreadyExistsError, NodeNotFoundError

logger = logging.getLogger("gateway.registry")


@dataclass
class UpstreamNode:
    name: str
    url: str  # base url, e.g. http://127.0.0.1:9001
    healthy: bool = True
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_reason: str = "registered (assumed healthy)"
    in_flight: int = 0

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "healthy": self.healthy,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "last_reason": self.last_reason,
            "in_flight": self.in_flight,
        }


class NodeRegistry:
    def __init__(self) -> None:
        self._nodes: dict[str, UpstreamNode] = {}
        self._lock = asyncio.Lock()

    async def register(self, name: str, url: str) -> UpstreamNode:
        async with self._lock:
            if name in self._nodes:
                raise NodeAlreadyExistsError(f"upstream node {name!r} already registered")
            node = UpstreamNode(name=name, url=url.rstrip("/"))
            self._nodes[name] = node
            logger.info("node registered: name=%s url=%s", name, node.url)
            return node

    async def unregister(self, name: str) -> UpstreamNode:
        async with self._lock:
            node = self._nodes.pop(name, None)
            if node is None:
                raise NodeNotFoundError(f"upstream node {name!r} is not registered")
            logger.info("node unregistered: name=%s", name)
            return node

    def get(self, name: str) -> UpstreamNode:
        node = self._nodes.get(name)
        if node is None:
            raise NodeNotFoundError(f"upstream node {name!r} is not registered")
        return node

    def all(self) -> list[UpstreamNode]:
        return list(self._nodes.values())

    def healthy_nodes(self) -> list[UpstreamNode]:
        return [n for n in self._nodes.values() if n.healthy]

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock
