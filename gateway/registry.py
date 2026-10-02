import asyncio
import logging
from dataclasses import dataclass, field

from .errors import NodeAlreadyExistsError, NodeDrainingError, NodeNotFoundError

logger = logging.getLogger("gateway.registry")


@dataclass
class UpstreamNode:
    name: str
    url: str  # base url, e.g. http://127.0.0.1:9001
    healthy: bool = True
    # "active" takes traffic; "draining" is out of the pool and waits for
    # in-flight requests to finish before being removed.
    state: str = "active"
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_reason: str = "registered (assumed healthy)"
    in_flight: int = 0
    drained_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    @property
    def draining(self) -> bool:
        return self.state == "draining"

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "healthy": self.healthy,
            "state": self.state,
            "draining": self.draining,
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

    async def begin_drain(self, name: str) -> tuple[UpstreamNode, bool]:
        """Remove a node from the selectable pool.

        Returns ``(node, already_draining)``. The node is *not* removed from
        the registry dict until :meth:`finish_drain` drops it; callers (the
        proxy layer) wait on ``node.drained_event`` and then remove it.
        """
        async with self._lock:
            node = self._nodes.get(name)
            if node is None:
                raise NodeNotFoundError(f"upstream node {name!r} is not registered")
            if node.draining:
                return node, True
            node.state = "draining"
            logger.info(
                "node draining: name=%s in_flight=%d (removed from pool)",
                name,
                node.in_flight,
            )
            if node.in_flight == 0:
                node.drained_event.set()
            return node, False

    async def finish_drain(self, name: str) -> UpstreamNode | None:
        """Drop a drained node. Returns None if it was already removed."""
        async with self._lock:
            node = self._nodes.pop(name, None)
        if node is not None:
            node.drained_event.set()
            logger.info("node unregistered: name=%s", name)
        return node

    async def unregister(self, name: str) -> UpstreamNode:
        """Backward-compatible immediate unregister (node must be active)."""
        node, already = await self.begin_drain(name)
        if already:
            raise NodeDrainingError(f"upstream node {name!r} is already draining")
        await self.finish_drain(name)
        return node

    def get(self, name: str) -> UpstreamNode:
        node = self._nodes.get(name)
        if node is None:
            raise NodeNotFoundError(f"upstream node {name!r} is not registered")
        return node

    def all(self) -> list[UpstreamNode]:
        return list(self._nodes.values())

    def healthy_nodes(self) -> list[UpstreamNode]:
        return [n for n in self._nodes.values() if n.healthy and not n.draining]

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock
