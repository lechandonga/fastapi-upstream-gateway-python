import asyncio
import logging
from dataclasses import dataclass, field

from .errors import NodeAlreadyExistsError, NodeDrainingError, NodeNotFoundError

logger = logging.getLogger("gateway.registry")

# Node lifecycle states.
STATE_HEALTHY = "healthy"        # in the rotation as far as health allows
STATE_UNHEALTHY = "unhealthy"    # health checks marked it down
STATE_DRAINING = "draining"      # unregister requested, waiting on in-flight


@dataclass
class UpstreamNode:
    name: str
    url: str  # base url, e.g. http://127.0.0.1:9001
    healthy: bool = True
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_reason: str = "registered (assumed healthy)"
    in_flight: int = 0
    draining: bool = False

    def snapshot(self) -> dict:
        state = STATE_DRAINING if self.draining else (
            STATE_HEALTHY if self.healthy else STATE_UNHEALTHY
        )
        return {
            "name": self.name,
            "url": self.url,
            "healthy": self.healthy,
            "state": state,
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

    async def unregister(self, name: str) -> UpstreamNode:
        async with self._lock:
            return self._unregister_locked(name)

    def _unregister_locked(self, name: str) -> UpstreamNode:
        node = self._nodes.get(name)
        if node is None:
            raise NodeNotFoundError(f"upstream node {name!r} is not registered")
        if node.draining:
            raise NodeDrainingError(
                f"upstream node {name!r} is already draining "
                f"({node.in_flight} in-flight request(s) remaining)"
            )
        if node.in_flight == 0:
            self._nodes.pop(name)
            logger.info("node unregistered immediately: name=%s", name)
        else:
            node.draining = True
            logger.info(
                "node draining: name=%s in_flight=%d", name, node.in_flight
            )
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

    async def release(self, node: UpstreamNode) -> bool:
        """Decrement the in-flight counter; return True if the node has now
        fully drained and was removed."""
        async with self._lock:
            node.in_flight = max(0, node.in_flight - 1)
            removed = False
            if node.draining and node.in_flight == 0:
                existing = self._nodes.get(node.name)
                if existing is node:
                    self._nodes.pop(node.name)
                    removed = True
            logger.info(
                "release node=%s in_flight=%d draining=%s removed=%s",
                node.name,
                node.in_flight,
                node.draining,
                removed,
            )
            return removed


    @property
    def lock(self) -> asyncio.Lock:
        return self._lock
