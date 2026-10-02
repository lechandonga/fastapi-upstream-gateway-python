import logging

from .errors import NoHealthyUpstreamError
from .registry import NodeRegistry, UpstreamNode

logger = logging.getLogger("gateway.balancer")


class RoundRobinStrategy:
    name = "round_robin"

    def __init__(self) -> None:
        self._cursor = 0

    def select(self, candidates: list[UpstreamNode], exclude: set[str]) -> UpstreamNode:
        pool = [n for n in candidates if n.name not in exclude]
        if not pool:
            raise NoHealthyUpstreamError("no healthy upstream node available")
        node = pool[self._cursor % len(pool)]
        self._cursor += 1
        return node


class LeastInFlightStrategy:
    name = "least_in_flight"

    def __init__(self) -> None:
        self._cursor = 0

    def select(self, candidates: list[UpstreamNode], exclude: set[str]) -> UpstreamNode:
        pool = [n for n in candidates if n.name not in exclude]
        if not pool:
            raise NoHealthyUpstreamError("no healthy upstream node available")
        best = min(n.in_flight for n in pool)
        tied = [n for n in pool if n.in_flight == best]
        node = tied[self._cursor % len(tied)]
        self._cursor += 1
        return node


STRATEGIES = {
    RoundRobinStrategy.name: RoundRobinStrategy,
    LeastInFlightStrategy.name: LeastInFlightStrategy,
}


class Balancer:
    """Picks nodes and tracks in-flight counters under the registry lock."""

    def __init__(self, registry: NodeRegistry, strategy_name: str) -> None:
        if strategy_name not in STRATEGIES:
            raise ValueError(
                f"unknown strategy {strategy_name!r}, expected one of {sorted(STRATEGIES)}"
            )
        self._registry = registry
        self._strategy = STRATEGIES[strategy_name]()

    @property
    def strategy_name(self) -> str:
        return self._strategy.name

    async def acquire(self, exclude: set[str] | None = None) -> UpstreamNode:
        async with self._registry.lock:
            node = self._strategy.select(self._registry.healthy_nodes(), exclude or set())
            node.in_flight += 1
            logger.info(
                "select node=%s strategy=%s in_flight=%d excluded=%s",
                node.name,
                self._strategy.name,
                node.in_flight,
                sorted(exclude or ()),
            )
            return node

    async def release(self, node: UpstreamNode) -> None:
        await self._registry.release(node)
