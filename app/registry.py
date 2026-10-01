"""Upstream node registry with threshold-based health probing."""
from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass, field

import httpx

from .config import Settings
from .errors import ErrorCode, GatewayError

HEALTH_PATH = "/health"


@dataclass
class Node:
    node_id: str
    base_url: str
    healthy: bool = True
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    inflight: int = 0
    last_reason: str = "registered (assumed healthy until probes prove otherwise)"
    last_check_at: float | None = None
    registered_at: float = field(default_factory=time.time)

    def snapshot(self) -> dict:
        return {
            "id": self.node_id,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "inflight": self.inflight,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "last_reason": self.last_reason,
            "last_check_at": self.last_check_at,
        }


class UpstreamRegistry:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self._settings = settings
        self._client = client
        self._owns_client = client is None
        self._nodes: dict[str, Node] = {}
        self._probe_tasks: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._rr_counter = itertools.count()
        self._log = _get_logger()

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._settings.health_timeout)

    async def close(self) -> None:
        for task in list(self._probe_tasks.values()):
            task.cancel()
        for task in list(self._probe_tasks.values()):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._probe_tasks.clear()
        if self._owns_client and self._client is not None:
            await self._client.aclose()

    # -- registration -----------------------------------------------------

    async def register(self, node_id: str, base_url: str) -> Node:
        base_url = base_url.rstrip("/")
        async with self._lock:
            if node_id in self._nodes:
                raise GatewayError(
                    ErrorCode.UPSTREAM_ALREADY_REGISTERED,
                    f"upstream {node_id!r} is already registered",
                    status_code=409,
                )
            node = Node(node_id=node_id, base_url=base_url)
            self._nodes[node_id] = node
            self._log.info(
                "register node id=%s url=%s state=healthy reason=%s",
                node_id, base_url, node.last_reason,
            )
        task = asyncio.create_task(self._probe_loop(node_id))
        self._probe_tasks[node_id] = task
        return node

    async def deregister(self, node_id: str) -> None:
        async with self._lock:
            if node_id not in self._nodes:
                raise GatewayError(
                    ErrorCode.UPSTREAM_NOT_FOUND,
                    f"upstream {node_id!r} is not registered",
                    status_code=404,
                )
            del self._nodes[node_id]
            self._log.info("deregister node id=%s", node_id)
        task = self._probe_tasks.pop(node_id, None)
        if task is not None:
            task.cancel()

    # -- selection --------------------------------------------------------

    async def select(self, exclude: set[str] | None = None) -> Node:
        exclude = exclude or set()
        async with self._lock:
            candidates = [
                n for n in self._nodes.values()
                if n.healthy and n.node_id not in exclude
            ]
            if not candidates:
                self._log.info(
                    "select failed strategy=%s excluded=%s reason=no healthy upstream",
                    self._settings.strategy, sorted(exclude),
                )
                raise GatewayError(
                    ErrorCode.NO_HEALTHY_UPSTREAM,
                    "no healthy upstream available",
                    status_code=503,
                )
            if self._settings.strategy == "least_inflight":
                node = min(
                    candidates,
                    key=lambda n: (n.inflight, next(self._rr_counter)),
                )
            else:
                index = next(self._rr_counter) % len(candidates)
                node = candidates[index]
            node.inflight += 1
            self._log.info(
                "select ok strategy=%s id=%s url=%s inflight=%d candidates=%d",
                self._settings.strategy, node.node_id, node.base_url,
                node.inflight, len(candidates),
            )
            return node

    async def release(self, node: Node) -> None:
        async with self._lock:
            node.inflight = max(0, node.inflight - 1)
            self._log.info(
                "release id=%s inflight=%d", node.node_id, node.inflight,
            )

    def set_strategy(self, strategy: str) -> None:
        if strategy not in ("round_robin", "least_inflight"):
            raise ValueError(
                "strategy must be 'round_robin' or 'least_inflight'"
            )
        self._settings.strategy = strategy
        self._log.info("strategy switched to=%s", strategy)

    @property
    def strategy(self) -> str:
        return self._settings.strategy

    # -- health bookkeeping ----------------------------------------------

    async def _record_probe(
        self, node: Node, success: bool, reason: str
    ) -> None:
        async with self._lock:
            node.last_check_at = time.time()
            node.last_reason = reason
            before = node.healthy
            if success:
                node.consecutive_successes += 1
                node.consecutive_failures = 0
                if not node.healthy and (
                    node.consecutive_successes
                    >= self._settings.healthy_threshold
                ):
                    node.healthy = True
            else:
                node.consecutive_failures += 1
                node.consecutive_successes = 0
                if node.healthy and (
                    node.consecutive_failures
                    >= self._settings.unhealthy_threshold
                ):
                    node.healthy = False
            if before != node.healthy:
                self._log.info(
                    "state change id=%s %s->%s reason=%s",
                    node.node_id,
                    "healthy" if before else "unhealthy",
                    "healthy" if node.healthy else "unhealthy",
                    reason,
                )
            else:
                self._log.info(
                    "probe id=%s success=%s state=%s fail=%d ok=%d reason=%s",
                    node.node_id, success,
                    "healthy" if node.healthy else "unhealthy",
                    node.consecutive_failures, node.consecutive_successes,
                    reason,
                )

    async def record_request_result(
        self, node: Node, success: bool, reason: str
    ) -> None:
        """Real-traffic feedback.

        Failures count toward the unhealthy threshold so a broken node is
        ejected quickly; recovery happens only through periodic probes.
        """
        async with self._lock:
            before = node.healthy
            if success:
                node.consecutive_failures = 0
            else:
                node.consecutive_failures += 1
                node.consecutive_successes = 0
                node.last_reason = f"traffic: {reason}"
                if node.healthy and (
                    node.consecutive_failures
                    >= self._settings.unhealthy_threshold
                ):
                    node.healthy = False
            if before != node.healthy:
                self._log.info(
                    "state change id=%s healthy->unhealthy reason=traffic: %s",
                    node.node_id, reason,
                )

    # -- probing ----------------------------------------------------------

    async def _probe_once(self, node: Node) -> tuple[bool, str]:
        url = node.base_url + HEALTH_PATH
        try:
            resp = await self._client.get(url)
        except httpx.TimeoutException:
            return False, "probe timeout"
        except httpx.TransportError as exc:
            return False, f"probe connection failed: {exc.__class__.__name__}"
        if 200 <= resp.status_code < 500:
            return True, f"probe {resp.status_code}"
        return False, f"probe {resp.status_code}"

    async def _probe_loop(self, node_id: str) -> None:
        interval = self._settings.health_interval
        try:
            while True:
                await asyncio.sleep(interval)
                node = self._nodes.get(node_id)
                if node is None:
                    return
                success, reason = await self._probe_once(node)
                await self._record_probe(node, success, reason)
        except asyncio.CancelledError:
            return

    # -- introspection ----------------------------------------------------

    async def status(self) -> list[dict]:
        async with self._lock:
            return [n.snapshot() for n in self._nodes.values()]

    async def get(self, node_id: str) -> Node:
        async with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise GatewayError(
                    ErrorCode.UPSTREAM_NOT_FOUND,
                    f"upstream {node_id!r} is not registered",
                    status_code=404,
                )
            return node


def _get_logger():
    import logging

    return logging.getLogger("gateway.registry")
