import asyncio
import logging

import httpx

from .config import GatewayConfig
from .registry import NodeRegistry, UpstreamNode

logger = logging.getLogger("gateway.health")


class HealthChecker:
    """Periodically probes every registered node. A node only flips state
    after `failure_threshold` consecutive failures or `success_threshold`
    consecutive successes, so transient blips never cause flapping."""

    def __init__(self, registry: NodeRegistry, config: GatewayConfig) -> None:
        self._registry = registry
        self._config = config
        self._client = httpx.AsyncClient(timeout=config.request_timeout)
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._client.aclose()

    async def _run_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.health_interval)
            await self.check_once()

    async def check_once(self) -> None:
        for node in self._registry.all():
            if node.draining:
                continue
            await self._probe(node)

    async def _probe(self, node: UpstreamNode) -> None:
        url = node.url + self._config.health_path
        try:
            resp = await self._client.get(url)
            if resp.status_code < 500:
                self._record_success(node, f"probe ok (status={resp.status_code})")
            else:
                self._record_failure(node, f"probe got status={resp.status_code}")
        except httpx.TimeoutException:
            self._record_failure(node, "probe timed out")
        except httpx.HTTPError as exc:
            self._record_failure(node, f"probe connect error: {exc.__class__.__name__}")

    def _record_success(self, node: UpstreamNode, reason: str) -> None:
        node.consecutive_failures = 0
        node.consecutive_successes += 1
        node.last_reason = reason
        if not node.healthy and node.consecutive_successes >= self._config.success_threshold:
            node.healthy = True
            logger.info(
                "state change node=%s unhealthy->healthy reason=%s (after %d consecutive successes)",
                node.name,
                reason,
                node.consecutive_successes,
            )

    def _record_failure(self, node: UpstreamNode, reason: str) -> None:
        node.consecutive_successes = 0
        node.consecutive_failures += 1
        node.last_reason = reason
        if node.healthy and node.consecutive_failures >= self._config.failure_threshold:
            node.healthy = False
            logger.info(
                "state change node=%s healthy->unhealthy reason=%s (after %d consecutive failures)",
                node.name,
                reason,
                node.consecutive_failures,
            )
