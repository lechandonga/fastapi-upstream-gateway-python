"""Gateway configuration.

Values can be overridden through environment variables prefixed with
``GW_`` (e.g. ``GW_STRATEGY=least_inflight``).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


@dataclass
class Settings:
    strategy: str = "round_robin"  # round_robin | least_inflight
    health_interval: float = 1.0
    health_timeout: float = 1.0
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2
    request_timeout: float = 5.0
    max_retries: int = 1

    @classmethod
    def from_env(cls) -> "Settings":
        strategy = os.getenv("GW_STRATEGY", cls.strategy)
        if strategy not in ("round_robin", "least_inflight"):
            raise ValueError(
                f"invalid GW_STRATEGY={strategy!r}, "
                "expected 'round_robin' or 'least_inflight'"
            )
        return cls(
            strategy=strategy,
            health_interval=_env_float("GW_HEALTH_INTERVAL", 1.0),
            health_timeout=_env_float("GW_HEALTH_TIMEOUT", 1.0),
            unhealthy_threshold=_env_int("GW_UNHEALTHY_THRESHOLD", 3),
            healthy_threshold=_env_int("GW_HEALTHY_THRESHOLD", 2),
            request_timeout=_env_float("GW_REQUEST_TIMEOUT", 5.0),
            max_retries=_env_int("GW_MAX_RETRIES", 1),
        )
