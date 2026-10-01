import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


@dataclass
class GatewayConfig:
    strategy: str = "round_robin"  # round_robin | least_in_flight
    health_interval: float = 5.0  # seconds between health probe rounds
    failure_threshold: int = 3  # consecutive failures before unhealthy
    success_threshold: int = 2  # consecutive successes before healthy again
    request_timeout: float = 5.0  # per-attempt upstream timeout (seconds)
    max_retries: int = 2  # extra attempts for idempotent requests
    health_path: str = "/health"

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            strategy=os.getenv("GATEWAY_STRATEGY", "round_robin"),
            health_interval=_get_float("GATEWAY_HEALTH_INTERVAL", 5.0),
            failure_threshold=_get_int("GATEWAY_FAILURE_THRESHOLD", 3),
            success_threshold=_get_int("GATEWAY_SUCCESS_THRESHOLD", 2),
            request_timeout=_get_float("GATEWAY_REQUEST_TIMEOUT", 5.0),
            max_retries=_get_int("GATEWAY_MAX_RETRIES", 2),
            health_path=os.getenv("GATEWAY_HEALTH_PATH", "/health"),
        )
