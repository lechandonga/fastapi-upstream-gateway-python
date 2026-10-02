import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


def _get_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        return list(default)
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


@dataclass
class GatewayConfig:
    strategy: str = "round_robin"  # round_robin | least_in_flight
    health_interval: float = 5.0  # seconds between health probe rounds
    failure_threshold: int = 3  # consecutive failures before unhealthy
    success_threshold: int = 2  # consecutive successes before healthy again
    request_timeout: float = 5.0  # per-attempt upstream timeout (seconds; idle timeout while streaming)
    max_retries: int = 2  # extra attempts for idempotent requests
    health_path: str = "/health"
    # Extra header names (lowercase) that must never leave the gateway in
    # either direction, on top of the standard hop-by-hop set and the
    # per-request Connection header nominations.
    block_request_headers: list[str] | None = None
    block_response_headers: list[str] | None = None

    def __post_init__(self) -> None:
        self.block_request_headers = [
            h.lower() for h in (self.block_request_headers or [])
        ]
        self.block_response_headers = [
            h.lower() for h in (self.block_response_headers or [])
        ]

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
            block_request_headers=_get_list("GATEWAY_BLOCK_REQUEST_HEADERS", []),
            block_response_headers=_get_list("GATEWAY_BLOCK_RESPONSE_HEADERS", []),
        )
