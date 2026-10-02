import os
from dataclasses import dataclass, field


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


def _get_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _get_optional_float(name: str) -> float | None:
    raw = os.getenv(name)
    return float(raw) if raw is not None else None


@dataclass
class GatewayConfig:
    strategy: str = "round_robin"  # round_robin | least_in_flight
    health_interval: float = 5.0  # seconds between health probe rounds
    failure_threshold: int = 3  # consecutive failures before unhealthy
    success_threshold: int = 2  # consecutive successes before healthy again
    request_timeout: float = 5.0  # per-attempt upstream timeout (seconds)
    max_retries: int = 2  # extra attempts for idempotent requests
    health_path: str = "/health"
    # headers that must never be sent to upstreams (request direction) or
    # returned to clients verbatim (response direction), header names
    # case-insensitive, comma separated in the env vars
    blocked_request_headers: list[str] = field(default_factory=list)
    blocked_response_headers: list[str] = field(default_factory=list)
    # idle gap timeout for streamed bodies: no bytes from upstream for this
    # long aborts the stream; a stream that keeps producing data is never cut
    # regardless of its total duration. When None, request_timeout applies.
    stream_idle_timeout: float | None = None

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
            blocked_request_headers=_get_list("GATEWAY_BLOCKED_REQUEST_HEADERS", []),
            blocked_response_headers=_get_list("GATEWAY_BLOCKED_RESPONSE_HEADERS", []),
            stream_idle_timeout=_get_optional_float("GATEWAY_STREAM_IDLE_TIMEOUT"),
        )

    @property
    def effective_stream_idle_timeout(self) -> float:
        return (
            self.stream_idle_timeout
            if self.stream_idle_timeout is not None
            else self.request_timeout
        )
