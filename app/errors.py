"""Error types shared by registry, load balancer and proxy."""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    UPSTREAM_ALREADY_REGISTERED = "upstream_already_registered"
    UPSTREAM_NOT_FOUND = "upstream_not_found"
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_CONNECT_ERROR = "upstream_connect_error"
    UPSTREAM_5XX = "upstream_5xx"
    UPSTREAM_BAD_GATEWAY = "upstream_bad_gateway"


class GatewayError(Exception):
    def __init__(self, code: ErrorCode, message: str, status_code: int = 502):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
