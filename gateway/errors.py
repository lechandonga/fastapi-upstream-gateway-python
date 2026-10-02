class GatewayError(Exception):
    """Base class for gateway errors."""


class NodeAlreadyExistsError(GatewayError):
    """Raised when registering a node name that is already registered."""


class NodeNotFoundError(GatewayError):
    """Raised when unregistering or querying a node that does not exist."""


class NodeDrainingError(GatewayError):
    """Raised when unregistering a node that is already draining."""


class NoHealthyUpstreamError(GatewayError):
    """Raised when no healthy upstream node is available."""
