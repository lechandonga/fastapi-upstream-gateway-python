"""Configurable, strict header policy for both proxy directions.

Two independent rules are applied per message:

* A fixed set of HTTP/1.1 hop-by-hop headers (RFC 7230 6.1) is always
  stripped.  In addition, headers explicitly listed in the message's own
  ``Connection`` header are hop-by-hop by declaration and are stripped too.
* Operator-configured denylists (``GATEWAY_BLOCKED_REQUEST_HEADERS`` /
  ``GATEWAY_BLOCKED_RESPONSE_HEADERS``) name headers that must never cross
  the gateway regardless of how the peer sends them.

Header name matching is always case-insensitive.
"""

from collections.abc import Iterable

# RFC 7230 section 6.1: the standard hop-by-hop headers.
HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)


def _normalise(names: Iterable[str]) -> set[str]:
    return {name.strip().lower() for name in names if name.strip()}


def filter_headers(
    headers: Iterable[tuple[str, str]],
    blocked: Iterable[str] = (),
) -> list[tuple[str, str]]:
    """Return header pairs with hop-by-hop and denied names removed.

    The (possibly repeated) ``Connection`` header itself is always removed,
    along with every header it names.
    """
    blocked_set = _normalise(blocked)

    def lowered(name) -> str:
        return name.decode("latin-1").strip().lower() if isinstance(name, bytes) else name.strip().lower()

    # First pass: collect everything declared hop-by-hop across all
    # Connection header values.
    declared: set[str] = set()
    pairs: list[tuple[str, str]] = []
    for name, value in headers:
        pairs.append((name, value))
        if lowered(name) == "connection":
            raw_value = value.decode("latin-1") if isinstance(value, bytes) else value
            for token in raw_value.split(","):
                token = token.strip().lower()
                if token and token != "close":
                    declared.add(token)

    forbidden = HOP_BY_HOP_HEADERS | blocked_set | declared
    return [(name, value) for name, value in pairs if lowered(name) not in forbidden]
