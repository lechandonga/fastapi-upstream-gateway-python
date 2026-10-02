import gzip
import threading
import time

import httpx

from gateway.config import GatewayConfig


def register(client, gw, name, url):
    return client.post(f"{gw.url}/admin/upstreams", json={"name": name, "url": url})


def node_state(client, gw, name):
    data = client.get(f"{gw.url}/admin/upstreams").json()["nodes"]
    by_name = {n["name"]: n for n in data}
    return by_name.get(name)


def gw_config(**overrides):
    base = dict(
        health_interval=3600,
        failure_threshold=2,
        success_threshold=2,
        request_timeout=0.5,
        stream_idle_timeout=0.5,
        max_retries=1,
    )
    base.update(overrides)
    return GatewayConfig(**base)


# ---------- streaming / large responses / compression ----------


def test_large_streaming_body_byte_for_byte(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(f"{gw.url}/big")
    assert resp.status_code == 200
    assert len(resp.content) == 512 * 1024
    assert resp.content == b"x" * (512 * 1024)
    assert node_state(client, gw, "a")["in_flight"] == 0


def test_gzipped_response_passes_through_verbatim(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    def raw_get(url):
        with httpx.Client(timeout=10) as c:
            with c.stream("GET", url) as resp:
                return dict(resp.headers), b"".join(resp.iter_raw())

    # httpx auto-decompresses regular .content; compare at the wire level
    headers, body = raw_get(f"{gw.url}/gzip-me")
    assert headers["content-encoding"].lower() == "gzip"
    assert headers["x-upstream-tag"] == "kept"

    # the same body must match what a direct connection to the upstream yields
    _, direct_body = raw_get(f"{up.url}/gzip-me")
    assert body == direct_body

    # and it must decode to the real payload, i.e. not be truncated/corrupted
    decoded = gzip.decompress(body)
    assert decoded == ("gateway-gzip-payload-" * 4096).encode()


def test_chunked_response_reassembles_correctly(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(f"{gw.url}/chunked")
    assert resp.status_code == 200
    assert resp.text == "".join(f"chunk-{i}\n" for i in range(10))


def test_first_byte_arrives_before_full_body(client, gateway, upstream_factory):
    # idle timeout long enough that the held-back second chunk does not abort
    gw = gateway(gw_config(stream_idle_timeout=10.0, request_timeout=10.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    received: list[bytes] = []
    first_byte_at = []
    finished = threading.Event()

    def consume():
        with client.stream("GET", f"{gw.url}/slow-stream") as resp:
            for chunk in resp.iter_bytes():
                if not first_byte_at:
                    first_byte_at.append(time.monotonic())
                received.append(chunk)
        finished.set()

    t = threading.Thread(target=consume)
    t.start()

    deadline = time.monotonic() + 5
    while not first_byte_at and time.monotonic() < deadline:
        time.sleep(0.02)
    assert first_byte_at, "client never received the first chunk"

    # in-flight stays held while the body is still open
    time.sleep(0.2)
    state = node_state(client, gw, "a")
    assert state["in_flight"] == 1

    # release the upstream gate; the final chunk flows and counters drop
    assert client.post(f"{up.url}/release-slow-stream").status_code == 200
    assert finished.wait(5)
    t.join(timeout=5)
    assert b"first\n" in b"".join(received) and b"second\n" in b"".join(received)
    assert node_state(client, gw, "a")["in_flight"] == 0


def test_stream_idle_timeout_aborts_stalled_upstream(client, gateway, upstream_factory):
    gw = gateway(gw_config(stream_idle_timeout=0.3))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    start = time.monotonic()
    resp = client.get(f"{gw.url}/slow-stream")
    elapsed = time.monotonic() - start
    assert resp.status_code == 200
    assert resp.content == b"first\n"
    assert elapsed < 3  # bounded by the idle gap, not by hanging forever
    assert node_state(client, gw, "a")["in_flight"] == 0


def test_continuously_streaming_body_not_cut_by_total_duration(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(request_timeout=0.3, stream_idle_timeout=0.3))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    start = time.monotonic()
    resp = client.get(
        f"{gw.url}/steady-stream", params={"duration": "1.2", "interval": "0.05"}
    )
    assert resp.status_code == 200
    assert time.monotonic() - start > 1.0
    lines = resp.text.strip().splitlines()
    assert int(lines[-1]) >= 15  # stream ran to completion despite 0.3s timeout
    assert node_state(client, gw, "a")["in_flight"] == 0


def test_client_disconnect_mid_stream_does_not_leak(client, gateway, upstream_factory):
    gw = gateway(gw_config(stream_idle_timeout=10.0, request_timeout=10.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    short = httpx.Client(timeout=10)
    with short.stream("GET", f"{gw.url}/slow-stream") as resp:
        first = next(resp.iter_bytes())
        assert first == b"first\n"
        state = node_state(client, gw, "a")
        assert state["in_flight"] == 1
    short.close()  # explicit client abort while upstream is still holding back

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if node_state(client, gw, "a")["in_flight"] == 0:
            break
        time.sleep(0.05)
    assert node_state(client, gw, "a")["in_flight"] == 0


# ---------- header policy ----------


def test_configured_request_headers_never_leave_gateway(
    client, gateway, upstream_factory
):
    gw = gateway(
        gw_config(blocked_request_headers=["x-internal-token", "X-Internal-Mark"])
    )
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(
        f"{gw.url}/headers-debug",
        headers={
            "x-internal-token": "topsecret",
            "x-internal-mark": "1",
            "x-normal": "ok",
        },
    )
    seen = resp.json()["headers"]
    assert "x-internal-token" not in seen
    assert "x-internal-mark" not in seen
    assert seen["x-normal"] == "ok"


def test_connection_declared_request_headers_are_stripped(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(
        f"{gw.url}/headers-debug",
        headers={
            "connection": "x-client-hop, keep-alive",
            "x-client-hop": "bye",
            "x-keep": "hi",
            "keep-alive": "timeout=5",
        },
    )
    seen = resp.json()["headers"]
    assert "x-client-hop" not in seen
    assert "keep-alive" not in seen
    assert seen["x-keep"] == "hi"


def test_response_blocklist_and_hop_declarations_stripped(
    client, gateway, upstream_factory
):
    gw = gateway(
        gw_config(blocked_response_headers=["x-internal-only"])
    )
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(f"{gw.url}/internal-response")
    assert resp.status_code == 200
    assert "x-internal-only" not in resp.headers
    assert resp.headers["x-public-tag"] == "visible"
    assert resp.headers.get_list("set-cookie") == ["a=1"]

    resp2 = client.get(f"{gw.url}/hop-response")
    assert "x-up-hop" not in resp2.headers
    assert "connection" not in resp2.headers
    assert resp2.headers["x-safe-resp"] == "kept"
    assert resp2.text == "hop-body"


# ---------- graceful drain ----------


def test_unregister_without_inflight_is_immediate(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.delete(f"{gw.url}/admin/upstreams/a")
    assert resp.status_code == 200
    assert resp.json()["state"] == "healthy"
    assert node_state(client, gw, "a") is None


def test_graceful_drain_keeps_node_until_inflight_finishes(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(stream_idle_timeout=10.0, request_timeout=10.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    got_first = threading.Event()

    def hold_stream():
        with client.stream("GET", f"{gw.url}/slow-stream") as resp:
            for chunk in resp.iter_bytes():
                if chunk:
                    got_first.set()

    t = threading.Thread(target=hold_stream)
    t.start()
    assert got_first.wait(5)

    draining = client.delete(f"{gw.url}/admin/upstreams/a")
    assert draining.status_code == 202
    body = draining.json()
    assert body["state"] == "draining"
    assert body["draining"] is True
    assert body["in_flight"] == 1

    state = node_state(client, gw, "a")
    assert state["state"] == "draining"
    assert state["in_flight"] == 1

    # repeated unregister while draining is a distinct, stable error
    again = client.delete(f"{gw.url}/admin/upstreams/a")
    assert again.status_code == 409
    assert again.json()["error"] == "node_draining"

    missing = client.delete(f"{gw.url}/admin/upstreams/ghost")
    assert missing.status_code == 404
    assert missing.json()["error"] == "node_not_found"

    # release the held stream; the node finishes draining and disappears
    assert client.post(f"{up.url}/release-slow-stream").status_code == 200
    t.join(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if node_state(client, gw, "a") is None:
            break
        time.sleep(0.05)
    assert node_state(client, gw, "a") is None


def test_no_new_request_routed_to_draining_node(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(stream_idle_timeout=10.0, request_timeout=10.0))
    a = upstream_factory("a")
    b = upstream_factory("b")
    register(client, gw, "a", a.url)
    register(client, gw, "b", b.url)

    got_first = threading.Event()

    def hold_stream():
        with client.stream("GET", f"{gw.url}/slow-stream") as resp:
            for chunk in resp.iter_bytes():
                if chunk:
                    got_first.set()

    # round-robin first pick is "a"
    t = threading.Thread(target=hold_stream)
    t.start()
    assert got_first.wait(5)

    drain = client.delete(f"{gw.url}/admin/upstreams/a")
    assert drain.status_code == 202

    # every new request must land on "b"; draining "a" gets no new traffic
    for _ in range(4):
        resp = client.get(f"{gw.url}/echo")
        assert resp.json()["node"] == "b"

    client.post(f"{a.url}/release-slow-stream")
    t.join(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and node_state(client, gw, "a") is not None:
        time.sleep(0.05)
    assert node_state(client, gw, "a") is None


def test_concurrent_unregister_and_requests_do_not_overselect(
    client, gateway, upstream_factory
):
    gw = gateway(
        gw_config(
            strategy="least_in_flight",
            stream_idle_timeout=10.0,
            request_timeout=10.0,
        )
    )
    a = upstream_factory("a")
    b = upstream_factory("b")
    register(client, gw, "a", a.url)
    register(client, gw, "b", b.url)

    got_first = threading.Event()

    def hold_stream():
        with client.stream("GET", f"{gw.url}/slow-stream") as resp:
            for chunk in resp.iter_bytes():
                if chunk:
                    got_first.set()

    t = threading.Thread(target=hold_stream)
    t.start()
    assert got_first.wait(5)
    held_node = "a"  # least-in-flight with empty pool picks "a" first
    assert node_state(client, gw, held_node)["in_flight"] == 1

    def delete_held():
        return client.delete(f"{gw.url}/admin/upstreams/{held_node}")

    dt = threading.Thread(target=delete_held)
    dt.start()
    time.sleep(0.3)

    # concurrent new traffic must only go to the still-available node
    picked = client.get(f"{gw.url}/echo").json()["node"]
    assert picked == "b"

    dt.join(timeout=5)
    client.post(f"{a.url}/release-slow-stream")
    t.join(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and node_state(client, gw, held_node) is not None:
        time.sleep(0.05)
    assert node_state(client, gw, "a") is None
    assert node_state(client, gw, "b")["in_flight"] == 0
