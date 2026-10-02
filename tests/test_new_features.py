import threading
import time

import httpx

from gateway.config import GatewayConfig
from tests.conftest import RawHttpUpstream


def register(client, gw, name, url):
    return client.post(f"{gw.url}/admin/upstreams", json={"name": name, "url": url})


def node_state(client, gw, name):
    listing = client.get(f"{gw.url}/admin/upstreams").json()["nodes"]
    for n in listing:
        if n["name"] == name:
            return n
    return None


def admin_unregister(gw, name):
    return f"{gw.url}/admin/upstreams/{name}"


def gw_config(**overrides):
    base = dict(
        health_interval=3600,
        failure_threshold=2,
        success_threshold=2,
        request_timeout=2.0,
        max_retries=2,
    )
    base.update(overrides)
    return GatewayConfig(**base)


# ---------------------------------------------------------------------------
# Streaming / large / compressed response passthrough
# ---------------------------------------------------------------------------

def test_large_response_body_is_byte_identical_to_direct(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    direct = client.get(f"{up.url}/large", params={"size": 512_000})
    via = client.get(f"{gw.url}/large", params={"size": 512_000})

    assert via.status_code == 200
    assert via.content == direct.content
    assert len(via.content) == 512_000


def test_gzipped_response_is_forwarded_intact_and_decodes(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    direct = client.get(f"{up.url}/gzip", headers={"accept-encoding": "identity"})
    via = client.get(f"{gw.url}/gzip", headers={"accept-encoding": "identity"})

    assert via.status_code == 200
    assert via.headers.get("content-encoding") == "gzip"
    assert via.content == direct.content
    # both must parse as the same JSON payload
    assert via.json() == direct.json()


def test_chunked_streaming_response_arrives_complete(client, gateway, upstream_factory):
    gw = gateway(gw_config(request_timeout=2.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(f"{gw.url}/stream", params={"chunks": 8, "interval": 0.05})
    assert resp.status_code == 200
    lines = resp.text.splitlines()
    assert lines == [f"chunk-{i:04d}" for i in range(8)]


def test_first_chunk_flushed_early_without_buffering_whole_stream(client, gateway, upstream_factory):
    gw = gateway(gw_config(request_timeout=1.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    # 25 chunks * 0.2s = 5s total, longer than the 1s request timeout.
    # It must still succeed because data keeps flowing (idle timeout only).
    with client.stream(
        "GET", f"{gw.url}/stream", params={"chunks": 25, "interval": 0.2}
    ) as resp:
        assert resp.status_code == 200
        chunks = list(resp.iter_bytes())
    body = b"".join(chunks)
    assert body.split(b"\n", 1)[0] == b"chunk-0000"
    assert body.count(b"chunk-") == 25


def test_idle_stall_mid_stream_terminates_without_corrupting_counter(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(request_timeout=0.5))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(
        f"{gw.url}/stream",
        params={"chunks": 100, "interval": 0.05, "stall_after": 2},
        timeout=10,
    )
    assert resp.status_code == 200
    text = resp.text
    assert "chunk-0001" in text
    assert "chunk-0050" not in text

    # in-flight counter must fall back after the broken stream unwinds
    deadline_state = None
    for _ in range(50):
        deadline_state = node_state(client, gw, "a")
        if deadline_state["in_flight"] == 0:
            break

        time.sleep(0.05)
    assert deadline_state["in_flight"] == 0


def test_stall_before_first_chunk_times_out_and_retries(client, gateway, upstream_factory):
    gw = gateway(gw_config(request_timeout=0.3, max_retries=1))
    slow = upstream_factory("slow")
    fast = upstream_factory("fast")
    register(client, gw, "slow", slow.url)
    register(client, gw, "fast", fast.url)
    fast._server.config.app.state.slow_first = False

    # round-robin: first attempt goes to slow
    resp = client.get(f"{gw.url}/slow-first")
    assert resp.status_code == 200
    assert resp.content == b"late"
    # the slow upstream never produced a body chunk
    assert slow._server.config.app.state.hits.get("/slow-first", 0) == 1


def test_in_flight_counter_held_for_full_stream_duration(client, gateway, upstream_factory):
    gw = gateway(gw_config(request_timeout=1.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    observed = []
    stop = threading.Event()

    def reader():
        with httpx.Client(timeout=10) as c:
            with c.stream(
                "GET", f"{gw.url}/stream", params={"chunks": 10, "interval": 0.15}
            ) as resp:
                for _ in resp.iter_bytes():
                    if stop.is_set():
                        break

    t = threading.Thread(target=reader)
    t.start()


    time.sleep(0.5)
    mid = node_state(client, gw, "a")
    assert mid["in_flight"] >= 1

    t.join(timeout=10)
    stop.set()
    end = node_state(client, gw, "a")
    assert end["in_flight"] == 0


def test_client_abandoning_stream_does_not_leak_counter_or_connection(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(request_timeout=2.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    def abandon():
        with httpx.Client(timeout=10) as c:
            with c.stream(
                "GET", f"{gw.url}/stream", params={"chunks": 200, "interval": 0.1}
            ) as resp:
                next(resp.iter_bytes())
                # drop the stream without reading the rest

    th = threading.Thread(target=abandon)
    th.start()
    th.join(timeout=5)


    state = None
    for _ in range(100):
        state = node_state(client, gw, "a")
        if state["in_flight"] == 0:
            break
        time.sleep(0.05)
    assert state["in_flight"] == 0

    # gateway stays usable on the same node right afterwards
    follow = client.get(f"{gw.url}/echo")
    assert follow.status_code == 200


# ---------------------------------------------------------------------------
# Header policy
# ---------------------------------------------------------------------------

def test_configured_request_block_headers_never_reach_upstream(client, gateway, upstream_factory):
    cfg = gw_config(block_request_headers=["x-internal-token", "x-internal-flag"])
    gw = gateway(cfg)
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(
        f"{gw.url}/headers-echo",
        headers={
            "x-internal-token": "supersecret",
            "x-internal-flag": "1",
            "x-client-shared": "visible",
        },
    )
    seen = resp.json()["headers"]
    assert "x-internal-token" not in seen
    assert "x-internal-flag" not in seen
    assert seen.get("x-client-shared") == "visible"
    # standard forwarding headers are still added
    assert seen["x-forwarded-proto"] == "http"
    assert "x-forwarded-for" in seen


def test_connection_nominated_request_headers_are_stripped(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(
        f"{gw.url}/headers-echo",
        headers={
            "connection": "keep-alive, x-hop-token",
            "x-hop-token": "must-not-forward",
        },
    )
    seen = resp.json()["headers"]
    assert "x-hop-token" not in seen
    assert seen.get("connection", "").lower() != "keep-alive, x-hop-token"


def test_response_hop_and_blocked_headers_removed_inbound(client, gateway):
    raw = RawHttpUpstream()
    gw = gateway(gw_config(block_response_headers=["x-upstream-internal"]))
    try:
        with httpx.Client(timeout=10) as c:
            register(c, gw, "raw", raw.url)
            resp = c.get(f"{gw.url}/anything")
            assert resp.status_code == 200
            assert resp.content == b"raw-body-ok"
            lower = {k.lower() for k in resp.headers}
            assert "x-secret" not in lower          # Connection-nominated hop header
            assert "x-upstream-internal" not in lower  # configured blocklist
            assert resp.headers.get("x-shared") == "kept"
    finally:
        raw.stop()


# ---------------------------------------------------------------------------
# Graceful drain / unregister
# ---------------------------------------------------------------------------

def test_unregister_without_inflight_is_immediate(client, gateway, upstream_factory):
    gw = gateway(gw_config())
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.delete(f"{gw.url}/admin/upstreams/a")
    assert resp.status_code == 200
    assert node_state(client, gw, "a") is None


def test_drain_states_are_distinct_and_removal_waits_for_inflight(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(request_timeout=5.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    started = threading.Event()
    release_stream = threading.Event()

    def hold_request():
        with httpx.Client(timeout=15) as c:
            with c.stream(
                "GET",
                f"{gw.url}/stream",
                params={"chunks": 1000, "interval": 0.05},
            ) as resp:
                it = resp.iter_bytes()
                next(it)
                started.set()
                release_stream.wait(10)

    t = threading.Thread(target=hold_request)
    t.start()
    assert started.wait(5)

    draining = client.delete(f"{gw.url}/admin/upstreams/a")
    assert draining.status_code == 202
    body = draining.json()
    assert body["state"] == "draining"
    assert body["draining"] is True
    assert body["in_flight"] >= 1

    # second unregister while draining: distinct, stable response
    again = client.delete(admin_unregister(gw, "a"))
    assert again.status_code == 409
    assert again.json()["error"] == "node_draining"

    # unknown node remains 404 and distinguishable
    missing = client.delete(admin_unregister(gw, "ghost"))
    assert missing.status_code == 404
    assert missing.json()["error"] == "node_not_found"

    # node is still visible while draining with a remaining count
    mid = node_state(client, gw, "a")
    assert mid is not None and mid["state"] == "draining"
    assert mid["in_flight"] >= 1

    # registering another node and sending traffic must not land on the
    # draining node
    other = upstream_factory("b")
    register(client, gw, "b", other.url)
    for _ in range(4):
        r = client.get(f"{gw.url}/echo")
        assert r.json()["node"] == "b"

    release_stream.set()
    t.join(timeout=10)


    for _ in range(100):
        if node_state(client, gw, "a") is None:
            break
        time.sleep(0.05)
    assert node_state(client, gw, "a") is None


def test_concurrent_unregister_never_loses_counter(client, gateway, upstream_factory):
    gw = gateway(gw_config(request_timeout=5.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    errors = []

    def worker():
        try:
            with httpx.Client(timeout=15) as c:
                with c.stream(
                    "GET", f"{gw.url}/stream", params={"chunks": 20, "interval": 0.05}
                ) as resp:
                    b"".join(resp.iter_bytes())
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()


    time.sleep(0.4)
    resp = client.delete(admin_unregister(gw, "a"))
    assert resp.status_code == 202

    for t in threads:
        t.join(timeout=15)
    assert not errors

    for _ in range(100):
        if node_state(client, gw, "a") is None:
            break
        time.sleep(0.05)
    removed = node_state(client, gw, "a")
    assert removed is None
    # in-flight requests completed normally: upstream saw them finish
    app = up._server.config.app.state
    assert app.hits.get("/stream", 0) == 6
    assert app.stream_completed == 6
