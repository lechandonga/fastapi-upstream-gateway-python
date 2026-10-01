from __future__ import annotations

import concurrent.futures
import time

import httpx
import pytest

from tests.fakes import FakeUpstream


@pytest.fixture
def fake():
    upstream = FakeUpstream("u1").start()
    yield upstream
    upstream.stop()


def register(client: httpx.Client, upstream: FakeUpstream):
    resp = client.post(
        "/admin/upstreams",
        json={"id": upstream.node_id, "base_url": upstream.base_url},
    )
    assert resp.status_code == 201


def wait_health(client, node_id, healthy: bool, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/admin/upstreams/{node_id}").json()
        if body["healthy"] is healthy:
            return body
        time.sleep(0.05)
    raise AssertionError(
        f"node {node_id} never reached healthy={healthy}: {body}"
    )


def wait_next_check(client, node_id, previous_check, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/admin/upstreams/{node_id}").json()
        if body["last_check_at"] != previous_check:
            return body
        time.sleep(0.02)
    raise AssertionError("no new probe observed")


# --- original interface --------------------------------------------------

def test_root_interface_unchanged(gateway):
    resp = gateway.client().get("/")
    assert resp.status_code == 200
    assert resp.json() == {"Hello": "World"}


# --- registration --------------------------------------------------------

def test_duplicate_registration_rejected(gateway, fake):
    client = gateway.client()
    register(client, fake)
    resp = client.post(
        "/admin/upstreams",
        json={"id": fake.node_id, "base_url": fake.base_url},
    )
    assert resp.status_code == 409
    assert resp.json()["error"] == "upstream_already_registered"


def test_deregister_unknown_rejected(gateway):
    resp = gateway.client().delete("/admin/upstreams/ghost")
    assert resp.status_code == 404
    assert resp.json()["error"] == "upstream_not_found"


def test_deregister_stops_forwarding(gateway, fake):
    client = gateway.client()
    register(client, fake)
    assert client.delete("/admin/upstreams/u1").status_code == 204
    resp = client.get("/gateway/echo/x")
    assert resp.status_code == 503
    assert resp.json()["error"] == "no_healthy_upstream"


# --- health threshold flapping -------------------------------------------

def test_health_threshold_flip_and_recovery(gateway, fake):
    client = gateway.client()
    register(client, fake)

    # 1-2 failures must not flip state (threshold = 3).
    fake.set_health(status=500)
    time.sleep(0.3)
    body = client.get("/admin/upstreams/u1").json()
    assert body["healthy"] is True
    assert body["consecutive_failures"] in (1, 2)

    body = wait_health(client, "u1", False)
    assert "probe 500" in body["last_reason"]

    # 1 successful probe must not immediately recover (threshold = 2).
    fake.set_health(status=200)
    time.sleep(0.3)
    body = client.get("/admin/upstreams/u1").json()
    assert body["healthy"] is False
    assert body["consecutive_successes"] in (1,)

    body = wait_health(client, "u1", True)
    assert "probe 200" in body["last_reason"]


def test_subthreshold_successes_do_not_flap(gateway, fake):
    client = gateway.client()
    register(client, fake)
    fake.set_health(status=500)
    wait_health(client, "u1", False)

    # fail, success, fail, success -> counters reset, never cross thresholds.
    last_check = client.get("/admin/upstreams/u1").json()["last_check_at"]
    for status in (500, 200, 500, 200):
        fake.set_health(status=status)
        body = wait_next_check(client, "u1", last_check)
        last_check = body["last_check_at"]
        assert body["healthy"] is False  # never flaps back to healthy
    # ended on a single success: below healthy_threshold (2)
    assert body["healthy"] is False
    assert body["consecutive_successes"] <= 1


def test_probe_timeout_marks_unhealthy(gateway, fake):
    client = gateway.client()
    register(client, fake)
    fake.set_health(delay=2.0)  # far beyond health_timeout=0.5
    body = wait_health(client, "u1", False)
    assert "timeout" in body["last_reason"]


# --- no healthy upstream -------------------------------------------------

def test_no_healthy_upstream_error(gateway, fake):
    client = gateway.client()
    register(client, fake)
    fake.set_health(status=500)
    wait_health(client, "u1", False)
    resp = client.get("/gateway/echo/x")
    assert resp.status_code == 503
    assert resp.json()["error"] == "no_healthy_upstream"
    assert fake.request_count == 0


def test_connection_failure_classified(gateway):
    client = gateway.client()
    # Nothing listens on this port.
    resp = client.post(
        "/admin/upstreams",
        json={"id": "dead", "base_url": "http://127.0.0.1:1"},
    )
    assert resp.status_code == 201
    resp = client.get("/gateway/echo/x")
    assert resp.status_code == 502
    assert resp.json()["error"] == "upstream_connect_error"


# --- load balancing ------------------------------------------------------

def _start_many(ids):
    return [FakeUpstream(node_id).start() for node_id in ids]


def _stop_many(fakes):
    for fake in fakes:
        fake.stop()


def test_round_robin_distribution(gateway):
    client = gateway.client()
    fakes = list(_start_many(["a", "b", "c"]))
    try:
        for fake in fakes:
            register(client, fake)
        nodes = []
        for _ in range(6):
            body = client.get("/gateway/echo/ping").json()
            nodes.append(body["node"])
        assert sorted(nodes) == ["a", "a", "b", "b", "c", "c"]
    finally:
        _stop_many(fakes)


def test_least_inflight_prefers_idle_node(gateway, settings):
    client = gateway.client()
    fakes = list(_start_many(["fast", "slow"]))
    try:
        for fake in fakes:
            register(client, fake)
        assert client.put(
            "/admin/strategy", json={"strategy": "least_inflight"}
        ).json()["strategy"] == "least_inflight"
        assert settings.strategy == "least_inflight"

        fakes[1].slow_delay = 0.6  # second node slow
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            slow_future = pool.submit(client.get, "/gateway/slow")
            time.sleep(0.25)  # let the slow request occupy one node
            body = client.get("/gateway/echo/now").json()
            assert slow_future.result().status_code == 200
        assert body["node"] == "fast"

        status = {s["id"]: s for s in client.get("/admin/upstreams").json()}
        assert all(s["inflight"] == 0 for s in status.values())
    finally:
        _stop_many(fakes)


def test_inflight_counters_under_concurrency(gateway):
    client = gateway.client()
    fakes = list(_start_many(["a", "b", "c"]))
    try:
        for fake in fakes:
            register(client, fake)
        client.put("/admin/strategy", json={"strategy": "least_inflight"})
        for fake in fakes:
            fake.slow_delay = 0.4

        def hit():
            with httpx.Client(base_url=gateway.base_url, timeout=10) as c:
                return c.get("/gateway/echo/x").json()["node"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
            chosen = list(pool.map(lambda _: hit(), range(9)))
        assert sorted(chosen) == ["a", "a", "a", "b", "b", "b", "c", "c", "c"]
        # Fair share is 3 per node; least-inflight must never over-dispatch
        # a node while another idle candidate exists.
        assert all(fake.max_inflight == 3 for fake in fakes)
        status = client.get("/admin/upstreams").json()
        assert all(node["inflight"] == 0 for node in status)
    finally:
        _stop_many(fakes)


def test_invalid_strategy_rejected(gateway):
    client = gateway.client()
    resp = client.put("/admin/strategy", json={"strategy": "random"})
    assert resp.status_code == 422


# --- forwarding semantics ------------------------------------------------

def test_hop_by_hop_headers_stripped_and_forward_headers_added(gateway, fake):
    client = gateway.client()
    register(client, fake)
    resp = client.get(
        "/gateway/echo/ping?x=1",
        headers={"X-Trace": "abc", "Connection": "X-Trace"},
    )
    received = resp.json()["headers"]
    assert "x-trace" not in received  # listed in Connection, must be stripped
    assert received.get("connection") == "keep-alive"  # regenerated, not passthrough
    assert received["x-forwarded-for"] == "127.0.0.1"
    assert received["x-forwarded-proto"] == "http"
    assert received["x-forwarded-host"]
    assert received["via"] == "1.1 fastapi-upstream-gateway"


def test_timeout_retries_another_idempotent_node(gateway):
    client = gateway.client()
    good = FakeUpstream("good").start()
    slow = FakeUpstream("slow").start()
    slow.slow_delay = 3.0  # exceeds gateway request_timeout=0.8
    try:
        register(client, slow)
        register(client, good)
        resp = client.get("/gateway/slow")
        assert resp.status_code == 200
        assert resp.json()["node"] == "good"
        assert slow.request_count == 1  # not retried on the same node
    finally:
        good.stop()
        slow.stop()


def test_non_idempotent_request_is_not_retried(gateway, fake):
    client = gateway.client()
    good = FakeUpstream("good").start()
    try:
        # "fake" stalls (accepts but never answers); gateway times out.
        fake.slow_delay = 3.0
        register(client, fake)
        register(client, good)
        resp = client.post("/gateway/echo/create", content="payload")
        # POST must be delivered exactly once, never retried on "good".
        assert resp.status_code == 504
        assert resp.json()["error"] == "upstream_timeout"
        assert good.request_count == 0
    finally:
        good.stop()


def test_upstream_5xx_classified_and_retried(gateway):
    client = gateway.client()
    broken = FakeUpstream("broken").start()
    good = FakeUpstream("good").start()
    try:
        register(client, broken)
        register(client, good)
        resp = client.get("/gateway/fail500")
        assert resp.status_code == 502
        assert resp.json()["error"] == "upstream_5xx"
        # both healthy nodes were tried once each
        assert broken.request_count == 1
        assert good.request_count == 1
    finally:
        broken.stop()
        good.stop()
