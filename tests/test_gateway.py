import threading

from gateway.config import GatewayConfig


def register(client, gw, name, url):
    return client.post(f"{gw.url}/admin/upstreams", json={"name": name, "url": url})


def check(client, gw):
    return client.post(f"{gw.url}/admin/upstreams/check")


def nodes(client, gw):
    return {n["name"]: n for n in client.get(f"{gw.url}/admin/upstreams").json()["nodes"]}


def test_root_interface_preserved(client, gateway):
    gw = gateway()
    assert client.get(f"{gw.url}/").json() == {"Hello": "World"}


def test_register_duplicate_and_unregister_missing_are_distinct(client, gateway, upstream_factory):
    gw = gateway()
    up = upstream_factory("a")
    assert register(client, gw, "a", up.url).status_code == 201

    dup = register(client, gw, "a", up.url)
    assert dup.status_code == 409
    assert dup.json()["error"] == "node_already_exists"

    missing = client.delete(f"{gw.url}/admin/upstreams/ghost")
    assert missing.status_code == 404
    assert missing.json()["error"] == "node_not_found"

    assert client.delete(f"{gw.url}/admin/upstreams/a").status_code == 200
    again = client.delete(f"{gw.url}/admin/upstreams/a")
    assert again.status_code == 404
    assert again.json()["error"] == "node_not_found"


def test_health_thresholds_prevent_flapping(client, gateway, upstream_factory):
    gw = gateway()  # failure_threshold=2, success_threshold=2
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    app = up._server.config.app
    app.state.healthy = False

    check(client, gw)
    state = nodes(client, gw)["a"]
    assert state["healthy"] is True
    assert state["consecutive_failures"] == 1

    check(client, gw)
    state = nodes(client, gw)["a"]
    assert state["healthy"] is False
    assert state["consecutive_failures"] == 2
    assert "503" in state["last_reason"] or "status" in state["last_reason"]

    # one success: not yet recovered
    app.state.healthy = True
    check(client, gw)
    state = nodes(client, gw)["a"]
    assert state["healthy"] is False
    assert state["consecutive_successes"] == 1

    check(client, gw)
    state = nodes(client, gw)["a"]
    assert state["healthy"] is True
    assert state["consecutive_successes"] == 2


def test_no_healthy_nodes_returns_503(client, gateway, upstream_factory):
    gw = gateway()
    up = upstream_factory("a")
    register(client, gw, "a", up.url)
    up._server.config.app.state.healthy = False
    check(client, gw)
    check(client, gw)
    assert nodes(client, gw)["a"]["healthy"] is False

    resp = client.get(f"{gw.url}/echo")
    assert resp.status_code == 503
    assert resp.json()["error"] == "no_healthy_upstream"
    # unhealthy node must never receive proxied traffic
    assert up._server.config.app.state.hits.get("/echo", 0) == 0


def test_round_robin_distribution(client, gateway, upstream_factory):
    gw = gateway()
    a = upstream_factory("a")
    b = upstream_factory("b")
    register(client, gw, "a", a.url)
    register(client, gw, "b", b.url)

    seen = [client.get(f"{gw.url}/echo").json()["node"] for _ in range(4)]
    assert seen == ["a", "b", "a", "b"]


def test_forwarded_headers_and_hop_by_hop_stripped(client, gateway, upstream_factory):
    gw = gateway()
    a = upstream_factory("a")
    register(client, gw, "a", a.url)

    resp = client.get(f"{gw.url}/echo", headers={"connection": "keep-alive, x-secret"})
    body = resp.json()
    assert body["x_forwarded_for"] == "127.0.0.1"
    assert body["x_forwarded_host"].startswith("127.0.0.1:")
    assert body["x_forwarded_proto"] == "http"
    # upstream must see a normal connection header, not the client's hop-by-hop list
    assert body["connection"] != "keep-alive, x-secret"


def test_timeout_retries_on_other_node_for_idempotent(client, gateway, upstream_factory):
    gw = gateway(GatewayConfig(health_interval=3600, request_timeout=0.2, max_retries=2))
    slow = upstream_factory("slow", delay=2.0)
    fast = upstream_factory("fast")
    register(client, gw, "slow", slow.url)
    register(client, gw, "fast", fast.url)

    resp = client.get(f"{gw.url}/work")
    assert resp.status_code == 200
    assert resp.json()["node"] == "fast"
    assert slow._server.config.app.state.hits.get("/work", 0) == 1
    assert fast._server.config.app.state.hits.get("/work", 0) == 1


def test_non_idempotent_request_is_never_retried(client, gateway, upstream_factory):
    gw = gateway(GatewayConfig(health_interval=3600, request_timeout=0.2, max_retries=2))
    slow = upstream_factory("slow", delay=2.0)
    fast = upstream_factory("fast")
    register(client, gw, "slow", slow.url)
    register(client, gw, "fast", fast.url)

    resp = client.post(f"{gw.url}/work")
    assert resp.status_code == 504
    assert resp.json()["error"] == "upstream_timeout"
    # only the first node was hit; no retry dispatched
    total = sum(
        s._server.config.app.state.hits.get("/work", 0) for s in (slow, fast)
    )
    assert total == 1


def test_upstream_5xx_normalized_and_retried(client, gateway, upstream_factory):
    gw = gateway(GatewayConfig(health_interval=3600, request_timeout=1.0, max_retries=1))
    a = upstream_factory("a")
    b = upstream_factory("b")
    register(client, gw, "a", a.url)
    register(client, gw, "b", b.url)

    # round robin: first attempt hits "a" (/boom -> 500), retry goes to "b"
    resp = client.get(f"{gw.url}/boom")
    assert resp.status_code == 502
    assert resp.json()["error"] == "upstream_server_error"
    assert a._server.config.app.state.hits.get("/boom", 0) == 1
    assert b._server.config.app.state.hits.get("/boom", 0) == 1


def test_connect_error_is_distinct(client, gateway):
    gw = gateway(GatewayConfig(health_interval=3600, request_timeout=0.5, max_retries=0))
    # register a port nothing listens on
    import tests.conftest as cf

    register(client, gw, "dead", f"http://127.0.0.1:{cf.free_port()}")
    resp = client.get(f"{gw.url}/echo")
    assert resp.status_code == 502
    assert resp.json()["error"] == "upstream_connect_error"


def test_least_in_flight_under_concurrency(client, gateway, upstream_factory):
    gw = gateway(
        GatewayConfig(
            strategy="least_in_flight",
            health_interval=3600,
            request_timeout=5.0,
        )
    )
    a = upstream_factory("a", delay=0.3)
    b = upstream_factory("b", delay=0.3)
    register(client, gw, "a", a.url)
    register(client, gw, "b", b.url)

    results = []
    errors = []

    def worker():
        try:
            results.append(client.get(f"{gw.url}/work").json()["node"])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(results) == 20
    # with equal latency, least-in-flight must split evenly
    assert results.count("a") == 10
    assert results.count("b") == 10
    # in-flight counters must fall back to zero after all requests complete
    state = nodes(client, gw)
    assert state["a"]["in_flight"] == 0
    assert state["b"]["in_flight"] == 0
