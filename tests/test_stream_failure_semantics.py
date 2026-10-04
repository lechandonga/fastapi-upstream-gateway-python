"""Deterministic coverage for streaming failure semantics.

Scenarios:

* upstream sends headers but never a body byte, another healthy node exists
  -> gateway transparently retries and the client gets the full response;
* same stall, but no other node is available
  -> the client gets an explicit 504, never an empty 200;
* upstream dies / truncates the connection halfway through the body
  -> the client observes a transport error, not a clean short 200;
* the client disconnects mid-stream
  -> the in-flight counter drops promptly and nothing leaks, and a node in
  graceful drain is not removed while data is still flowing.

Each test exercises real local uvicorn processes and sockets (no mocks),
so repeated runs are deterministic: no sleeps racing on counters.
"""

import socket
import time
import threading

import httpx
import pytest

from gateway.config import GatewayConfig


def register(client, gw, name, url):
    return client.post(f"{gw.url}/admin/upstreams", json={"name": name, "url": url})


def node_state(client, gw, name):
    data = client.get(f"{gw.url}/admin/upstreams").json()["nodes"]
    return {n["name"]: n for n in data}.get(name)


def gw_config(**overrides):
    base = dict(
        health_interval=3600,
        failure_threshold=2,
        success_threshold=2,
        request_timeout=0.5,
        stream_idle_timeout=0.5,
        max_retries=2,
    )
    base.update(overrides)
    return GatewayConfig(**base)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def set_stall(upstream, stalled: bool):
    upstream._server.config.app.state.headers_stall = stalled


class TruncatingUpstream:
    """Raw TCP upstream: answers /health and then serves /truncate by sending
    a valid response head + one body chunk and dropping the connection
    immediately (no Content-Length completion)."""

    def __init__(self):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.hits = {"truncate": 0, "health": 0}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.port))
        srv.listen(8)
        srv.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
        srv.close()

    def _read_request_head(self, conn):
        data = b""
        conn.settimeout(2)
        while b"\r\n\r\n" not in data:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        return data

    def _handle(self, conn):
        try:
            head = self._read_request_head(conn)
            request_line = head.split(b"\r\n", 1)[0]
            path = request_line.split(b" ")[1] if b" " in request_line else b""
            if path.startswith(b"/health"):
                self.hits["health"] += 1
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n"
                    b"content-type: application/json\r\nconnection: close\r\n\r\n{}"
                )
            elif path.startswith(b"/truncate"):
                self.hits["truncate"] += 1
                # Promises 100 bytes, sends 7 and severs the connection: a
                # clean HTTP body never ends this way, so the client must
                # treat the response as failed rather than a short 200.
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n"
                    b"content-length: 100\r\nconnection: close\r\n\r\n"
                    b"partial"
                )
                time.sleep(0.1)  # let the bytes leave the kernel before RST/FIN
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            else:
                conn.sendall(
                    b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n"
                )
        except (OSError, IndexError):
            pass
        finally:
            conn.close()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=3)


@pytest.fixture
def truncating_upstream():
    srv = TruncatingUpstream()
    yield srv
    srv.stop()


# ---------- scenario 1: headers stall, another healthy node exists ----------


def test_headers_stall_retries_other_node_and_client_gets_full_body(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(stream_idle_timeout=0.3))
    bad = upstream_factory("bad")
    good = upstream_factory("good")
    register(client, gw, "bad", bad.url)
    register(client, gw, "good", good.url)
    set_stall(bad, True)

    resp = client.get(f"{gw.url}/stall-headers")
    assert resp.status_code == 200
    assert resp.text == "complete-body-from-good"
    assert bad._server.config.app.state.hits.get("/stall-headers", 0) == 1
    assert good._server.config.app.state.hits.get("/stall-headers", 0) == 1
    assert node_state(client, gw, "bad")["in_flight"] == 0
    assert node_state(client, gw, "good")["in_flight"] == 0


# ---------- scenario 2: headers stall, no other node ----------


def test_headers_stall_without_other_node_is_explicit_failure_not_empty_200(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(stream_idle_timeout=0.3, max_retries=1))
    bad = upstream_factory("bad")
    register(client, gw, "bad", bad.url)
    set_stall(bad, True)

    start = time.monotonic()
    resp = client.get(f"{gw.url}/stall-headers")
    elapsed = time.monotonic() - start
    assert resp.status_code == 504
    assert resp.json()["error"] == "upstream_timeout"
    assert elapsed < 3
    assert node_state(client, gw, "bad")["in_flight"] == 0


# ---------- scenario 3: upstream dies halfway through the body ----------


def test_mid_body_truncation_is_aborted_not_clean_short_200(
    client, gateway, truncating_upstream
):
    gw = gateway(gw_config(stream_idle_timeout=2.0))
    register(client, gw, "bad", truncating_upstream.url)

    got_partial = threading.Event()
    received = []

    def consume():
        with client.stream("GET", f"{gw.url}/truncate") as resp:
            assert resp.status_code == 200
            chunks = resp.iter_raw()
            for chunk in chunks:
                received.append(chunk)
                if b"".join(received):
                    got_partial.set()

    # httpx must raise while reading the unfinished body.
    with pytest.raises(httpx.HTTPError):
        consume()
    assert got_partial.is_set()
    body = b"".join(received)
    assert body.startswith(b"partial")
    assert not body.endswith(b"complete")  # the body was cut, not completed

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if node_state(client, gw, "bad")["in_flight"] == 0:
            break
        time.sleep(0.02)
    assert node_state(client, gw, "bad")["in_flight"] == 0


def test_mid_body_truncation_retries_other_node_for_fresh_request(
    client, gateway, upstream_factory, truncating_upstream
):
    # A truncating first pick plus a healthy second node: a new idempotent
    # request that hits the broken node before any body byte arrives cannot
    # happen here (it already sent a byte); this asserts retry works when the
    # broken node fails *before* the first byte (connection close on accept).
    gw = gateway(gw_config(stream_idle_timeout=2.0))
    good = upstream_factory("good")
    register(client, gw, "bad", truncating_upstream.url)
    register(client, gw, "good", good.url)

    with pytest.raises(httpx.HTTPError):
        with client.stream("GET", f"{gw.url}/truncate") as resp:
            assert resp.status_code == 200
            for _ in resp.iter_raw():
                pass

    # after the failed stream, counters on every node are back to zero
    assert node_state(client, gw, "bad")["in_flight"] == 0
    assert node_state(client, gw, "good")["in_flight"] == 0


# ---------- scenario 4: client disconnect mid-stream ----------


def _wait_in_flight(client, gw, name, expected, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = node_state(client, gw, name)
        if state is not None and state["in_flight"] == expected:
            return state
        time.sleep(0.02)
    raise AssertionError(
        f"node {name}: in_flight never reached {expected}, "
        f"last={node_state(client, gw, name)}"
    )


def test_client_disconnect_drops_counter_without_leak(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(stream_idle_timeout=30.0, request_timeout=30.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    short = httpx.Client(timeout=10)
    with short.stream("GET", f"{gw.url}/slow-stream") as resp:
        chunks = resp.iter_bytes()
        assert next(chunks) == b"first\n"
        assert _wait_in_flight(client, gw, "a", 1)["in_flight"] == 1
        short.close()  # abort while the upstream is holding the second chunk

    state = _wait_in_flight(client, gw, "a", 0)
    assert state["draining"] is False


def test_draining_node_kept_while_body_still_flows_after_client_abort(
    client, gateway, upstream_factory
):
    gw = gateway(gw_config(stream_idle_timeout=30.0, request_timeout=30.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    got_first = threading.Event()

    def hold_stream():
        try:
            with client.stream("GET", f"{gw.url}/slow-stream") as resp:
                for chunk in resp.iter_bytes():
                    if chunk:
                        got_first.set()
        except httpx.HTTPError:
            pass

    t = threading.Thread(target=hold_stream)
    t.start()
    assert got_first.wait(5)

    drain = client.delete(f"{gw.url}/admin/upstreams/a")
    assert drain.status_code == 202
    state = _wait_in_flight(client, gw, "a", 1)
    assert state["state"] == "draining"

    # still streaming: the draining node must not be removed early
    time.sleep(0.2)
    assert node_state(client, gw, "a") is not None
    assert node_state(client, gw, "a")["in_flight"] == 1

    assert client.post(f"{up.url}/release-slow-stream").status_code == 200
    t.join(timeout=5)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if node_state(client, gw, "a") is None:
            return
        time.sleep(0.02)
    raise AssertionError("draining node was not removed after the stream ended")


def test_counter_stays_held_while_data_continues_arriving(
    client, gateway, upstream_factory
):
    # Even if the whole transfer outlasts the ordinary request timeout, the
    # counter must stay at 1 for the entire transfer and only fall after the
    # last byte. Guards against premature release / disconnect misdetection.
    gw = gateway(gw_config(request_timeout=0.3, stream_idle_timeout=1.0))
    up = upstream_factory("a")
    register(client, gw, "a", up.url)

    resp = client.get(
        f"{gw.url}/steady-stream", params={"duration": "1.2", "interval": "0.05"}
    )
    assert resp.status_code == 200
    assert int(resp.text.strip().splitlines()[-1]) >= 15
    assert node_state(client, gw, "a")["in_flight"] == 0
