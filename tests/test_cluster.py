"""End-to-end tests against real server processes talking over TCP."""
import threading
import time

import pytest

from quorumkv.client import QuorumClient, QuorumError
from quorumkv.devcluster import LocalCluster


@pytest.fixture
def cluster(tmp_path):
    c = LocalCluster(3, str(tmp_path / "data"), fsync=True)
    c.start_all()
    c.leader(timeout=15)
    yield c
    c.stop_all()


def test_basic_operations_and_redirects(cluster):
    follower = next(i for i in cluster.ids if i != cluster.leader())
    # Point the client at a follower only: it must discover the leader via 307.
    client = QuorumClient([cluster.http_addr(follower)])
    assert client.put("user/1", "alice") == {"ok": True}
    assert client.put("user/2", "bob") == {"ok": True}
    assert client.get("user/1") == "alice"
    assert client.cas("user/1", "alice", "alicia")["swapped"] is True
    assert client.cas("user/1", "alice", "nope")["swapped"] is False
    assert client.cas("new", None, "created")["swapped"] is True
    assert client.delete("user/2")["deleted"] is True
    assert client.get("user/2") is None
    assert [r["key"] for r in client.scan(start="user/", end="user0")] == ["user/1"]
    client.close()


def test_survives_leader_crash_with_no_lost_acknowledged_writes(cluster):
    """Writers and CAS-incrementers run while the leader is killed (twice).

    * every write the cluster acknowledged must be readable afterwards;
    * the CAS counter must equal the number of acknowledged increments, which shows
      no increment was lost or applied twice. Increments whose outcome is unknown
      (the request failed mid-failover) are tracked and allowed either way.
    """
    acked: dict[str, str] = {}
    lock = threading.Lock()
    stop = threading.Event()
    incr = {"ok": 0, "unknown": 0}

    def writer(wid: int):
        c = QuorumClient(cluster.addrs, timeout=2.0, retries=60)
        n = 0
        while not stop.is_set():
            key, val = f"w{wid}/{n}", f"value-{n}"
            try:
                c.put(key, val)
                with lock:
                    acked[key] = val
            except QuorumError:
                pass
            n += 1
        c.close()

    def incrementer():
        c = QuorumClient(cluster.addrs, timeout=2.0, retries=60)
        while not stop.is_set():
            try:
                cur = c.get("counter")
                nxt = str(int(cur or 0) + 1)
                r = c.cas("counter", cur, nxt)
                if r["swapped"]:
                    incr["ok"] += 1
            except QuorumError:
                incr["unknown"] += 1
        c.close()

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    threads += [threading.Thread(target=incrementer) for _ in range(2)]
    for t in threads:
        t.start()
    for _ in range(2):
        time.sleep(2.5)
        victim = cluster.leader()
        cluster.kill(victim)
        time.sleep(2.5)
        cluster.start(victim)
    time.sleep(2)
    stop.set()
    for t in threads:
        t.join(timeout=30)

    assert len(acked) > 100, "cluster made too little progress"
    reader = QuorumClient(cluster.addrs, retries=60)
    missing = [k for k, v in acked.items() if reader.get(k) != v]
    assert not missing, f"{len(missing)} acknowledged writes lost, e.g. {missing[:5]}"
    final = int(reader.get("counter") or 0)
    assert incr["ok"] <= final <= incr["ok"] + incr["unknown"]
    reader.close()


def test_restarted_node_catches_up_from_disk_and_leader(cluster):
    client = QuorumClient(cluster.addrs)
    for i in range(50):
        client.put(f"a{i}", str(i))
    victim = next(i for i in cluster.ids if i != cluster.leader())
    cluster.kill(victim)
    for i in range(50):
        client.put(f"b{i}", str(i))
    cluster.start(victim)
    deadline = time.time() + 10
    st = {}
    while time.time() < deadline:
        try:
            st = client.status(cluster.http_addr(victim))
            leader_st = client.status(cluster.http_addr(cluster.leader()))
            if st["last_applied"] >= leader_st["commit_index"] - 1:
                break
        except OSError:
            pass
        time.sleep(0.2)
    stale = QuorumClient([cluster.http_addr(victim)])
    assert stale.get("b49", stale=True) == "49"
    assert stale.get("a0", stale=True) == "0"
    client.close()
