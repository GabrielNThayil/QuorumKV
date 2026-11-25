"""A QuorumKV node: Raft core + LSM state machine + peer transport + HTTP API.

    python -m quorumkv.server --id n1 --data data/n1 \\
        --cluster n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003

Each cluster member is id=host:raft_port:http_port.

HTTP API
    GET    /kv/<key>                 linearizable read (ReadIndex); ?consistency=stale reads locally
    PUT    /kv/<key>       body=value
    DELETE /kv/<key>
    POST   /cas/<key>      {"expected": str|null, "value": str}   atomic compare-and-swap
    GET    /scan?start=&end=&limit=  ordered range scan (linearizable)
    GET    /status                   Raft + storage state as JSON
    GET    /metrics                  Prometheus text format
Writes sent to a follower get a 307 redirect to the leader.
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import itertools
import json
import logging
import os
import random
import signal
import time
from urllib.parse import parse_qs, unquote, urlsplit

from .raft.node import LEADER, NotLeader, RaftNode
from .raft.storage import FileRaftStorage
from .statemachine import INTERNAL_PREFIX, KVStateMachine
from .storage.lsm import LSMEngine
from .transport import PeerTransport

log = logging.getLogger("quorumkv")

_REASONS = {200: "OK", 204: "No Content", 307: "Temporary Redirect", 400: "Bad Request",
            404: "Not Found", 405: "Method Not Allowed", 503: "Service Unavailable", 504: "Gateway Timeout"}
_BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]


class LeadershipLost(Exception):
    pass


class Histogram:
    def __init__(self):
        self.counts = [0] * (len(_BUCKETS) + 1)
        self.total = 0.0
        self.n = 0

    def observe(self, v: float) -> None:
        self.counts[bisect.bisect_left(_BUCKETS, v)] += 1
        self.total += v
        self.n += 1


def parse_cluster(spec: str) -> dict[str, dict]:
    members = {}
    for part in spec.split(","):
        node_id, addr = part.split("=")
        host, raft_port, http_port = addr.rsplit(":", 2)
        members[node_id] = {"host": host, "raft": int(raft_port), "http": int(http_port)}
    return members


class KVServer:
    def __init__(self, node_id: str, members: dict[str, dict], data_dir: str,
                 tick_ms: int = 20, fsync: bool = True, bind_host: str | None = None,
                 request_timeout: float = 5.0, client_urls: dict[str, str] | None = None):
        self.id = node_id
        self.members = members
        self.client_urls = client_urls or {}
        self.tick_s = tick_ms / 1000
        self.bind_host = bind_host or members[node_id]["host"]
        self.request_timeout = request_timeout

        self.engine = LSMEngine(os.path.join(data_dir, "lsm"))
        self.sm = KVStateMachine(self.engine)
        self.raft_storage = FileRaftStorage(os.path.join(data_dir, "raft"), fsync=fsync)
        self.node = RaftNode(
            node_id, list(members), self.raft_storage, self.sm.apply,
            applied_index=self.sm.applied_index,
            rng=random.Random(),
            on_applied=self._on_applied, on_read_ready=self._on_read_ready,
            on_role_change=self._on_role_change,
        )
        self.transport = PeerTransport(
            node_id,
            {p: (m["host"], m["raft"]) for p, m in members.items() if p != node_id},
            self._on_peer_message,
        )
        self._proposals: dict[int, tuple[int, asyncio.Future]] = {}
        self._reads: dict[int, asyncio.Future] = {}
        self._rid = itertools.count(1)
        self._flush_scheduled = False
        self._replicate_pending = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self.requests: dict[str, int] = {}
        self.latency: dict[str, Histogram] = {"read": Histogram(), "write": Histogram()}
        self.started = time.time()

    # ------------------------------------------------------------ raft driving
    def _schedule_flush(self) -> None:
        if not self._flush_scheduled:
            self._flush_scheduled = True
            self._loop.call_soon(self._flush)

    def _flush(self) -> None:
        """One Raft ready cycle: persist, then send, then apply."""
        self._flush_scheduled = False
        if self._replicate_pending:
            self._replicate_pending = False
            self.node.replicate()
        self.raft_storage.sync()
        for m in self.node.take_messages():
            self.transport.send(m)
        self.node.apply_committed()

    def _on_peer_message(self, msg: dict) -> None:
        self.node.step(msg)
        self._schedule_flush()

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(self.tick_s)
            self.node.tick()
            self._flush()

    def _on_applied(self, index: int, term: int, result) -> None:
        p = self._proposals.pop(index, None)
        if p is None:
            return
        t, fut = p
        if fut.done():
            return
        if t == term:
            fut.set_result(result)
        else:
            fut.set_exception(LeadershipLost())

    def _on_read_ready(self, rid: int, ok: bool) -> None:
        fut = self._reads.pop(rid, None)
        if fut is not None and not fut.done():
            fut.set_result(ok)

    def _on_role_change(self, role: str) -> None:
        log.info("%s is now %s (term %d)", self.id, role, self.node.term)
        if role != LEADER:
            # Outcome unknown: the entry may still be committed by the next leader.
            for _, fut in self._proposals.values():
                if not fut.done():
                    fut.set_exception(LeadershipLost())
            self._proposals.clear()

    async def propose(self, cmd: dict):
        index, term = self.node.propose(cmd)
        fut = self._loop.create_future()
        self._proposals[index] = (term, fut)
        self._replicate_pending = True
        self._schedule_flush()
        return await asyncio.wait_for(fut, self.request_timeout)

    async def linearizable_barrier(self) -> None:
        rid = next(self._rid)
        fut = self._loop.create_future()
        self._reads[rid] = fut
        try:
            self.node.read_index(rid)
        except NotLeader:
            self._reads.pop(rid, None)
            raise
        self._schedule_flush()
        if not await asyncio.wait_for(fut, self.request_timeout):
            raise LeadershipLost()

    # -------------------------------------------------------------------- HTTP
    def _leader_url(self, path: str) -> str | None:
        leader = self.node.leader_id
        if leader is None or leader not in self.members:
            return None
        if leader in self.client_urls:  # address clients use, e.g. a published Docker port
            return self.client_urls[leader].rstrip("/") + path
        m = self.members[leader]
        return f"http://{m['host']}:{m['http']}{path}"

    async def _route(self, method: str, target: str, body: bytes):
        url = urlsplit(target)
        path, query = url.path, parse_qs(url.query)
        try:
            if path.startswith("/kv/"):
                key = unquote(path[4:])
                if not key or key.encode()[:1] == INTERNAL_PREFIX:
                    return 400, {"error": "invalid key"}
                if method == "GET":
                    self._count("get")
                    if query.get("consistency", [""])[0] != "stale":
                        t0 = time.perf_counter()
                        await self.linearizable_barrier()
                        self.latency["read"].observe(time.perf_counter() - t0)
                    value = self.sm.get(key)
                    if value is None:
                        return 404, {"error": "not found", "key": key}
                    return 200, {"key": key, "value": value}
                if method == "PUT":
                    self._count("put")
                    return 200, await self._timed_write({"op": "put", "key": key, "value": body.decode()})
                if method == "DELETE":
                    self._count("delete")
                    return 200, await self._timed_write({"op": "delete", "key": key})
                return 405, {"error": "method not allowed"}
            if path.startswith("/cas/") and method == "POST":
                self._count("cas")
                key = unquote(path[5:])
                if not key or key.encode()[:1] == INTERNAL_PREFIX:
                    return 400, {"error": "invalid key"}
                req = json.loads(body or b"{}")
                if not isinstance(req.get("value"), str):
                    return 400, {"error": "value must be a string"}
                return 200, await self._timed_write(
                    {"op": "cas", "key": key, "expected": req.get("expected"), "value": req["value"]})
            if path == "/scan" and method == "GET":
                self._count("scan")
                await self.linearizable_barrier()
                limit = min(int(query.get("limit", ["100"])[0]), 10_000)
                rows = self.sm.scan(query.get("start", [None])[0], query.get("end", [None])[0], limit)
                return 200, {"items": [{"key": k, "value": v} for k, v in rows]}
            if path == "/status":
                return 200, {**self.node.status(), "storage": self.engine.describe(),
                             "transport": {"sent": self.transport.sent, "dropped": self.transport.dropped},
                             "uptime_s": round(time.time() - self.started, 1)}
            if path == "/metrics":
                return 200, self._metrics()
            return 404, {"error": "no such endpoint"}
        except NotLeader:
            loc = self._leader_url(target)
            if loc is None:
                return 503, {"error": "no leader elected yet, retry"}
            return 307, {"error": "not leader", "leader": self.node.leader_id, "location": loc}
        except LeadershipLost:
            if method == "GET":  # reads change nothing, so they are always safe to retry
                return 503, {"error": "leadership changed, retry"}
            return 503, {"error": "leadership changed; write may or may not have been applied",
                         "outcome": "unknown"}
        except asyncio.TimeoutError:
            if method == "GET":
                return 504, {"error": "timed out waiting for quorum, retry"}
            return 504, {"error": "timed out waiting for quorum; write may or may not have been applied",
                         "outcome": "unknown"}
        except (ValueError, UnicodeDecodeError) as e:
            return 400, {"error": str(e)}

    async def _timed_write(self, cmd: dict):
        t0 = time.perf_counter()
        result = await self.propose(cmd)
        self.latency["write"].observe(time.perf_counter() - t0)
        return result

    def _count(self, op: str) -> None:
        self.requests[op] = self.requests.get(op, 0) + 1

    def _metrics(self) -> str:
        st = self.node.status()
        lines = [
            f'quorumkv_raft_term{{node="{self.id}"}} {st["term"]}',
            f'quorumkv_raft_is_leader{{node="{self.id}"}} {int(st["role"] == LEADER)}',
            f'quorumkv_raft_commit_index{{node="{self.id}"}} {st["commit_index"]}',
            f'quorumkv_raft_applied_index{{node="{self.id}"}} {st["last_applied"]}',
        ]
        for op, n in sorted(self.requests.items()):
            lines.append(f'quorumkv_requests_total{{node="{self.id}",op="{op}"}} {n}')
        for k, v in self.engine.describe().items():
            lines.append(f'quorumkv_storage_{k}{{node="{self.id}"}} {v}')
        for kind, h in self.latency.items():
            cum = 0
            for b, c in zip(_BUCKETS + [float("inf")], h.counts):
                cum += c
                le = "+Inf" if b == float("inf") else b
                lines.append(f'quorumkv_{kind}_latency_seconds_bucket{{node="{self.id}",le="{le}"}} {cum}')
            lines.append(f'quorumkv_{kind}_latency_seconds_sum{{node="{self.id}"}} {h.total:.6f}')
            lines.append(f'quorumkv_{kind}_latency_seconds_count{{node="{self.id}"}} {h.n}')
        return "\n".join(lines) + "\n"

    async def _serve_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                method, target, _ = line.decode("latin-1").split(" ", 2)
                headers = {}
                while True:
                    h = await reader.readline()
                    if h in (b"\r\n", b"\n", b""):
                        break
                    name, _, value = h.decode("latin-1").partition(":")
                    headers[name.strip().lower()] = value.strip()
                body = await reader.readexactly(int(headers.get("content-length", 0)))
                status, payload = await self._route(method.upper(), target, body)
                if isinstance(payload, str):
                    data, ctype = payload.encode(), "text/plain; version=0.0.4"
                else:
                    data, ctype = json.dumps(payload).encode(), "application/json"
                extra = f"Location: {payload['location']}\r\n" if status == 307 else ""
                writer.write(
                    f"HTTP/1.1 {status} {_REASONS.get(status, 'OK')}\r\nContent-Type: {ctype}\r\n"
                    f"Content-Length: {len(data)}\r\n{extra}\r\n".encode() + data)
                await writer.drain()
                if headers.get("connection", "").lower() == "close":
                    break
        except (asyncio.IncompleteReadError, ConnectionError, ValueError):
            pass
        finally:
            writer.close()

    # --------------------------------------------------------------- lifecycle
    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        me = self.members[self.id]
        await self.transport.start(self.bind_host, me["raft"])
        http = await asyncio.start_server(self._serve_http, self.bind_host, me["http"])
        log.info("%s listening raft=%s http=%s (applied=%d, log=%d)", self.id, me["raft"], me["http"],
                 self.node.last_applied, self.node.last_index())
        ticker = asyncio.create_task(self._tick_loop())
        stop = asyncio.Event()
        try:
            self._loop.add_signal_handler(signal.SIGTERM, stop.set)
        except (NotImplementedError, RuntimeError):
            pass  # Windows
        try:
            await stop.wait()
        finally:
            ticker.cancel()
            http.close()
            await self.transport.close()
            self.raft_storage.close()
            self.engine.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Run a QuorumKV node")
    ap.add_argument("--id", required=True)
    ap.add_argument("--cluster", required=True, help="id=host:raft_port:http_port,...")
    ap.add_argument("--data", required=True)
    ap.add_argument("--bind", default=None, help="bind address (default: this node's host)")
    ap.add_argument("--client-urls", default="",
                    help="id=url,... addresses clients use to reach each node's HTTP API, used in leader "
                         "redirects (e.g. published Docker ports). Default: host:http_port from --cluster")
    ap.add_argument("--tick-ms", type=int, default=20)
    ap.add_argument("--no-fsync", action="store_true", help="skip fsync on the Raft log (faster, less durable)")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")
    client_urls = dict(p.split("=", 1) for p in args.client_urls.split(",") if p)
    server = KVServer(args.id, parse_cluster(args.cluster), args.data, tick_ms=args.tick_ms,
                      fsync=not args.no_fsync, bind_host=args.bind, client_urls=client_urls)
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
