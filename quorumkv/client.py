"""Client library and CLI.

The client finds the leader on its own: it follows 307 redirects, remembers the
leader, and on a connection error, 503 or 504 retries against the other nodes with
backoff.

    python -m quorumkv.client --nodes 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 put greeting hello
    python -m quorumkv.client --nodes ... get greeting
    python -m quorumkv.client --nodes ... cas counter 1 2
    python -m quorumkv.client --nodes ... scan --start a --end z
    python -m quorumkv.client --nodes ... status
"""
from __future__ import annotations

import argparse
import http.client
import json
import sys
import time
from urllib.parse import quote, urlencode, urlsplit


class QuorumError(Exception):
    pass


class OutcomeUnknown(QuorumError):
    """A non-idempotent write may or may not have been applied (e.g. leader changed mid-request)."""


class QuorumClient:
    def __init__(self, nodes: list[str], timeout: float = 5.0, retries: int = 30):
        self.nodes = nodes
        self.leader: str | None = None
        self.timeout = timeout
        self.retries = retries
        self._conns: dict[str, http.client.HTTPConnection] = {}

    def _conn(self, node: str) -> http.client.HTTPConnection:
        c = self._conns.get(node)
        if c is None:
            host, port = node.rsplit(":", 1)
            c = http.client.HTTPConnection(host, int(port), timeout=self.timeout)
            self._conns[node] = c
        return c

    def _drop(self, node: str) -> None:
        c = self._conns.pop(node, None)
        if c:
            c.close()

    def request(self, method: str, path: str, body: bytes | None = None, idempotent: bool = True):
        """Send a request, following leader redirects and retrying transient failures.

        Retrying is only safe when repeating the operation can't change the result (reads, PUT,
        DELETE). For a non-idempotent operation such as CAS, a failure after the request may have
        reached the leader raises OutcomeUnknown instead: the write may or may not have committed,
        and blindly retrying could apply it twice or misreport it.
        """
        attempt = 0
        candidates = list(self.nodes)
        target = self.leader or candidates[0]
        while True:
            try:
                c = self._conn(target)
                c.request(method, path, body=body, headers={"Content-Type": "application/octet-stream"})
                resp = c.getresponse()
                data = resp.read()
                status = resp.status
            except ConnectionRefusedError:
                self._drop(target)  # never reached the server: always safe to retry
                status, data = None, b""
            except (OSError, http.client.HTTPException) as e:
                self._drop(target)
                if not idempotent:
                    raise OutcomeUnknown(f"{method} {path}: connection lost mid-request ({e!r})") from e
                status, data = None, b""
            if not idempotent and status in (503, 504) and data and json.loads(data).get("outcome") == "unknown":
                raise OutcomeUnknown(f"{method} {path}: {json.loads(data)['error']}")
            if status == 307:
                loc = urlsplit(json.loads(data)["location"])
                target = self.leader = loc.netloc
                continue
            if status in (200, 404, 400):
                if status != 400:
                    self.leader = target
                return status, json.loads(data) if data else None
            attempt += 1
            if attempt > self.retries:
                raise QuorumError(f"{method} {path} failed after {attempt} attempts (last status {status})")
            self.leader = None
            target = candidates[attempt % len(candidates)]
            time.sleep(min(0.05 * attempt, 0.5))

    # convenience wrappers ------------------------------------------------------
    def put(self, key: str, value: str) -> dict:
        return self.request("PUT", "/kv/" + quote(key, safe=""), value.encode())[1]

    def get(self, key: str, stale: bool = False) -> str | None:
        q = "?consistency=stale" if stale else ""
        status, data = self.request("GET", "/kv/" + quote(key, safe="") + q)
        return data["value"] if status == 200 else None

    def delete(self, key: str) -> dict:
        return self.request("DELETE", "/kv/" + quote(key, safe=""))[1]

    def cas(self, key: str, expected: str | None, value: str) -> dict:
        body = json.dumps({"expected": expected, "value": value}).encode()
        return self.request("POST", "/cas/" + quote(key, safe=""), body, idempotent=False)[1]

    def scan(self, start: str | None = None, end: str | None = None, limit: int = 100) -> list[dict]:
        q = {k: v for k, v in {"start": start, "end": end, "limit": limit}.items() if v is not None}
        return self.request("GET", "/scan?" + urlencode(q))[1]["items"]

    def status(self, node: str) -> dict:
        host, port = node.rsplit(":", 1)
        c = http.client.HTTPConnection(host, int(port), timeout=self.timeout)
        try:
            c.request("GET", "/status")
            return json.loads(c.getresponse().read())
        finally:
            c.close()

    def close(self) -> None:
        for n in list(self._conns):
            self._drop(n)


def main() -> None:
    ap = argparse.ArgumentParser(description="QuorumKV client")
    ap.add_argument("--nodes", default="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("put").add_argument("kv", nargs=2)
    g = sub.add_parser("get")
    g.add_argument("key")
    g.add_argument("--stale", action="store_true")
    sub.add_parser("delete").add_argument("key")
    c = sub.add_parser("cas")
    c.add_argument("key")
    c.add_argument("expected", help='use "-" for "key must not exist"')
    c.add_argument("value")
    s = sub.add_parser("scan")
    s.add_argument("--start")
    s.add_argument("--end")
    s.add_argument("--limit", type=int, default=100)
    sub.add_parser("status")
    args = ap.parse_args()

    client = QuorumClient(args.nodes.split(","))
    if args.cmd == "put":
        out = client.put(*args.kv)
    elif args.cmd == "get":
        out = client.get(args.key, stale=args.stale)
        if out is None:
            print("(not found)")
            sys.exit(1)
    elif args.cmd == "delete":
        out = client.delete(args.key)
    elif args.cmd == "cas":
        out = client.cas(args.key, None if args.expected == "-" else args.expected, args.value)
    elif args.cmd == "scan":
        out = client.scan(args.start, args.end, args.limit)
    else:
        out = {}
        for n in client.nodes:
            try:
                st = client.status(n)
                out[n] = {k: st[k] for k in ("id", "role", "term", "leader", "commit_index", "last_applied")}
            except OSError as e:
                out[n] = f"unreachable ({e.__class__.__name__})"
    print(json.dumps(out, indent=2) if not isinstance(out, str) else out)


if __name__ == "__main__":
    main()
