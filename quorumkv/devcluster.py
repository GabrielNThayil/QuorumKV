"""Launch and control a local multi-process cluster (used by tests, benchmarks and demos).

    python -m quorumkv.devcluster --nodes 3 --data ./data
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time

from .client import QuorumClient


def free_ports(n: int) -> list[int]:
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


class LocalCluster:
    def __init__(self, n: int, data_dir: str, fsync: bool = True, base_port: int | None = None,
                 log_dir: str | None = None):
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        ports = list(range(base_port, base_port + 2 * n)) if base_port else free_ports(2 * n)
        self.raft_ports = dict(zip(self.ids, ports[:n]))
        self.http_ports = dict(zip(self.ids, ports[n:]))
        self.spec = ",".join(f"{i}=127.0.0.1:{self.raft_ports[i]}:{self.http_ports[i]}" for i in self.ids)
        self.data_dir = data_dir
        self.log_dir = log_dir or data_dir
        self.fsync = fsync
        self.procs: dict[str, subprocess.Popen] = {}

    def http_addr(self, i: str) -> str:
        return f"127.0.0.1:{self.http_ports[i]}"

    @property
    def addrs(self) -> list[str]:
        return [self.http_addr(i) for i in self.ids]

    def start(self, i: str) -> None:
        os.makedirs(self.log_dir, exist_ok=True)
        cmd = [sys.executable, "-m", "quorumkv.server", "--id", i, "--cluster", self.spec,
               "--data", os.path.join(self.data_dir, i), "--log-level", "WARNING"]
        if not self.fsync:
            cmd.append("--no-fsync")
        log = open(os.path.join(self.log_dir, f"{i}.log"), "ab")
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.procs[i] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=root)

    def start_all(self) -> None:
        for i in self.ids:
            self.start(i)

    def kill(self, i: str) -> None:
        """Hard kill (SIGKILL / TerminateProcess): no graceful shutdown, no final flush."""
        p = self.procs.pop(i, None)
        if p:
            p.kill()
            p.wait()

    def stop_all(self) -> None:
        for i in list(self.procs):
            self.kill(i)

    def leader(self, timeout: float = 10.0) -> str:
        client = QuorumClient(self.addrs, timeout=1.0)
        deadline = time.time() + timeout
        while time.time() < deadline:
            for i in self.ids:
                if i not in self.procs:
                    continue
                try:
                    st = client.status(self.http_addr(i))
                except OSError:
                    continue
                if st["role"] == "leader":
                    return i
            time.sleep(0.1)
        raise TimeoutError("no leader elected")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run a local QuorumKV cluster")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--data", default="data")
    ap.add_argument("--base-port", type=int, default=7001, help="raft ports start here, then http ports")
    ap.add_argument("--no-fsync", action="store_true")
    args = ap.parse_args()
    c = LocalCluster(args.nodes, args.data, fsync=not args.no_fsync, base_port=args.base_port)
    c.start_all()
    print(f"cluster: {c.spec}")
    print(f"leader:  {c.leader()}")
    print(f"try:     python -m quorumkv.client --nodes {','.join(c.addrs)} put hello world")
    print("Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        c.stop_all()


if __name__ == "__main__":
    main()
