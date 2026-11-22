"""Throughput / latency benchmark against a fresh local cluster.

    python bench/bench.py --nodes 3 --concurrency 32 --seconds 10
    python bench/bench.py --no-fsync

Uses raw asyncio HTTP/1.1 keep-alive connections to the leader so the client is
not the bottleneck. Reports ops/sec and p50/p95/p99 latency for writes and
linearizable reads.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from quorumkv.devcluster import LocalCluster  # noqa: E402


async def worker(host, port, method, keys, deadline, lat, value):
    reader, writer = await asyncio.open_connection(host, port)
    i = 0
    try:
        while time.perf_counter() < deadline:
            key = keys[i % len(keys)]
            i += 1
            body = value if method == "PUT" else b""
            req = (f"{method} /kv/{key} HTTP/1.1\r\nHost: x\r\nContent-Length: {len(body)}\r\n\r\n").encode() + body
            t0 = time.perf_counter()
            writer.write(req)
            await writer.drain()
            status = await reader.readline()
            length = 0
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b""):
                    break
                if h.lower().startswith(b"content-length:"):
                    length = int(h.split(b":")[1])
            await reader.readexactly(length)
            if status.split()[1] in (b"200", b"404"):
                lat.append(time.perf_counter() - t0)
    finally:
        writer.close()


async def phase(host, port, method, concurrency, seconds, nkeys, value):
    lat: list[float] = []
    deadline = time.perf_counter() + seconds
    keys = [f"bench{k}" for k in range(nkeys)]
    start = time.perf_counter()
    await asyncio.gather(*[
        worker(host, port, method, keys[w::concurrency] or keys, deadline, lat, value)
        for w in range(concurrency)
    ])
    elapsed = time.perf_counter() - start
    lat.sort()
    q = lambda p: lat[min(len(lat) - 1, int(p * len(lat)))] * 1000  # noqa: E731
    return {"ops": len(lat), "ops_per_s": len(lat) / elapsed, "p50_ms": q(0.50), "p95_ms": q(0.95),
            "p99_ms": q(0.99), "mean_ms": statistics.fmean(lat) * 1000 if lat else 0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--seconds", type=float, default=10)
    ap.add_argument("--keys", type=int, default=10_000)
    ap.add_argument("--value-size", type=int, default=100)
    ap.add_argument("--no-fsync", action="store_true")
    args = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="qkv-bench-")
    c = LocalCluster(args.nodes, tmp, fsync=not args.no_fsync)
    c.start_all()
    try:
        leader = c.leader(timeout=15)
        port = c.http_ports[leader]
        value = b"x" * args.value_size
        print(f"{args.nodes} nodes, fsync={'off' if args.no_fsync else 'on'}, "
              f"concurrency={args.concurrency}, value={args.value_size}B, {args.seconds}s per phase")
        for name, method in (("write (PUT)", "PUT"), ("linearizable read (GET)", "GET")):
            r = asyncio.run(phase("127.0.0.1", port, method, args.concurrency, args.seconds, args.keys, value))
            print(f"{name:26s} {r['ops_per_s']:8.0f} ops/s   p50 {r['p50_ms']:6.2f} ms   "
                  f"p95 {r['p95_ms']:6.2f} ms   p99 {r['p99_ms']:6.2f} ms   ({r['ops']} ops)")
    finally:
        c.stop_all()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
