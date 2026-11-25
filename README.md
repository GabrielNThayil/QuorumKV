# QuorumKV

[![tests](https://github.com/GabrielNThayil/QuorumKV/actions/workflows/tests.yml/badge.svg)](https://github.com/GabrielNThayil/QuorumKV/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A fault-tolerant, strongly consistent distributed key-value store, built from scratch:
**Raft consensus** for replication and a **log-structured merge-tree (LSM) storage engine** for
persistence. Pure Python 3.11 standard library, no external dependencies.

A 3-node cluster keeps serving reads and writes when any single node is killed, and never
loses an acknowledged write. That claim is checked by a randomized fault-injection simulator
and by end-to-end tests that hard-kill the leader of a real multi-process cluster mid-workload.

```
            HTTP (PUT/GET/DELETE/CAS/SCAN)          307 redirect to leader
 client ───────────────────────────────▶ node ───────────────────────────────┐
                                          │                                  │
              ┌───────────────────────────┴──────────────────────────────┐   │
              │  HTTP API  ──▶  Raft core (pure state machine)           │◀──┘
              │                  │  election · PreVote · CheckQuorum     │
              │                  │  log replication · ReadIndex          │
              │                  ▼                                       │
              │   Raft log (CRC-checked, group-commit fsync)             │
              │                  │ committed entries                     │
              │                  ▼                                       │
              │   KV state machine ──▶ LSM engine                        │
              │                         WAL → memtable → SSTables        │
              │                         bloom filters · block cache      │
              │                         compaction · MANIFEST            │
              └───────────────────────────┬──────────────────────────────┘
                                          │ length-prefixed JSON over TCP
                                   peer nodes (n2, n3, …)
```

## Features

**Consensus (`quorumkv/raft/`)**
- Leader election with randomized timeouts; terms and votes persisted before any reply leaves the node.
- Log replication with **pipelining** and **fast conflict back-off** (a follower reports the
  conflicting term so the leader skips it in one round trip instead of one entry at a time).
- **Commit rule from Raft §5.4.2**: only current-term entries are committed by counting replicas;
  a new leader appends a no-op to commit older entries safely.
- **PreVote**: a partitioned node can't inflate its term and depose a healthy leader when it rejoins.
- **CheckQuorum**: a leader that loses contact with a majority steps down.
- **ReadIndex linearizable reads**: the leader confirms it is still leader with a quorum heartbeat
  round *after* the read arrives, so a deposed leader can never serve stale data. `?consistency=stale`
  opts into fast local reads.
- **Group commit**: one `fsync` covers every entry appended during an event-loop iteration.
- The core is a deterministic, I/O-free state machine (`tick` / `step` / `propose`); the network,
  disk and clock are injected by the host, which is what makes it testable.

**Storage engine (`quorumkv/storage/`)**
- Write-ahead log with per-record CRC32; a torn tail from a crash is detected and truncated.
- Memtable flushed to immutable **SSTables**: sorted data blocks, sparse index, **bloom filter**
  (~1% false positives, double hashing) and footer with a magic number.
- Point reads do bloom check → binary search on the sparse index → one block read, through an LRU **block cache**.
- Ordered **range scans** via a k-way heap merge across memtable and all SSTables (newest wins).
- **Compaction** merges SSTables and drops tombstones.
- Crash-safe file ordering: SSTable fsync → atomic `MANIFEST` replace → WAL reset. Orphaned files
  from an interrupted flush/compaction are cleaned up on startup.
- The Raft applied index is stored in the same WAL stream as the data, so after a crash the state
  machine and its applied-index marker can never disagree.

**Server and tooling**
- asyncio HTTP/1.1 API with keep-alive; followers answer writes with `307` to the leader.
- Atomic **compare-and-swap** (`POST /cas/<key>`), used by the tests to show no increment is lost or applied twice.
- `/status` (JSON) and `/metrics` (Prometheus format: Raft term/commit/applied, request counts,
  latency histograms, storage stats).
- Client library and CLI with leader discovery, retries and backoff.
- Local multi-process cluster launcher, Docker Compose cluster, benchmark harness.

## Quick start

```bash
# 3-node local cluster (raft ports 7001-7003, http ports 7004-7006)
python -m quorumkv.devcluster --nodes 3 --data ./data

# in another terminal
python -m quorumkv.client --nodes 127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006 put greeting hello
python -m quorumkv.client --nodes 127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006 get greeting
python -m quorumkv.client --nodes 127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006 status
curl -L -X PUT --data "bar" http://127.0.0.1:7005/kv/foo     # -L follows the leader redirect
curl http://127.0.0.1:7004/metrics
```

Or with Docker:

```bash
docker compose up --build
curl -L -X PUT --data "bar" http://localhost:8001/kv/foo
docker compose kill node1        # kill a node; the cluster keeps serving
```

Inside Compose the nodes reach each other as `nodeN:7000`, while clients on the host use the
published ports. `--client-urls` tells each node which address to put in leader redirects, so
`curl -L` against any node works from the host.

## Testing

```bash
python -m pytest tests -q          # 35 tests
```

| Suite | What it proves |
|---|---|
| `tests/test_raft.py` | Election, replication, leader crash, minority partitions, PreVote, CheckQuorum, ReadIndex. Includes **randomized chaos runs over 12 seeds**: 1,500 steps each of node crashes/restarts, network partitions, 5% message loss, 5% duplication and reordering. After every run it checks **election safety, log matching and state machine safety**, and that every entry ever seen committed is present on every node. |
| `tests/sim.py` | The deterministic network/cluster simulator behind those tests. One seed reproduces a failure exactly. |
| `tests/test_storage.py` | Bloom filter false-positive rate, SSTable round-trip, flush/compaction, WAL crash recovery, torn-write handling, and a 5,000-op randomized workload checked against a Python dict model (with restarts). |
| `tests/test_cluster.py` | Real processes over TCP: redirects, CAS, scans; **the leader is hard-killed twice during concurrent writes and CAS increments**, then every acknowledged write is checked and the counter is checked against the increments that succeeded; a restarted node catches up from disk plus the leader. |

## Benchmarks

`python bench/bench.py --seconds 8` on a laptop (Windows 11, all 3 nodes on one machine, 32
concurrent keep-alive clients, 100-byte values):

| Mode | Writes | Write p50 / p99 | Linearizable reads | Read p50 / p99 |
|---|---|---|---|---|
| fsync on (durable) | ~880 ops/s | 33 ms / 102 ms | ~5,400 ops/s | 5.9 ms / 7.4 ms |
| fsync off | ~5,060 ops/s | 5.3 ms / 18 ms | ~4,700 ops/s | 6.3 ms / 12 ms |

Durable writes are bound by `fsync` latency on the laptop SSD (each write needs a durable log
append on a majority). Group commit is what keeps throughput above one write per fsync.
Numbers vary by machine; rerun the script to measure yours.

## Design notes and trade-offs

- **Why a pure Raft core?** Same idea as etcd/raft: the algorithm never touches sockets, files or
  clocks. The host runs a fixed *ready cycle*: persist → send → apply. That ordering is the
  durability argument (nothing leaves the node before it is on disk), and it lets the simulator
  run thousands of adversarial schedules in seconds.
- **Unknown outcomes.** If leadership changes while a write is in flight, the server returns
  `503` marked `"outcome": "unknown"`. The entry may still commit under the next leader. The client
  retries idempotent operations (reads, PUT, DELETE) on its own, but raises `OutcomeUnknown` for CAS rather
  than retrying blindly, since a retried CAS could be misreported as failed after it already applied. Exactly-once semantics
  would need client sessions with sequence numbers (see roadmap).
- **Python and the GIL.** Each node is a single asyncio event loop, which removes most locking
  questions. The cost is raw throughput: flush and compaction run inline on the loop.

## Roadmap
- Raft log compaction via snapshots + `InstallSnapshot` for far-behind followers
- Dynamic membership changes (joint consensus / single-server changes)
- Client sessions for exactly-once writes
- Background flush/compaction; leveled compaction
- A Jepsen-style history checker (Knossos/Porcupine-style linearizability verification)

CI runs the full suite on Linux and Windows with Python 3.11-3.13, plus a Docker Compose smoke
test that writes through a follower and keeps serving after the leader container is killed.

## Layout

```
quorumkv/
  raft/node.py        Raft consensus core (election, replication, PreVote, CheckQuorum, ReadIndex)
  raft/storage.py     durable Raft log + hard state
  storage/wal.py      write-ahead log
  storage/sstable.py  SSTable format, sparse index, block cache
  storage/bloom.py    bloom filter
  storage/lsm.py      LSM engine: memtable, flush, compaction, scans, recovery
  statemachine.py     replicated KV state machine (put/delete/CAS)
  transport.py        peer TCP transport
  server.py           node process: HTTP API, metrics, Raft driving loop
  client.py           client library + CLI
  devcluster.py       local multi-process cluster launcher
tests/                simulator, unit, chaos and end-to-end tests
bench/bench.py        throughput/latency benchmark
```

## License

MIT, see [LICENSE](LICENSE).
