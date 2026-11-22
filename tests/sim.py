"""Deterministic in-process cluster simulator for the Raft core.

Every message goes through a simulated network that can drop, duplicate, delay
(and so reorder) and partition traffic. Nodes can crash and restart: the durable
MemoryRaftStorage object survives, while volatile state and the state machine are
rebuilt by replaying the log, as a real restart would do. All randomness comes
from one seed, so any failure can be replayed exactly.
"""
from __future__ import annotations

import json
import random
from collections import defaultdict

from quorumkv.raft.node import LEADER, NotLeader, RaftNode
from quorumkv.raft.storage import MemoryRaftStorage


class SimCluster:
    def __init__(self, n: int = 3, seed: int = 0, drop_rate: float = 0.0, dup_rate: float = 0.0,
                 max_delay: int = 1):
        self.rng = random.Random(seed)
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        self.drop_rate = drop_rate
        self.dup_rate = dup_rate
        self.max_delay = max_delay
        self.storages = {i: MemoryRaftStorage() for i in self.ids}
        self.nodes: dict[str, RaftNode] = {}
        self.applied: dict[str, list] = {}
        self.down: set[str] = set()
        self.groups: list[set[str]] | None = None
        self.inflight: list[tuple[int, dict]] = []
        self.now = 0
        self.leaders_by_term: dict[int, set[str]] = defaultdict(set)
        self.committed_entries: dict[tuple[int, int], object] = {}  # (index, term) -> cmd
        self.read_results: dict[object, bool] = {}
        for i in self.ids:
            self._start(i)

    # ------------------------------------------------------------------ nodes
    def _start(self, i: str) -> None:
        self.applied[i] = []
        log = self.applied[i]

        def apply(idx, cmd, log=log):
            log.append((idx, cmd))
            return idx

        def on_applied(idx, term, _result, i=i):
            self.committed_entries[(idx, term)] = self.storages[i].entry(idx)[1]

        def on_read(rid, ok):
            self.read_results[rid] = ok

        self.nodes[i] = RaftNode(
            i, self.ids, self.storages[i], apply, applied_index=0,
            rng=random.Random(self.rng.random()), election_ticks=(10, 20), heartbeat_ticks=2,
            max_batch=64, on_applied=on_applied, on_read_ready=on_read,
        )

    def crash(self, i: str) -> None:
        self.down.add(i)
        self.nodes[i].take_messages()

    def restart(self, i: str) -> None:
        self.down.discard(i)
        self._start(i)

    def partition(self, *groups) -> None:
        self.groups = [set(g) for g in groups]

    def heal(self) -> None:
        self.groups = None

    def can_talk(self, a: str, b: str) -> bool:
        if a in self.down or b in self.down:
            return False
        if self.groups is None:
            return True
        return any(a in g and b in g for g in self.groups)

    # ---------------------------------------------------------------- driving
    def _ready(self, i: str) -> None:
        node = self.nodes[i]
        node.storage.sync()
        for m in node.take_messages():
            if self.rng.random() < self.drop_rate:
                continue
            copies = 2 if self.rng.random() < self.dup_rate else 1
            for _ in range(copies):
                wire = json.loads(json.dumps(m))  # same serialisation as the real transport
                self.inflight.append((self.now + self.rng.randint(1, self.max_delay), wire))
        node.apply_committed()
        if node.role == LEADER:
            self.leaders_by_term[node.term].add(i)

    def step(self) -> None:
        self.now += 1
        due = [m for t, m in self.inflight if t <= self.now]
        self.inflight = [(t, m) for t, m in self.inflight if t > self.now]
        self.rng.shuffle(due)
        for m in due:
            if self.can_talk(m["from"], m["to"]):
                self.nodes[m["to"]].step(m)
                self._ready(m["to"])
        for i in self.ids:
            if i not in self.down:
                self.nodes[i].tick()
                self._ready(i)

    def run(self, steps: int) -> None:
        for _ in range(steps):
            self.step()

    def leader(self) -> str | None:
        live = [i for i in self.ids if i not in self.down and self.nodes[i].role == LEADER]
        if not live:
            return None
        return max(live, key=lambda i: self.nodes[i].term)

    def wait_for_leader(self, max_steps: int = 500) -> str:
        for _ in range(max_steps):
            ldr = self.leader()
            if ldr is not None and self._leader_has_quorum(ldr):
                return ldr
            self.step()
        raise AssertionError("no leader elected")

    def _leader_has_quorum(self, ldr: str) -> bool:
        reachable = sum(1 for j in self.ids if j == ldr or self.can_talk(ldr, j))
        return reachable >= self.nodes[ldr].quorum

    def propose(self, cmd) -> tuple[int, int] | None:
        ldr = self.leader()
        if ldr is None:
            return None
        try:
            idx, term = self.nodes[ldr].propose(cmd)
        except NotLeader:
            return None
        self.nodes[ldr].replicate()
        self._ready(ldr)
        return idx, term

    # ------------------------------------------------------------- invariants
    def check_election_safety(self) -> None:
        for term, leaders in self.leaders_by_term.items():
            assert len(leaders) <= 1, f"two leaders in term {term}: {leaders}"

    def check_log_matching(self) -> None:
        logs = {i: self.storages[i].entries for i in self.ids}
        for a in self.ids:
            for b in self.ids:
                if a >= b:
                    continue
                la, lb = logs[a], logs[b]
                n = min(len(la), len(lb))
                # Find the last index where terms agree; everything before must be identical.
                for k in range(n - 1, -1, -1):
                    if la[k][0] == lb[k][0]:
                        assert la[: k + 1] == lb[: k + 1], f"log matching violated between {a} and {b}"
                        break

    def check_state_machine_safety(self) -> None:
        longest = max(self.applied.values(), key=len)
        for i, log in self.applied.items():
            assert log == longest[: len(log)], f"{i} applied a different sequence"

    def check_all(self) -> None:
        self.check_election_safety()
        self.check_log_matching()
        self.check_state_machine_safety()
