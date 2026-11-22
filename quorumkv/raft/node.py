"""Raft consensus core.

A deterministic, I/O-free state machine in the style of etcd/raft. The host drives it:

    node.tick()            advance logical time by one tick
    node.step(msg)         deliver a message from a peer
    node.propose(cmd)      append a client command (leader only)
    node.read_index(rid)   start a linearizable read (leader only)

and after each call runs one "ready" cycle, in this order:

    storage.sync()                  make new log entries / votes durable
    send(node.take_messages())      only then let messages leave the node
    node.apply_committed()          apply newly committed entries to the state machine

Implemented: leader election with randomized timeouts, PreVote (a partitioned node
cannot inflate terms and depose a healthy leader when it rejoins), CheckQuorum (a
leader that loses contact with a majority steps down), log replication with
pipelining and fast conflict back-off, commit only of current-term entries (Raft
paper section 5.4.2), leader no-op on election, and ReadIndex linearizable reads.
"""
from __future__ import annotations

import random
from typing import Any, Callable

FOLLOWER = "follower"
PRE_CANDIDATE = "pre_candidate"
CANDIDATE = "candidate"
LEADER = "leader"


class NotLeader(Exception):
    def __init__(self, leader_id: str | None):
        super().__init__(f"not leader (leader is {leader_id})")
        self.leader_id = leader_id


class RaftNode:
    def __init__(
        self,
        node_id: str,
        members: list[str],
        storage,
        apply_fn: Callable[[int, Any], Any],
        applied_index: int = 0,
        rng: random.Random | None = None,
        election_ticks: tuple[int, int] = (10, 20),
        heartbeat_ticks: int = 2,
        max_batch: int = 512,
        on_applied: Callable[[int, int, Any], None] | None = None,
        on_read_ready: Callable[[Any, bool], None] | None = None,
        on_role_change: Callable[[str], None] | None = None,
    ):
        self.id = node_id
        self.peers = [m for m in members if m != node_id]
        self.storage = storage
        self.apply_fn = apply_fn
        self.rng = rng or random.Random()
        self.election_ticks = election_ticks
        self.heartbeat_ticks = heartbeat_ticks
        self.max_batch = max_batch
        self.on_applied = on_applied or (lambda i, t, r: None)
        self.on_read_ready = on_read_ready or (lambda rid, ok: None)
        self.on_role_change = on_role_change or (lambda role: None)

        self.role = FOLLOWER
        self.leader_id: str | None = None
        # Applied entries are committed by definition, so they bound commit_index from below.
        self.commit_index = applied_index
        self.last_applied = applied_index

        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.votes: set[str] = set()
        self.elapsed = 0
        self.heartbeat_elapsed = 0
        self.quorum_elapsed = 0
        self.recent_active: set[str] = set()
        self.term_start_index = 0

        self.hb_seq = 0
        self.peer_ack_seq: dict[str, int] = {}
        self.pending_reads: list[tuple[Any, int, int]] = []  # (rid, read_index, required_seq)

        self.outbox: list[dict] = []
        self._reset_election_timeout()

    # ----------------------------------------------------------------- helpers
    @property
    def term(self) -> int:
        return self.storage.term

    @property
    def quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def last_index(self) -> int:
        return self.storage.last_index()

    def last_term(self) -> int:
        return self.storage.term_at(self.storage.last_index())

    def _reset_election_timeout(self) -> None:
        lo, hi = self.election_ticks
        self.election_timeout = self.rng.randint(lo, hi)
        self.elapsed = 0

    def _send(self, to: str, msg: dict, term: int | None = None) -> None:
        msg["from"] = self.id
        msg["to"] = to
        msg["term"] = self.term if term is None else term
        self.outbox.append(msg)

    def take_messages(self) -> list[dict]:
        out, self.outbox = self.outbox, []
        return out

    def _log_up_to_date(self, last_term: int, last_index: int) -> bool:
        return (last_term, last_index) >= (self.last_term(), self.last_index())

    # ------------------------------------------------------------ transitions
    def _become_follower(self, term: int, leader_id: str | None) -> None:
        was = self.role
        if term > self.term:
            self.storage.save_hard_state(term, None)
        self.role = FOLLOWER
        self.leader_id = leader_id
        self._reset_election_timeout()
        if was == LEADER:
            self._fail_reads()
        if was != FOLLOWER:
            self.on_role_change(FOLLOWER)

    def _become_pre_candidate(self) -> None:
        self.role = PRE_CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timeout()
        for p in self.peers:
            self._send(
                p,
                {"type": "prevote", "last_index": self.last_index(), "last_term": self.last_term()},
                term=self.term + 1,
            )
        self._check_votes()

    def _become_candidate(self) -> None:
        self.role = CANDIDATE
        self.leader_id = None
        self.storage.save_hard_state(self.term + 1, self.id)
        self.votes = {self.id}
        self._reset_election_timeout()
        for p in self.peers:
            self._send(p, {"type": "vote", "last_index": self.last_index(), "last_term": self.last_term()})
        self._check_votes()

    def _become_leader(self) -> None:
        self.role = LEADER
        self.leader_id = self.id
        last = self.last_index()
        self.next_index = {p: last + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.peer_ack_seq = {p: 0 for p in self.peers}
        self.recent_active = set()
        self.heartbeat_elapsed = 0
        self.quorum_elapsed = 0
        # A no-op in the new term lets the leader commit entries from earlier terms (5.4.2).
        self.storage.append([[self.term, None]])
        self.term_start_index = self.last_index()
        self.on_role_change(LEADER)
        self.broadcast_append()
        self._maybe_commit()

    def _check_votes(self) -> None:
        if len(self.votes) < self.quorum:
            return
        if self.role == PRE_CANDIDATE:
            self._become_candidate()
        elif self.role == CANDIDATE:
            self._become_leader()

    # ------------------------------------------------------------------- time
    def tick(self) -> None:
        if self.role == LEADER:
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= self.heartbeat_ticks:
                self.heartbeat_elapsed = 0
                self.broadcast_append()
            self.quorum_elapsed += 1
            if self.quorum_elapsed >= self.election_ticks[1]:
                self.quorum_elapsed = 0
                # CheckQuorum: a leader cut off from a majority must stop acting as leader.
                if self.peers and len(self.recent_active) + 1 < self.quorum:
                    self._become_follower(self.term, None)
                    return
                self.recent_active = set()
        else:
            self.elapsed += 1
            if self.elapsed >= self.election_timeout:
                if self.peers:
                    self._become_pre_candidate()
                else:
                    self._become_candidate()

    # --------------------------------------------------------------- messages
    def step(self, m: dict) -> None:
        mtype = m["type"]
        if mtype == "prevote":
            return self._on_prevote(m)
        if mtype == "prevote_resp":
            if not m["granted"] and m["term"] > self.term:
                self._become_follower(m["term"], None)
            elif m["granted"] and self.role == PRE_CANDIDATE and m["term"] == self.term + 1:
                self.votes.add(m["from"])
                self._check_votes()
            return

        if m["term"] > self.term:
            self._become_follower(m["term"], m["from"] if mtype == "append" else None)
        elif m["term"] < self.term:
            # Stale sender: answer so it learns the newer term.
            if mtype == "append":
                self._send(m["from"], {"type": "append_resp", "success": False, "match_index": 0,
                                       "hint": 0, "seq": m.get("seq", 0)})
            elif mtype == "vote":
                self._send(m["from"], {"type": "vote_resp", "granted": False})
            return

        if mtype == "vote":
            self._on_vote(m)
        elif mtype == "vote_resp":
            if self.role == CANDIDATE and m["granted"]:
                self.votes.add(m["from"])
                self._check_votes()
        elif mtype == "append":
            self._on_append(m)
        elif mtype == "append_resp":
            self._on_append_resp(m)

    def _heard_from_leader_recently(self) -> bool:
        return self.role == LEADER or (
            self.leader_id is not None and self.elapsed < self.election_ticks[0]
        )

    def _on_prevote(self, m: dict) -> None:
        granted = (
            m["term"] > self.term
            and not self._heard_from_leader_recently()
            and self._log_up_to_date(m["last_term"], m["last_index"])
        )
        self._send(m["from"], {"type": "prevote_resp", "granted": granted},
                   term=m["term"] if granted else self.term)

    def _on_vote(self, m: dict) -> None:
        granted = (
            self.storage.voted_for in (None, m["from"])
            and self._log_up_to_date(m["last_term"], m["last_index"])
        )
        if granted:
            self.storage.save_hard_state(self.term, m["from"])
            self._reset_election_timeout()
        self._send(m["from"], {"type": "vote_resp", "granted": granted})

    def _on_append(self, m: dict) -> None:
        if self.role != FOLLOWER:
            self._become_follower(self.term, m["from"])
        self.leader_id = m["from"]
        self.elapsed = 0

        prev_i, prev_t = m["prev_index"], m["prev_term"]
        reply = {"type": "append_resp", "seq": m.get("seq", 0)}
        last = self.last_index()
        if prev_i > last:
            reply.update(success=False, match_index=0, hint=last + 1)
        elif self.storage.term_at(prev_i) != prev_t:
            # Fast back-off: skip the whole conflicting term in one round trip.
            ct = self.storage.term_at(prev_i)
            i = prev_i
            while i - 1 > self.commit_index and self.storage.term_at(i - 1) == ct:
                i -= 1
            reply.update(success=False, match_index=0, hint=i, conflict_term=ct)
        else:
            entries = m["entries"]
            for j, entry in enumerate(entries):
                idx = prev_i + 1 + j
                existing = self.storage.term_at(idx)
                if existing == entry[0]:
                    continue
                if existing is not None:
                    if idx <= self.commit_index:
                        raise AssertionError("leader tried to overwrite a committed entry")
                    self.storage.truncate_from(idx)
                self.storage.append(entries[j:])
                break
            match = prev_i + len(entries)
            if m["leader_commit"] > self.commit_index:
                self.commit_index = min(m["leader_commit"], match)
            reply.update(success=True, match_index=match)
        self._send(m["from"], reply)

    def _on_append_resp(self, m: dict) -> None:
        if self.role != LEADER:
            return
        p = m["from"]
        self.recent_active.add(p)
        if m.get("seq", 0) > self.peer_ack_seq.get(p, 0):
            self.peer_ack_seq[p] = m["seq"]
        if m["success"]:
            if m["match_index"] > self.match_index[p]:
                self.match_index[p] = m["match_index"]
            self.next_index[p] = max(self.next_index[p], self.match_index[p] + 1)
            self._maybe_commit()
        else:
            new_next = m["hint"]
            ct = m.get("conflict_term")
            if ct is not None:
                # If we have entries from that term, resume just after our last one.
                i = self.last_index()
                while i > 0 and self.storage.term_at(i) > ct:
                    i -= 1
                if i > 0 and self.storage.term_at(i) == ct:
                    new_next = i + 1
            self.next_index[p] = max(self.match_index[p] + 1, min(new_next, self.last_index() + 1))
        if self.next_index[p] <= self.last_index():
            self._send_append(p)
        self._check_reads()

    # ------------------------------------------------------------ replication
    def _send_append(self, p: str) -> None:
        ni = self.next_index[p]
        prev = ni - 1
        entries = self.storage.entries_from(ni, self.max_batch)
        self._send(p, {
            "type": "append",
            "prev_index": prev,
            "prev_term": self.storage.term_at(prev),
            "entries": entries,
            "leader_commit": self.commit_index,
            "seq": self.hb_seq,
        })
        if entries:
            self.next_index[p] = ni + len(entries)  # optimistic pipelining

    def broadcast_append(self) -> None:
        self.hb_seq += 1
        for p in self.peers:
            self._send_append(p)

    def replicate(self) -> None:
        """Push newly proposed entries to followers right away instead of waiting for a heartbeat."""
        if self.role != LEADER:
            return
        self.broadcast_append()
        self._maybe_commit()

    def _maybe_commit(self) -> None:
        if self.role != LEADER:
            return
        matches = sorted([self.last_index()] + [self.match_index[p] for p in self.peers], reverse=True)
        n = matches[self.quorum - 1]
        # Only entries from the current term are committed by counting replicas (5.4.2).
        if n > self.commit_index and self.storage.term_at(n) == self.term:
            self.commit_index = n

    # ------------------------------------------------------------- client API
    def propose(self, cmd: Any) -> tuple[int, int]:
        if self.role != LEADER:
            raise NotLeader(self.leader_id)
        self.storage.append([[self.term, cmd]])
        return self.last_index(), self.term

    def read_index(self, rid: Any) -> None:
        """Register a linearizable read. on_read_ready(rid, True) fires once it is safe to
        read the local state machine: leadership was confirmed by a quorum *after* the
        request arrived, and everything committed at that point has been applied."""
        if self.role != LEADER:
            raise NotLeader(self.leader_id)
        read_index = max(self.commit_index, self.term_start_index)
        self.pending_reads.append((rid, read_index, self.hb_seq + 1))
        self.broadcast_append()
        self._check_reads()

    def _check_reads(self) -> None:
        if not self.pending_reads or self.role != LEADER:
            return
        waiting = []
        for rid, ri, seq in self.pending_reads:
            acks = 1 + sum(1 for p in self.peers if self.peer_ack_seq.get(p, 0) >= seq)
            if acks >= self.quorum and self.last_applied >= ri:
                self.on_read_ready(rid, True)
            else:
                waiting.append((rid, ri, seq))
        self.pending_reads = waiting

    def _fail_reads(self) -> None:
        reads, self.pending_reads = self.pending_reads, []
        for rid, _, _ in reads:
            self.on_read_ready(rid, False)

    def apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            i = self.last_applied + 1
            term, cmd = self.storage.entry(i)
            result = self.apply_fn(i, cmd)
            self.last_applied = i
            self.on_applied(i, term, result)
        self._check_reads()

    def status(self) -> dict:
        return {
            "id": self.id,
            "role": self.role,
            "term": self.term,
            "leader": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_index": self.last_index(),
            "peers": {
                p: {"match_index": self.match_index.get(p), "next_index": self.next_index.get(p)}
                for p in self.peers
            } if self.role == LEADER else {},
        }
