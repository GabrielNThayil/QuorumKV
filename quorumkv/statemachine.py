"""Replicated key-value state machine backed by the LSM engine.

The index of the last applied Raft entry is stored in the same LSM (and so the same
WAL stream) as the data it describes. After a crash the two can never disagree: the
node resumes applying from exactly the entry after the last one that reached disk.
"""
from __future__ import annotations

from typing import Any

from .storage.lsm import LSMEngine

APPLIED_KEY = b"\x00raft/applied"
INTERNAL_PREFIX = b"\x00"


class KVStateMachine:
    def __init__(self, engine: LSMEngine):
        self.engine = engine

    @property
    def applied_index(self) -> int:
        v = self.engine.get(APPLIED_KEY)
        return int(v) if v else 0

    def apply(self, index: int, cmd: dict | None) -> Any:
        result: Any = None
        if cmd is not None:
            op = cmd["op"]
            key = cmd["key"].encode()
            if op == "put":
                self.engine.put(key, cmd["value"].encode())
                result = {"ok": True}
            elif op == "delete":
                existed = self.engine.get(key) is not None
                self.engine.delete(key)
                result = {"ok": True, "deleted": existed}
            elif op == "cas":
                cur = self.engine.get(key)
                expected = cmd.get("expected")
                if (cur is None and expected is None) or (cur is not None and cur.decode() == expected):
                    self.engine.put(key, cmd["value"].encode())
                    result = {"ok": True, "swapped": True}
                else:
                    result = {"ok": True, "swapped": False, "current": cur.decode() if cur is not None else None}
            else:
                result = {"ok": False, "error": f"unknown op {op}"}
        self.engine.put(APPLIED_KEY, str(index).encode())
        return result

    def get(self, key: str) -> str | None:
        v = self.engine.get(key.encode())
        return v.decode() if v is not None else None

    def scan(self, start: str | None, end: str | None, limit: int) -> list[tuple[str, str]]:
        lo = start.encode() if start else b"\x01"
        rows = self.engine.scan(max(lo, b"\x01"), end.encode() if end else None, limit)
        return [(k.decode(), v.decode()) for k, v in rows]
