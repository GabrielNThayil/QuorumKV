"""Durable Raft state: current term, vote, and the replicated log.

Log indices are 1-based; index 0 is a sentinel with term 0. Each entry is a
[term, command] pair, where command is a JSON-serialisable dict, or None for a
leader's no-op entry.
"""
from __future__ import annotations

import json
import os
import zlib


class MemoryRaftStorage:
    """In-memory storage. Survives a simulated crash because the test harness keeps the object."""

    def __init__(self):
        self.term = 0
        self.voted_for: str | None = None
        self.entries: list[list] = []

    def save_hard_state(self, term: int, voted_for: str | None) -> None:
        self.term = term
        self.voted_for = voted_for

    def last_index(self) -> int:
        return len(self.entries)

    def term_at(self, index: int) -> int | None:
        if index == 0:
            return 0
        if index < 0 or index > len(self.entries):
            return None
        return self.entries[index - 1][0]

    def entry(self, index: int) -> list:
        return self.entries[index - 1]

    def entries_from(self, start: int, max_n: int) -> list[list]:
        return self.entries[start - 1 : start - 1 + max_n]

    def append(self, entries: list[list]) -> None:
        self.entries.extend([list(e) for e in entries])

    def truncate_from(self, index: int) -> None:
        del self.entries[index - 1 :]

    def sync(self) -> None:
        pass


class FileRaftStorage(MemoryRaftStorage):
    """Append-only log file + atomically replaced hard-state file.

    Log line format: "<crc32 hex> <json [term, cmd]>\\n". On load, the first line with
    a bad checksum (a torn write) and everything after it is discarded.
    """

    def __init__(self, directory: str, fsync: bool = True):
        super().__init__()
        os.makedirs(directory, exist_ok=True)
        self.fsync = fsync
        self._state_path = os.path.join(directory, "state.json")
        self._log_path = os.path.join(directory, "raft.log")
        self._dirty = False

        if os.path.exists(self._state_path):
            with open(self._state_path, "r", encoding="utf-8") as f:
                st = json.load(f)
            self.term, self.voted_for = st["term"], st["voted_for"]

        good = 0
        if os.path.exists(self._log_path):
            with open(self._log_path, "rb") as f:
                for line in f:
                    if not line.endswith(b"\n"):
                        break
                    crc, _, body = line[:-1].partition(b" ")
                    try:
                        if int(crc, 16) != zlib.crc32(body):
                            break
                        self.entries.append(json.loads(body))
                    except ValueError:
                        break
                    good += len(line)
            with open(self._log_path, "r+b") as f:
                f.truncate(good)
        self._f = open(self._log_path, "ab")

    @staticmethod
    def _encode(entry) -> bytes:
        body = json.dumps(entry, separators=(",", ":")).encode()
        return b"%08x %s\n" % (zlib.crc32(body), body)

    def save_hard_state(self, term: int, voted_for: str | None) -> None:
        super().save_hard_state(term, voted_for)
        tmp = self._state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"term": term, "voted_for": voted_for}, f)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self._state_path)

    def append(self, entries: list[list]) -> None:
        super().append(entries)
        self._f.write(b"".join(self._encode(e) for e in entries))
        self._dirty = True

    def truncate_from(self, index: int) -> None:
        super().truncate_from(index)
        # Rare (only on log conflicts), so rewriting the whole file is acceptable.
        self._f.close()
        tmp = self._log_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(b"".join(self._encode(e) for e in self.entries))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._log_path)
        self._f = open(self._log_path, "ab")
        self._dirty = False

    def sync(self) -> None:
        """Group commit: one flush/fsync covers every entry appended since the last call."""
        if self._dirty:
            self._f.flush()
            if self.fsync:
                os.fsync(self._f.fileno())
            self._dirty = False

    def close(self) -> None:
        self.sync()
        self._f.close()
