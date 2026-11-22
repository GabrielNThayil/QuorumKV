"""Log-structured merge-tree storage engine.

Writes go to the WAL and an in-memory memtable. When the memtable passes a size
limit it is written out as an immutable SSTable and the WAL is reset. Reads check
the memtable, then SSTables newest-to-oldest. When too many SSTables pile up they
are merged into one (newest value wins, tombstones dropped).

Crash safety comes from ordering: SSTable fsync -> MANIFEST atomic replace -> WAL
reset. A crash between any two steps leaves either the old or the new state, and
replaying a WAL whose contents were already flushed is harmless (writes are
idempotent).
"""
from __future__ import annotations

import heapq
import json
import os
from typing import Iterator

from .sstable import MISSING, BlockCache, SSTable, write_sstable
from .wal import OP_DELETE, OP_PUT, WriteAheadLog

_ENTRY_OVERHEAD = 48  # rough per-entry memory cost of the dict slot + objects


class LSMEngine:
    def __init__(
        self,
        directory: str,
        memtable_limit: int = 4 << 20,
        compaction_trigger: int = 4,
        fsync_wal: bool = False,
        block_cache_blocks: int = 1024,
    ):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self.memtable_limit = memtable_limit
        self.compaction_trigger = compaction_trigger
        self.cache = BlockCache(block_cache_blocks)
        self.stats = {"flushes": 0, "compactions": 0, "bloom_negatives": 0, "table_probes": 0}

        self._manifest_path = os.path.join(directory, "MANIFEST")
        self._wal_path = os.path.join(directory, "wal.log")
        manifest = {"tables": [], "next_id": 1}
        if os.path.exists(self._manifest_path):
            with open(self._manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        self._next_id: int = manifest["next_id"]
        self.tables: list[SSTable] = [  # oldest first
            SSTable(os.path.join(directory, name), self.cache) for name in manifest["tables"]
        ]
        self._remove_orphans({t.path for t in self.tables})

        self.memtable: dict[bytes, bytes | None] = {}
        self._mem_bytes = 0
        for op, key, value in WriteAheadLog.replay(self._wal_path):
            self._mem_set(key, value if op == OP_PUT else None)
        self.wal = WriteAheadLog(self._wal_path, fsync=fsync_wal)

    # ------------------------------------------------------------------ writes
    def put(self, key: bytes, value: bytes) -> None:
        self.wal.append(OP_PUT, key, value)
        self._mem_set(key, value)
        self._maybe_flush()

    def delete(self, key: bytes) -> None:
        self.wal.append(OP_DELETE, key, None)
        self._mem_set(key, None)
        self._maybe_flush()

    def _mem_set(self, key: bytes, value: bytes | None) -> None:
        old = self.memtable.get(key, MISSING)
        if old is MISSING:
            self._mem_bytes += len(key) + _ENTRY_OVERHEAD
        elif old is not None:
            self._mem_bytes -= len(old)
        if value is not None:
            self._mem_bytes += len(value)
        self.memtable[key] = value

    def _maybe_flush(self) -> None:
        if self._mem_bytes >= self.memtable_limit:
            self.flush()

    # ------------------------------------------------------------------- reads
    def get(self, key: bytes) -> bytes | None:
        v = self.memtable.get(key, MISSING)
        if v is not MISSING:
            return v
        for table in reversed(self.tables):
            self.stats["table_probes"] += 1
            if not table.bloom.might_contain(key):
                self.stats["bloom_negatives"] += 1
                continue
            v = table.get(key)
            if v is not MISSING:
                return v
        return None

    def scan(self, start: bytes | None = None, end: bytes | None = None, limit: int | None = None):
        """Return live (key, value) pairs with start <= key < end, in key order."""
        out = []
        for key, value in self._merged(start):
            if end is not None and key >= end:
                break
            if value is None:
                continue
            out.append((key, value))
            if limit is not None and len(out) >= limit:
                break
        return out

    def _merged(self, start: bytes | None) -> Iterator[tuple[bytes, bytes | None]]:
        """Merge memtable + all tables in key order; for duplicate keys the newest wins."""
        mem = sorted(
            (k, v) for k, v in self.memtable.items() if start is None or k >= start
        )
        sources = [iter(mem)] + [t.iter_from(start) for t in reversed(self.tables)]
        # Tag every item with its source rank so equal keys come out newest first.
        tagged = [((k, rank, v) for k, v in src) for rank, src in enumerate(sources)]
        last = None
        for key, _rank, value in heapq.merge(*tagged, key=lambda x: (x[0], x[1])):
            if key == last:
                continue
            last = key
            yield key, value

    # ------------------------------------------------------- flush / compaction
    def flush(self) -> None:
        if not self.memtable:
            return
        path = self._new_table_path()
        items = sorted(self.memtable.items())
        write_sstable(path, items, len(items))
        self.tables.append(SSTable(path, self.cache))
        self._save_manifest()
        # Only now is it safe to drop the WAL.
        self.wal.close()
        open(self._wal_path, "wb").close()
        self.wal = WriteAheadLog(self._wal_path, fsync=self.wal.fsync)
        self.memtable = {}
        self._mem_bytes = 0
        self.stats["flushes"] += 1
        if len(self.tables) >= self.compaction_trigger:
            self.compact()

    def compact(self) -> None:
        """Full merge of every SSTable into one. Safe to drop tombstones: nothing older remains."""
        if len(self.tables) < 2:
            return
        old = self.tables
        path = self._new_table_path()
        sources = [t.iter_from(None) for t in reversed(old)]
        tagged = [((k, rank, v) for k, v in src) for rank, src in enumerate(sources)]

        def live():
            last = None
            for key, _rank, value in heapq.merge(*tagged, key=lambda x: (x[0], x[1])):
                if key == last:
                    continue
                last = key
                if value is not None:
                    yield key, value

        write_sstable(path, live(), sum(t.count for t in old))
        self.tables = [SSTable(path, self.cache)]
        self._save_manifest()
        for t in old:
            t.close()
            os.remove(t.path)
        self.stats["compactions"] += 1

    def _new_table_path(self) -> str:
        name = f"{self._next_id:06d}.sst"
        self._next_id += 1
        return os.path.join(self.dir, name)

    def _save_manifest(self) -> None:
        tmp = self._manifest_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"tables": [os.path.basename(t.path) for t in self.tables], "next_id": self._next_id}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._manifest_path)

    def _remove_orphans(self, live: set[str]) -> None:
        """Delete tables left behind by a crash mid-flush or mid-compaction."""
        for name in os.listdir(self.dir):
            path = os.path.join(self.dir, name)
            if (name.endswith(".sst") and path not in live) or name.endswith(".sst.tmp"):
                os.remove(path)

    def close(self) -> None:
        self.wal.close()
        for t in self.tables:
            t.close()

    def describe(self) -> dict:
        return {
            "memtable_keys": len(self.memtable),
            "memtable_bytes": self._mem_bytes,
            "sstables": len(self.tables),
            "sstable_bytes": sum(t.size_bytes for t in self.tables),
            "block_cache_hits": self.cache.hits,
            "block_cache_misses": self.cache.misses,
            **self.stats,
        }
