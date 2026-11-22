"""Immutable sorted string tables (SSTables).

File layout:
    [data]    entries sorted by key: key_len u32 | value_len i32 (-1 = tombstone) | key | value
    [index]   sparse index, one entry per block of INDEX_EVERY records:
              count u32, then (key_len u32 | key | offset u64)*
    [bloom]   serialized BloomFilter over every key in the table
    [footer]  index_offset u64 | bloom_offset u64 | entry_count u64 | magic u64

A point lookup checks the bloom filter, binary-searches the sparse index and
reads a single block. Blocks are kept in a small shared LRU cache.
"""
from __future__ import annotations

import os
import struct
from bisect import bisect_right
from collections import OrderedDict
from typing import Iterable, Iterator

from .bloom import BloomFilter

MAGIC = 0x564B4D55524F5551  # "QUORUMKV"
INDEX_EVERY = 16
_FOOTER = struct.Struct("<QQQQ")
_ENTRY = struct.Struct("<Ii")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")

MISSING = object()  # sentinel: key not present in this table (distinct from a tombstone)


class CorruptTableError(Exception):
    pass


class BlockCache:
    def __init__(self, capacity: int = 512):
        self.capacity = capacity
        self._data: OrderedDict = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        block = self._data.get(key)
        if block is not None:
            self._data.move_to_end(key)
            self.hits += 1
        else:
            self.misses += 1
        return block

    def put(self, key, block) -> None:
        self._data[key] = block
        self._data.move_to_end(key)
        while len(self._data) > self.capacity:
            self._data.popitem(last=False)

    def drop_table(self, path: str) -> None:
        for k in [k for k in self._data if k[0] == path]:
            del self._data[k]


def write_sstable(path: str, items: Iterable[tuple[bytes, bytes | None]], expected_count: int) -> int:
    """Write sorted (key, value|None) pairs to `path` atomically. Returns entry count."""
    bloom = BloomFilter.for_capacity(expected_count)
    index: list[tuple[bytes, int]] = []
    tmp = path + ".tmp"
    count = 0
    offset = 0
    last_key = None
    with open(tmp, "wb", buffering=1 << 20) as f:
        for key, value in items:
            if last_key is not None and key <= last_key:
                raise ValueError("sstable keys must be strictly increasing")
            last_key = key
            if count % INDEX_EVERY == 0:
                index.append((key, offset))
            bloom.add(key)
            rec = _ENTRY.pack(len(key), -1 if value is None else len(value)) + key + (value or b"")
            f.write(rec)
            offset += len(rec)
            count += 1
        index_offset = offset
        buf = bytearray(_U32.pack(len(index)))
        for key, off in index:
            buf += _U32.pack(len(key)) + key + _U64.pack(off)
        f.write(buf)
        bloom_offset = index_offset + len(buf)
        f.write(bloom.to_bytes())
        f.write(_FOOTER.pack(index_offset, bloom_offset, count, MAGIC))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return count


def _parse_block(block: bytes) -> list[tuple[bytes, bytes | None]]:
    out = []
    pos = 0
    n = len(block)
    while pos < n:
        klen, vlen = _ENTRY.unpack_from(block, pos)
        pos += _ENTRY.size
        key = block[pos : pos + klen]
        pos += klen
        if vlen < 0:
            out.append((key, None))
        else:
            out.append((key, block[pos : pos + vlen]))
            pos += vlen
    return out


class SSTable:
    def __init__(self, path: str, cache: BlockCache | None = None):
        self.path = path
        self.cache = cache or BlockCache()
        self._f = open(path, "rb")
        size = os.fstat(self._f.fileno()).st_size
        if size < _FOOTER.size:
            raise CorruptTableError(path)
        self._f.seek(size - _FOOTER.size)
        index_off, bloom_off, self.count, magic = _FOOTER.unpack(self._f.read(_FOOTER.size))
        if magic != MAGIC:
            raise CorruptTableError(f"bad magic in {path}")
        self._f.seek(index_off)
        meta = self._f.read(size - _FOOTER.size - index_off)
        (n,) = _U32.unpack_from(meta, 0)
        pos = 4
        self.index_keys: list[bytes] = []
        self.index_offsets: list[int] = []
        for _ in range(n):
            (klen,) = _U32.unpack_from(meta, pos)
            pos += 4
            self.index_keys.append(meta[pos : pos + klen])
            pos += klen
            (off,) = _U64.unpack_from(meta, pos)
            pos += 8
            self.index_offsets.append(off)
        self.bloom = BloomFilter.from_bytes(meta[bloom_off - index_off :])
        self.data_end = index_off
        self.size_bytes = size

    def _block(self, i: int) -> list[tuple[bytes, bytes | None]]:
        cache_key = (self.path, i)
        block = self.cache.get(cache_key)
        if block is None:
            start = self.index_offsets[i]
            end = self.index_offsets[i + 1] if i + 1 < len(self.index_offsets) else self.data_end
            self._f.seek(start)
            block = _parse_block(self._f.read(end - start))
            self.cache.put(cache_key, block)
        return block

    def get(self, key: bytes):
        """Return the value, None for a tombstone, or MISSING if the key is not here."""
        if not self.bloom.might_contain(key):
            return MISSING
        i = bisect_right(self.index_keys, key) - 1
        if i < 0:
            return MISSING
        for k, v in self._block(i):
            if k == key:
                return v
            if k > key:
                break
        return MISSING

    def iter_from(self, start: bytes | None = None) -> Iterator[tuple[bytes, bytes | None]]:
        if not self.index_keys:
            return
        i = 0 if start is None else max(0, bisect_right(self.index_keys, start) - 1)
        for b in range(i, len(self.index_keys)):
            for k, v in self._block(b):
                if start is None or k >= start:
                    yield k, v

    def close(self) -> None:
        self._f.close()
        self.cache.drop_table(self.path)
