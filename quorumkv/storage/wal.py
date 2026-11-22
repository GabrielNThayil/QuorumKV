"""Write-ahead log for the LSM memtable.

Record layout (little endian):
    crc32(payload) u32 | len(payload) u32 | payload
    payload = op u8 | key_len u32 | key | value

A crash can leave a torn record at the tail. Replay stops at the first record
whose length or checksum is wrong and truncates the file there, so the log is
always a clean prefix of what was written.
"""
from __future__ import annotations

import os
import struct
import zlib
from typing import Iterator

OP_PUT = 1
OP_DELETE = 2

_REC = struct.Struct("<II")
_PAYLOAD = struct.Struct("<BI")


class WriteAheadLog:
    def __init__(self, path: str, fsync: bool = False):
        self.path = path
        self.fsync = fsync
        self._f = open(path, "ab")

    def append(self, op: int, key: bytes, value: bytes | None) -> None:
        payload = _PAYLOAD.pack(op, len(key)) + key + (value or b"")
        self._f.write(_REC.pack(zlib.crc32(payload), len(payload)) + payload)
        # Flushing to the OS survives a process crash; fsync also survives power loss.
        self._f.flush()
        if self.fsync:
            os.fsync(self._f.fileno())

    def sync(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        if not self._f.closed:
            self._f.flush()
            self._f.close()

    @staticmethod
    def replay(path: str) -> Iterator[tuple[int, bytes, bytes | None]]:
        """Yield (op, key, value) for every intact record, truncating a torn tail."""
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            data = f.read()
        pos = 0
        good_end = 0
        n = len(data)
        while pos + _REC.size <= n:
            crc, length = _REC.unpack_from(data, pos)
            start = pos + _REC.size
            end = start + length
            if end > n or length < _PAYLOAD.size:
                break
            payload = data[start:end]
            if zlib.crc32(payload) != crc:
                break
            op, klen = _PAYLOAD.unpack_from(payload)
            key = payload[_PAYLOAD.size : _PAYLOAD.size + klen]
            value = payload[_PAYLOAD.size + klen :] if op == OP_PUT else None
            yield op, bytes(key), (bytes(value) if value is not None else None)
            pos = end
            good_end = end
        if good_end != n:
            with open(path, "r+b") as f:
                f.truncate(good_end)
