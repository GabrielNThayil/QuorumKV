"""Bloom filter used by SSTables to skip disk reads for keys that are absent.

Uses Kirsch-Mitzenmacher double hashing: two 64-bit hashes derived from one
BLAKE2b digest generate all k probe positions.
"""
from __future__ import annotations

import hashlib
import math
import struct


class BloomFilter:
    __slots__ = ("num_bits", "num_hashes", "bits")

    def __init__(self, num_bits: int, num_hashes: int, bits: bytes | None = None):
        self.num_bits = max(8, num_bits)
        self.num_hashes = max(1, num_hashes)
        size = (self.num_bits + 7) // 8
        self.bits = bytearray(bits) if bits is not None else bytearray(size)
        if len(self.bits) != size:
            raise ValueError("bloom filter bit array has the wrong length")

    @classmethod
    def for_capacity(cls, expected_items: int, fp_rate: float = 0.01) -> "BloomFilter":
        n = max(1, expected_items)
        m = math.ceil(-n * math.log(fp_rate) / (math.log(2) ** 2))
        k = max(1, round(m / n * math.log(2)))
        return cls(m, k)

    def _positions(self, key: bytes):
        h1, h2 = struct.unpack("<QQ", hashlib.blake2b(key, digest_size=16).digest())
        h2 |= 1  # odd step so probes cover the whole table
        m = self.num_bits
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % m

    def add(self, key: bytes) -> None:
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)

    def might_contain(self, key: bytes) -> bool:
        bits = self.bits
        return all(bits[p >> 3] & (1 << (p & 7)) for p in self._positions(key))

    def to_bytes(self) -> bytes:
        return struct.pack("<II", self.num_bits, self.num_hashes) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        m, k = struct.unpack_from("<II", data)
        return cls(m, k, data[8 : 8 + (m + 7) // 8])
