import os
import random

from quorumkv.raft.storage import FileRaftStorage
from quorumkv.storage.bloom import BloomFilter
from quorumkv.storage.lsm import LSMEngine
from quorumkv.storage.sstable import MISSING, SSTable, write_sstable


def test_bloom_filter_has_no_false_negatives_and_low_fp_rate():
    bf = BloomFilter.for_capacity(10_000, fp_rate=0.01)
    for i in range(10_000):
        bf.add(f"key-{i}".encode())
    assert all(bf.might_contain(f"key-{i}".encode()) for i in range(10_000))
    fps = sum(bf.might_contain(f"absent-{i}".encode()) for i in range(20_000))
    assert fps / 20_000 < 0.02
    clone = BloomFilter.from_bytes(bf.to_bytes())
    assert clone.might_contain(b"key-42")


def test_sstable_roundtrip_point_lookups_and_iteration(tmp_path):
    items = [(f"k{i:05d}".encode(), None if i % 7 == 0 else f"v{i}".encode()) for i in range(1000)]
    path = str(tmp_path / "t.sst")
    write_sstable(path, items, len(items))
    t = SSTable(path)
    assert t.count == 1000
    for k, v in items:
        assert t.get(k) == v
    assert t.get(b"k99999") is MISSING
    assert t.get(b"a") is MISSING
    assert list(t.iter_from(b"k00990")) == items[990:]
    t.close()


def test_lsm_reads_across_memtable_and_sstables(tmp_path):
    db = LSMEngine(str(tmp_path), memtable_limit=2_000, compaction_trigger=100)
    for i in range(500):
        db.put(f"k{i}".encode(), f"v{i}".encode())
    assert len(db.tables) > 3  # several flushes happened
    db.put(b"k10", b"new")
    db.delete(b"k11")
    assert db.get(b"k10") == b"new"
    assert db.get(b"k11") is None
    assert db.get(b"k499") == b"v499"
    # k0 lives in the oldest table: newer tables are skipped by their bloom filters.
    assert db.get(b"k0") == b"v0"
    assert db.stats["bloom_negatives"] >= len(db.tables) - 2
    db.close()


def test_lsm_compaction_merges_and_drops_tombstones(tmp_path):
    db = LSMEngine(str(tmp_path), memtable_limit=1_000, compaction_trigger=4)
    for i in range(400):
        db.put(f"k{i:03d}".encode(), b"x" * 20)
    for i in range(0, 400, 2):
        db.delete(f"k{i:03d}".encode())
    db.flush()
    db.compact()
    assert len(db.tables) == 1
    assert db.tables[0].count == 200  # tombstones gone
    assert db.get(b"k000") is None and db.get(b"k001") == b"x" * 20
    assert db.stats["compactions"] >= 1
    db.close()


def test_lsm_recovers_from_crash_via_wal(tmp_path):
    db = LSMEngine(str(tmp_path), memtable_limit=1 << 20)
    db.put(b"a", b"1")
    db.put(b"b", b"2")
    db.delete(b"a")
    db.wal._f.flush()
    # "Crash": never call close(); open a fresh engine on the same directory.
    db2 = LSMEngine(str(tmp_path))
    assert db2.get(b"a") is None and db2.get(b"b") == b"2"
    db2.close()
    db.wal.close()


def test_lsm_ignores_torn_wal_tail(tmp_path):
    db = LSMEngine(str(tmp_path))
    db.put(b"good", b"yes")
    db.close()
    with open(os.path.join(str(tmp_path), "wal.log"), "ab") as f:
        f.write(b"\x12\x34\x56\x78\x40\x00\x00\x00partial-record")
    db2 = LSMEngine(str(tmp_path))
    assert db2.get(b"good") == b"yes"
    db2.put(b"after", b"ok")
    db2.close()
    db3 = LSMEngine(str(tmp_path))
    assert db3.get(b"after") == b"ok"
    db3.close()


def test_lsm_matches_dict_model_under_random_workload(tmp_path):
    rng = random.Random(42)
    db = LSMEngine(str(tmp_path), memtable_limit=4_000, compaction_trigger=3)
    model: dict[bytes, bytes] = {}
    for step in range(5000):
        k = f"key{rng.randint(0, 300):04d}".encode()
        if rng.random() < 0.7:
            v = f"val{step}".encode()
            db.put(k, v)
            model[k] = v
        else:
            db.delete(k)
            model.pop(k, None)
        if step % 500 == 0:
            db.close()
            db = LSMEngine(str(tmp_path), memtable_limit=4_000, compaction_trigger=3)
    for k in {f"key{i:04d}".encode() for i in range(301)}:
        assert db.get(k) == model.get(k)
    assert db.scan() == sorted(model.items())
    lo, hi = b"key0100", b"key0200"
    assert db.scan(lo, hi) == sorted((k, v) for k, v in model.items() if lo <= k < hi)
    assert db.stats["compactions"] > 0
    db.close()


def test_file_raft_storage_persists_and_truncates(tmp_path):
    s = FileRaftStorage(str(tmp_path))
    s.save_hard_state(3, "n2")
    s.append([[1, None], [1, {"op": "put"}], [2, {"op": "x"}], [3, {"op": "y"}]])
    s.sync()
    s.truncate_from(3)
    s.append([[3, {"op": "z"}]])
    s.close()
    s2 = FileRaftStorage(str(tmp_path))
    assert (s2.term, s2.voted_for) == (3, "n2")
    assert s2.entries == [[1, None], [1, {"op": "put"}], [3, {"op": "z"}]]
    s2.close()


def test_file_raft_storage_drops_torn_line(tmp_path):
    s = FileRaftStorage(str(tmp_path))
    s.append([[1, {"op": "a"}], [1, {"op": "b"}]])
    s.close()
    with open(os.path.join(str(tmp_path), "raft.log"), "ab") as f:
        f.write(b'deadbeef [1,{"op":"c"')  # crash mid-write: no newline, bad crc
    s2 = FileRaftStorage(str(tmp_path))
    assert s2.last_index() == 2
    s2.close()
