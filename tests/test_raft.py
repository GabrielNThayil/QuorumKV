import pytest

from quorumkv.raft.node import FOLLOWER, LEADER, NotLeader

from .sim import SimCluster


def put(k, v):
    return {"op": "put", "key": k, "value": v}


def settle(c: SimCluster, steps: int = 200):
    c.run(steps)
    c.check_all()


def test_elects_exactly_one_leader():
    c = SimCluster(3, seed=1)
    ldr = c.wait_for_leader()
    c.run(100)
    assert c.leader() == ldr
    assert sum(1 for n in c.nodes.values() if n.role == LEADER) == 1
    assert all(n.leader_id == ldr for n in c.nodes.values())
    c.check_all()


def test_single_node_cluster_commits_alone():
    c = SimCluster(1, seed=2)
    c.wait_for_leader()
    c.propose(put("a", "1"))
    c.run(5)
    assert c.applied["n1"][-1][1] == put("a", "1")


def test_replicates_to_all_nodes():
    c = SimCluster(5, seed=3)
    c.wait_for_leader()
    for i in range(50):
        c.propose(put(f"k{i}", str(i)))
    settle(c)
    for i in c.ids:
        cmds = [cmd for _, cmd in c.applied[i] if cmd is not None]
        assert cmds == [put(f"k{j}", str(j)) for j in range(50)]


def test_follower_rejects_proposals():
    c = SimCluster(3, seed=4)
    ldr = c.wait_for_leader()
    c.run(10)  # let the first heartbeat reach followers
    follower = next(i for i in c.ids if i != ldr)
    with pytest.raises(NotLeader) as e:
        c.nodes[follower].propose(put("x", "y"))
    assert e.value.leader_id == ldr


def test_leader_crash_preserves_committed_entries():
    c = SimCluster(3, seed=5)
    old = c.wait_for_leader()
    for i in range(20):
        c.propose(put(f"k{i}", "v"))
    c.run(50)
    c.crash(old)
    new = c.wait_for_leader()
    assert new != old
    c.propose(put("after", "crash"))
    c.run(50)
    c.restart(old)
    settle(c, 300)
    for i in c.ids:
        keys = [cmd["key"] for _, cmd in c.applied[i] if cmd]
        assert keys[:20] == [f"k{j}" for j in range(20)] and keys[-1] == "after"


def test_minority_leader_cannot_commit_and_log_converges_after_heal():
    c = SimCluster(5, seed=6)
    old = c.wait_for_leader()
    others = [i for i in c.ids if i != old]
    c.partition({old, others[0]}, set(others[1:]))
    # Writes to the stranded leader must never commit.
    stranded = c.nodes[old].propose(put("lost", "x"))
    c.nodes[old].replicate()
    c.run(150)
    assert ("lost" not in [cmd and cmd["key"] for _, cmd in c.applied[old]])
    new = c.wait_for_leader()
    assert new in others[1:]
    c.propose(put("won", "y"))
    c.run(50)
    c.heal()
    settle(c, 300)
    for i in c.ids:
        keys = [cmd["key"] for _, cmd in c.applied[i] if cmd]
        assert "won" in keys and "lost" not in keys
    assert stranded is not None


def test_check_quorum_isolated_leader_steps_down():
    c = SimCluster(3, seed=7)
    ldr = c.wait_for_leader()
    c.partition({ldr}, set(c.ids) - {ldr})
    c.run(60)
    # Stepped down (it may since have started a pre-vote, which cannot succeed alone).
    assert c.nodes[ldr].role != LEADER
    assert c.nodes[ldr].role in (FOLLOWER, "pre_candidate")


def test_prevote_isolated_follower_does_not_disrupt_cluster():
    c = SimCluster(3, seed=8)
    ldr = c.wait_for_leader()
    term = c.nodes[ldr].term
    follower = next(i for i in c.ids if i != ldr)
    c.partition({follower}, set(c.ids) - {follower})
    c.run(500)  # many election timeouts
    assert c.nodes[follower].term == term, "PreVote should stop term inflation"
    c.heal()
    c.run(100)
    assert c.leader() == ldr and c.nodes[ldr].term == term


def test_read_index_confirms_leadership():
    c = SimCluster(3, seed=9)
    ldr = c.wait_for_leader()
    c.run(20)
    c.nodes[ldr].read_index("r1")
    c.run(10)
    assert c.read_results.get("r1") is True


def test_read_index_on_partitioned_leader_never_succeeds():
    c = SimCluster(5, seed=10)
    old = c.wait_for_leader()
    c.run(20)
    c.partition({old}, set(c.ids) - {old})
    c.nodes[old].read_index("stale")
    c.run(5)
    assert c.read_results.get("stale") is not True
    c.wait_for_leader()
    c.propose(put("fresh", "1"))
    c.run(100)
    # Either still pending or failed on step-down, never served a stale value.
    assert c.read_results.get("stale") is not True


@pytest.mark.parametrize("seed", range(12))
def test_randomized_chaos_preserves_safety(seed):
    """Random crashes, restarts, partitions, drops, duplicates and reordering.

    Checks election safety, log matching, state machine safety, and that every
    entry ever observed as committed survives in the final state of every node."""
    c = SimCluster(5, seed=seed, drop_rate=0.05, dup_rate=0.05, max_delay=3)
    rng = c.rng
    for step in range(1500):
        r = rng.random()
        if r < 0.004:
            live = [i for i in c.ids if i not in c.down]
            if len(c.down) < 2:
                c.crash(rng.choice(live))
        elif r < 0.010 and c.down:
            c.restart(rng.choice(sorted(c.down)))
        elif r < 0.013:
            ids = c.ids[:]
            rng.shuffle(ids)
            cut = rng.randint(1, 4)
            c.partition(set(ids[:cut]), set(ids[cut:]))
        elif r < 0.020:
            c.heal()
        if rng.random() < 0.3:
            c.propose(put(f"s{seed}-{step}", str(step)))
        c.step()
        if step % 100 == 0:
            c.check_all()

    c.heal()
    for i in sorted(c.down):
        c.restart(i)
    c.wait_for_leader()
    c.propose(put("final", "barrier"))
    c.run(400)
    c.check_all()

    final = {i: dict((idx, cmd) for idx, cmd in c.applied[i]) for i in c.ids}
    for (idx, _term), cmd in c.committed_entries.items():
        for i in c.ids:
            assert final[i].get(idx) == cmd, f"committed entry {idx} lost or changed on {i}"
    # The cluster made real progress despite the chaos.
    assert len(c.committed_entries) > 100
