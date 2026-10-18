import unittest

from raftlab import Cluster, InvariantViolation
from raftlab.node import LEADER


class RaftTests(unittest.TestCase):
    def elect(self, c):
        self.assertTrue(c.run_until(lambda: c.leader() is not None))
        return c.leader()

    def test_elects_single_leader(self):
        c = Cluster(5, seed=3)
        ld = self.elect(c)
        c.run(100)
        self.assertEqual([n for n in c.nodes.values() if n.state == LEADER], [ld])

    def test_replication_and_state_machine(self):
        c = Cluster(3, seed=1)
        self.elect(c)
        c.propose(("set", "a", 1))
        c.propose(("set", "b", 2))
        c.propose(("del", "a"))
        c.run(100)
        for n in c.nodes.values():
            self.assertEqual(n.kv, {"b": 2})
        self.assertTrue(c.converged())

    def test_leader_crash_reelection_keeps_committed(self):
        c = Cluster(5, seed=5)
        ld = self.elect(c)
        c.propose(("set", "x", 1))
        c.run(60)
        c.crash(ld.id)
        new = None
        self.assertTrue(c.run_until(lambda: c.leader() is not None and c.leader().id != ld.id))
        new = c.leader()
        self.assertEqual(new.kv.get("x"), 1)
        c.propose(("set", "y", 2))
        c.run(80)
        c.restart(ld.id)
        c.run(200)
        self.assertEqual(c.nodes[ld.id].kv, {"x": 1, "y": 2})

    def test_minority_partition_cannot_commit(self):
        c = Cluster(5, seed=2)
        ld = self.elect(c)
        others = [i for i in c.nodes if i != ld.id]
        c.partition([ld.id, others[0]], others[1:])
        idx = ld.propose(("set", "lost", 1))
        c.run(300)
        self.assertLess(ld.commit_index, idx)  # old leader cannot reach quorum
        c.heal()
        c.run(400)
        self.assertTrue(c.converged())
        for n in c.nodes.values():
            self.assertNotIn("lost", n.kv)  # uncommitted write was overwritten

    def test_follower_log_repaired_after_long_gap(self):
        c = Cluster(3, seed=9)
        ld = self.elect(c)
        lag = [i for i in c.nodes if i != ld.id][0]
        c.crash(lag)
        for k in range(30):
            c.propose(("set", f"k{k}", k))
            c.run(5)
        c.restart(lag)
        c.run(300)
        self.assertEqual(c.nodes[lag].kv, c.leader().kv)

    def test_checker_detects_split_brain(self):
        c = Cluster(3, seed=0)
        self.elect(c)
        t = c.leader().term
        other = [n for n in c.nodes.values() if n.state != LEADER][0]
        other.state, other.term = LEADER, t
        with self.assertRaises(InvariantViolation):
            c._check()

    def test_chaos_fuzz_many_seeds(self):
        for seed in range(25):
            c = Cluster(5, seed=seed, drop=0.1)
            ids = list(c.nodes)
            for t in range(1500):
                if t % 7 == 0:
                    c.propose(("set", f"k{c.rng.randint(0, 5)}", t))
                if t % 120 == 0:
                    r = c.rng.random()
                    if r < 0.3 and len(c.down) < 2:
                        c.crash(c.rng.choice(ids))
                    elif r < 0.55 and c.down:
                        c.restart(sorted(c.down)[0])
                    elif r < 0.8:
                        s = c.rng.sample(ids, 2)
                        c.partition(s, [i for i in ids if i not in s])
                    else:
                        c.heal()
                c.step()  # invariants checked every tick
            c.heal()
            for i in sorted(c.down):
                c.restart(i)
            c.drop = 0.0
            self.assertTrue(c.run_until(c.converged, 3000), f"seed {seed} did not converge")


if __name__ == "__main__":
    unittest.main()
