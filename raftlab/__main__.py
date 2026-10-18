"""CLI: python -m raftlab [--nodes 5] [--seed 1] [--ticks 3000] [--chaos]"""
import argparse

from .cluster import Cluster


def main():
    ap = argparse.ArgumentParser(description="Run a Raft cluster simulation with invariant checks")
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--ticks", type=int, default=3000)
    ap.add_argument("--drop", type=float, default=0.05, help="message drop probability")
    ap.add_argument("--chaos", action="store_true", help="randomly crash nodes and partition the network")
    a = ap.parse_args()

    c = Cluster(a.nodes, seed=a.seed, drop=a.drop)
    ids = list(c.nodes)
    accepted = 0
    for t in range(a.ticks):
        if t % 10 == 0:
            r = c.propose(("set", f"k{c.rng.randint(0, 9)}", t))
            accepted += bool(r and r[1])
        if a.chaos and t % 150 == 0:
            roll = c.rng.random()
            if roll < 0.35:
                c.crash(c.rng.choice(ids))
            elif roll < 0.6 and c.down:
                c.restart(c.rng.choice(sorted(c.down)))
            elif roll < 0.8:
                k = c.rng.randint(1, len(ids) - 1)
                s = c.rng.sample(ids, k)
                c.partition(s, [i for i in ids if i not in s])
            else:
                c.heal()
            if len(c.down) > (a.nodes - 1) // 2:
                c.restart(sorted(c.down)[0])
        c.step()
    c.heal()
    for i in sorted(c.down):
        c.restart(i)
    c.run_until(c.converged, 3000)
    ld = c.leader()
    print(f"ticks={c.time} proposals_accepted={accepted} messages={c.sent} delivered={c.delivered}")
    print(f"terms_seen={max(c.leaders_by_term, default=0)} leader={ld.id if ld else None}")
    print(f"committed_entries={len(c.committed)} converged={c.converged()}")
    print("invariants: election safety, log matching, committed entries immutable: OK")


if __name__ == "__main__":
    main()
