# raftlab

A deterministic Raft consensus simulator in pure Python, with fault injection and safety invariants checked on every single tick. Zero dependencies.

## Why

Raft is easy to read and notoriously easy to implement wrongly. The bugs live in the corners: a leader committing an entry from an old term, a follower keeping a conflicting suffix, a vote granted to a stale candidate. Real clocks and sockets make those corners nearly impossible to reproduce. raftlab removes both: every node is a pure tick driven state machine, the network is a seeded priority queue, and every run is replayable from one integer.

## What is implemented

* Leader election with randomized timeouts, persistent term and vote, and the up to date log check
* Log replication with consistency checks, suffix truncation on conflict, and fast backup using a conflict term hint
* The section 5.4.2 rule: a leader only counts replicas for entries of its own term, and appends a no op on election so older entries commit safely
* A replicated key value state machine (`set`, `del`) applied in log order
* Crash and restart (term, vote and log survive; commit index and state machine are rebuilt)
* Network faults: random message drops, variable delay, arbitrary partitions
* Invariant checker run after every tick: election safety (one leader per term), log matching, and committed entries never change

## Run it

```
python -m raftlab --nodes 5 --seed 1 --chaos
```

Sample output:

```
ticks=3002 proposals_accepted=290 messages=5918 delivered=3729
terms_seen=61 leader=1
committed_entries=251 converged=True
invariants: election safety, log matching, committed entries immutable: OK
```

Options: `--nodes`, `--seed`, `--ticks`, `--drop` (message loss probability) and `--chaos` (random crashes and partitions).

As a library:

```python
from raftlab import Cluster

c = Cluster(5, seed=42, drop=0.05)
c.run_until(lambda: c.leader() is not None)
c.propose(("set", "x", 1))
c.run(100)
print(c.leader().kv)   # {'x': 1}
```

## Tests

```
python -m unittest discover -s tests
```

The suite covers election, replication, leader crash with re election, a minority partition that must not commit (and whose uncommitted write must be overwritten after healing), repair of a follower that missed 30 entries, a test that proves the checker really catches split brain, and a 25 seed chaos fuzz with crashes, partitions and 10 percent message loss that must always converge.

## Design notes

* `node.py` has no I/O. Nodes push messages into an outbox, which makes the protocol logic easy to read next to the paper.
* `cluster.py` owns time, the network and the invariants. Separate seeded RNGs for nodes and network mean changing the drop rate does not reshuffle election timeouts.
* Invariants are checked from outside the nodes, so a protocol bug cannot hide itself.
* Out of scope on purpose: membership changes and snapshots. The goal is a compact, verifiable core.
