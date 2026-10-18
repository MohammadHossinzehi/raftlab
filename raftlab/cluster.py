"""Deterministic network + fault injection + safety invariant checks."""
from __future__ import annotations

import heapq
import random

from .node import Node, LEADER


class InvariantViolation(AssertionError):
    pass


class Cluster:
    def __init__(self, n=5, seed=0, drop=0.0, delay=(1, 3), **node_kw):
        self.rng = random.Random(seed)
        self.net_rng = random.Random(seed * 7919 + 1)
        ids = list(range(n))
        self.nodes = {i: Node(i, ids, random.Random(seed * 31 + i), **node_kw) for i in ids}
        self.time = 0
        self.drop = drop
        self.delay = delay
        self.queue = []  # (deliver_at, seq, msg)
        self.seq = 0
        self.down = set()
        self.groups = None  # list of sets when partitioned
        self.leaders_by_term = {}
        self.committed = {}  # index -> LogEntry, must never change
        self.sent = 0
        self.delivered = 0

    # ------------------------------------------------------------- faults
    def crash(self, i):
        self.down.add(i)

    def restart(self, i):
        if i in self.down:
            self.down.discard(i)
            self.nodes[i].crash_restart()

    def partition(self, *groups):
        self.groups = [set(g) for g in groups]

    def heal(self):
        self.groups = None

    def _connected(self, a, b):
        if self.groups is None:
            return True
        return any(a in g and b in g for g in self.groups)

    # --------------------------------------------------------------- run
    def step(self):
        self.time += 1
        while self.queue and self.queue[0][0] <= self.time:
            _, _, m = heapq.heappop(self.queue)
            if m["dst"] in self.down or not self._connected(m["src"], m["dst"]):
                continue
            self.delivered += 1
            self.nodes[m["dst"]].receive(m)
            self._flush(self.nodes[m["dst"]])
        for i, node in self.nodes.items():
            if i in self.down:
                continue
            node.tick()
            self._flush(node)
        self._check()

    def _flush(self, node):
        for m in node.outbox:
            self.sent += 1
            if self.net_rng.random() < self.drop:
                continue
            at = self.time + self.net_rng.randint(*self.delay)
            self.seq += 1
            heapq.heappush(self.queue, (at, self.seq, m))
        node.outbox = []

    def run(self, ticks):
        for _ in range(ticks):
            self.step()

    def run_until(self, pred, limit=2000):
        for _ in range(limit):
            if pred():
                return True
            self.step()
        return pred()

    # ----------------------------------------------------------- queries
    def leader(self):
        """The live leader with the highest term, if any."""
        ls = [n for i, n in self.nodes.items() if i not in self.down and n.state == LEADER]
        return max(ls, key=lambda n: n.term, default=None)

    def propose(self, command):
        ld = self.leader()
        return None if ld is None else (ld.id, ld.propose(command))

    # -------------------------------------------------------- invariants
    def _check(self):
        for i, n in self.nodes.items():
            if n.state == LEADER:
                prev = self.leaders_by_term.setdefault(n.term, i)
                if prev != i:
                    raise InvariantViolation(f"two leaders in term {n.term}: {prev} and {i}")
        for i, n in self.nodes.items():
            for idx in range(1, n.commit_index + 1):
                e = n.log[idx - 1]
                old = self.committed.setdefault(idx, e)
                if old != e:
                    raise InvariantViolation(f"committed entry {idx} changed on node {i}")
        # log matching: same (index, term) implies identical prefix
        ns = list(self.nodes.values())
        for a in range(len(ns)):
            for b in range(a + 1, len(ns)):
                la, lb = ns[a].log, ns[b].log
                for k in range(min(len(la), len(lb)) - 1, -1, -1):
                    if la[k].term == lb[k].term:
                        if la[: k + 1] != lb[: k + 1]:
                            raise InvariantViolation("log matching violated")
                        break

    def converged(self):
        live = [n for i, n in self.nodes.items() if i not in self.down]
        return len({tuple(n.log) for n in live}) == 1 and len({n.commit_index for n in live}) == 1
