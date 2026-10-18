"""A single Raft node as a pure, tick driven state machine.

The node never touches a clock or a socket. The cluster calls `tick()` once per
simulated time unit and `receive(msg)` for each delivered message; the node
returns outgoing messages through `self.outbox`. That makes every run exactly
reproducible from a seed.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: tuple  # ("set", key, value) or ("del", key) or ("noop",)


class Node:
    def __init__(self, nid, peers, rng, election_range=(15, 30), heartbeat=5):
        self.id = nid
        self.peers = [p for p in peers if p != nid]
        self.rng = rng
        self.election_range = election_range
        self.heartbeat = heartbeat
        # persistent state (survives crash)
        self.term = 0
        self.voted_for = None
        self.log = []  # index i in list is Raft index i+1
        # volatile state (lost on crash)
        self._reset_volatile()

    # ---------------------------------------------------------------- state
    def _reset_volatile(self):
        self.state = FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.kv = {}
        self.leader_id = None
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.outbox = []
        self.hb_timer = 0
        self._reset_election_timer()

    def crash_restart(self):
        """Simulate power loss: keep term, vote and log; lose everything else."""
        self._reset_volatile()

    def _reset_election_timer(self):
        self.election_timer = self.rng.randint(*self.election_range)

    @property
    def quorum(self):
        return (len(self.peers) + 1) // 2 + 1

    def last_index(self):
        return len(self.log)

    def last_term(self):
        return self.log[-1].term if self.log else 0

    def term_at(self, index):
        return self.log[index - 1].term if index >= 1 else 0

    def _send(self, to, **msg):
        msg.update(src=self.id, dst=to)
        self.outbox.append(msg)

    # ---------------------------------------------------------------- timers
    def tick(self):
        if self.state == LEADER:
            self.hb_timer -= 1
            if self.hb_timer <= 0:
                self._broadcast_append()
            return
        self.election_timer -= 1
        if self.election_timer <= 0:
            self._start_election()

    def _start_election(self):
        self.state = CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes = {self.id}
        self.leader_id = None
        self._reset_election_timer()
        if len(self.votes) >= self.quorum:  # single node cluster
            self._become_leader()
            return
        for p in self.peers:
            self._send(p, type="RequestVote", term=self.term,
                       last_index=self.last_index(), last_term=self.last_term())

    def _become_leader(self):
        self.state = LEADER
        self.leader_id = self.id
        self.next_index = {p: self.last_index() + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # A no-op in the new term lets us commit earlier terms' entries (section 5.4.2).
        self.log.append(LogEntry(self.term, ("noop",)))
        self._broadcast_append()
        self._advance_commit()

    def _step_down(self, term):
        if term > self.term:
            self.term = term
            self.voted_for = None
        if self.state != FOLLOWER:
            self.state = FOLLOWER
        self._reset_election_timer()

    # ---------------------------------------------------------------- client
    def propose(self, command):
        """Append a client command. Returns its log index, or None if not leader."""
        if self.state != LEADER:
            return None
        self.log.append(LogEntry(self.term, tuple(command)))
        self._broadcast_append()
        self._advance_commit()
        return self.last_index()

    # ------------------------------------------------------------- messages
    def receive(self, m):
        if m["term"] > self.term:
            self._step_down(m["term"])
        handler = getattr(self, "_on_" + m["type"])
        handler(m)

    def _on_RequestVote(self, m):
        up_to_date = (m["last_term"], m["last_index"]) >= (self.last_term(), self.last_index())
        grant = (m["term"] == self.term and up_to_date
                 and self.voted_for in (None, m["src"]))
        if grant:
            self.voted_for = m["src"]
            self._reset_election_timer()
        self._send(m["src"], type="VoteReply", term=self.term, granted=grant)

    def _on_VoteReply(self, m):
        if self.state != CANDIDATE or m["term"] != self.term or not m["granted"]:
            return
        self.votes.add(m["src"])
        if len(self.votes) >= self.quorum:
            self._become_leader()

    def _broadcast_append(self):
        self.hb_timer = self.heartbeat
        for p in self.peers:
            self._send_append(p)

    def _send_append(self, p):
        ni = self.next_index[p]
        prev = ni - 1
        self._send(p, type="AppendEntries", term=self.term, prev_index=prev,
                   prev_term=self.term_at(prev), entries=tuple(self.log[prev:]),
                   commit=self.commit_index)

    def _on_AppendEntries(self, m):
        if m["term"] < self.term:
            self._send(m["src"], type="AppendReply", term=self.term, success=False, match=0, hint=0)
            return
        # a valid leader for this term exists
        self.state = FOLLOWER
        self.leader_id = m["src"]
        self._reset_election_timer()
        prev = m["prev_index"]
        if prev > self.last_index() or self.term_at(prev) != m["prev_term"]:
            # fast backup: suggest the first index of the conflicting term
            hint = min(prev, self.last_index() + 1)
            if hint <= self.last_index():
                bad = self.term_at(hint)
                while hint > 1 and self.term_at(hint - 1) == bad:
                    hint -= 1
            self._send(m["src"], type="AppendReply", term=self.term, success=False,
                       match=0, hint=max(1, hint))
            return
        idx = prev
        for e in m["entries"]:
            idx += 1
            if idx <= self.last_index():
                if self.log[idx - 1].term == e.term:
                    continue
                del self.log[idx - 1:]  # conflict: truncate suffix
            self.log.append(e)
        match = prev + len(m["entries"])
        if m["commit"] > self.commit_index:
            self.commit_index = min(m["commit"], match)
            self._apply()
        self._send(m["src"], type="AppendReply", term=self.term, success=True, match=match, hint=0)

    def _on_AppendReply(self, m):
        if self.state != LEADER or m["term"] != self.term:
            return
        p = m["src"]
        if m["success"]:
            self.match_index[p] = max(self.match_index[p], m["match"])
            self.next_index[p] = self.match_index[p] + 1
            self._advance_commit()
        else:
            self.next_index[p] = max(1, min(m["hint"], self.next_index[p] - 1))
            self._send_append(p)

    # --------------------------------------------------------- commit/apply
    def _advance_commit(self):
        for n in range(self.last_index(), self.commit_index, -1):
            if self.term_at(n) != self.term:
                break  # only count replicas for entries of the current term
            count = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
            if count >= self.quorum:
                self.commit_index = n
                break
        self._apply()

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            cmd = self.log[self.last_applied - 1].command
            if cmd[0] == "set":
                self.kv[cmd[1]] = cmd[2]
            elif cmd[0] == "del":
                self.kv.pop(cmd[1], None)
