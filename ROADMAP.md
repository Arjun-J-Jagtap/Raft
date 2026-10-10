# Roadmap

Every task names the code to write and the test that proves it works. All protocol code goes in
`raft/raft.go` (split it into `election.go`, `replication.go` etc. whenever you like; it's one
package). The tests already exist and currently fail; making them pass *is* the work.

**A phase is done when its tests pass 200 runs in a row under the race detector:**

```sh
scripts/stress.sh -n 200 '<pattern>'
```

One green run proves little. Most Raft bugs are interleavings that show up once in a hundred runs.

---

## Phase 0: Foundation ✅

Network simulator, persister, RPC codec, cluster harness with a safety checker, all test suites for
phases 1–3, stress script, CI.

---

## Phase 1: Understand before coding (~3h)

- [ ] **1.1** Read the [extended Raft paper](https://raft.github.io/raft.pdf), sections 1–5 and 8.
      Skip 6 (membership) and 7 (compaction) for now.
- [ ] **1.2** Read Figure 2 until you can write it from memory. It is the spec: every rule in it is
      mandatory, and most bugs come from "simplifying" one.
- [ ] **1.3** Read Figure 8 and its caption until you can explain *why* a leader may not commit an
      earlier-term entry by counting replicas.
- [ ] **1.4** Read [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
      (prose only, no code). It lists the classic mistakes.
- [ ] **1.5** Skim `raft/cluster_test.go`: know what `one`, `checkOneLeader`, `wait`, `crash`,
      `restart`, `connect` and `disconnect` do, since every test is built from them.
- [ ] **1.6** Run `go test -run TestElectionInitial ./raft/` and watch it fail with
      "expected one leader, got none". That's the starting line.

---

## Phase 2: Leader election (§5.2), ~4h

- [ ] **2.1 Roles.** Add a role field (follower / candidate / leader). Make `State()` return
      `isLeader` correctly.
- [ ] **2.2 The term rule.** Write one helper, e.g. `becomeFollower(term)`: set `currentTerm`,
      clear `votedFor`, become follower. Call it whenever *any* RPC or reply carries a higher term.
- [ ] **2.3 Election timer.** Store when you last heard from a valid leader, plus a randomized
      timeout (e.g. 300–600ms). Start a `ticker` goroutine in `Make` that checks it every ~10ms and
      exits when `killed()`.
- [ ] **2.4 Become a candidate.** On timeout: `currentTerm++`, vote for yourself, reset the timer,
      and send `RequestVote` to every peer *in its own goroutine*. Never hold `rf.mu` while calling
      `sendRequestVote`.
- [ ] **2.5 Count votes.** In each reply goroutine: apply the term rule; ignore the reply unless you
      are *still a candidate in the same term*; on a majority, become leader.
- [ ] **2.6 RequestVote handler.** Grant if the term is current, you haven't voted for someone else
      this term, and the candidate's log is at least as up to date (§5.4.1: compare last *term*
      first, then last *index*). Reset your election timer only when you grant.
- [ ] **2.7 Heartbeats.** A leader sends empty `AppendEntries` to all peers about every 100ms. In the
      handler, for now: reject an old term, otherwise reset the timer and stay or become follower.
- [ ] **2.8 Traces.** Add `rf.dlog(...)` on every role change. Run with `RAFT_DEBUG=1`.

**Tests:** `TestElectionInitial`, `TestElectionReElect`, `TestElectionMany`,
`TestElectionPartitionedLeaderStepsDown`
**Gate:** `scripts/stress.sh -n 200 Election`

Check yourself: Why randomized timeouts? Why can't two leaders exist in one term? What happens if
you reset the timer on *every* incoming RPC?

---

## Phase 3: Log replication (§5.3, §5.4), ~6h

- [ ] **3.1 Leader state.** On winning, set `nextIndex[i] = len(log)` and `matchIndex[i] = 0` for
      every peer.
- [ ] **3.2 Start.** If leader: append `{Term: currentTerm, Command}`, return its index, and trigger
      replication.
- [ ] **3.3 Send entries.** Heartbeats become real: for each peer send `PrevLogIndex =
      nextIndex-1`, its term, and every entry from `nextIndex` on. On success, set
      `matchIndex = PrevLogIndex + len(Entries)` and `nextIndex = matchIndex + 1`.
- [ ] **3.4 Consistency check.** In the handler, reply `Success=false` if you don't have an entry at
      `PrevLogIndex` with term `PrevLogTerm`.
- [ ] **3.5 Append carefully.** Walk the new entries. Truncate your log *only at the first entry
      whose term conflicts*, then append the rest. (A delayed, shorter `AppendEntries` must never
      erase entries you already have.)
- [ ] **3.6 Follower commit.** If `LeaderCommit > commitIndex`, set
      `commitIndex = min(LeaderCommit, index of last new entry)`.
- [ ] **3.7 Applier.** One goroutine that sends `ApplyMsg`s for `lastApplied+1 .. commitIndex` in
      order. Use a `sync.Cond` or channel to wake it, and don't hold `rf.mu` while sending on
      `applyCh`.
      → `TestReplicationBasic`, `TestReplicationFollowerFailure`, `TestReplicationNoQuorumNoCommit`
- [ ] **3.8 Leader commit, with the previous-term rule.** Advance `commitIndex` to N if a majority
      has `matchIndex >= N` **and `log[N].Term == currentTerm`**.
      → `TestReplicationPrevTermCommitRule` (fails if you skip the term check)
- [ ] **3.9 Stale replies.** Ignore a reply if your term changed since you sent the request. Never
      move `nextIndex`/`matchIndex` backwards on a success.
      → `TestReplicationConcurrentStarts`, `TestReplicationRejoin`
- [ ] **3.10 Fast back-off.** On a mismatch, the follower fills in `ConflictTerm`/`ConflictIndex`
      (see the comment on `AppendEntriesReply`); the leader jumps `nextIndex` back a whole term at
      a time.
      → `TestReplicationBackup`
- [ ] **3.11 Efficiency.** Don't flood: one in-flight replication per peer, heartbeats ~10/s.
      → `TestReplicationRPCCount`, `TestReplicationRPCBytes`

**Gate:** `scripts/stress.sh -n 200 'Election|Replication'`

Check yourself: Walk through Figure 8 with your own code. Why does `nextIndex` start at the end of
the leader's log and not at 1? Why is `commitIndex` not persisted?

---

## Phase 4: Crash recovery (§5.1), ~3h

- [ ] **4.1 persist().** gob-encode `currentTerm`, `votedFor`, `log`;
      `rf.persister.Save(state, rf.persister.ReadSnapshot())`.
- [ ] **4.2 readPersist().** Decode the same three fields, in the same order.
- [ ] **4.3 Call persist() everywhere they change**: term bump, vote, append, truncate. Do it
      *before* replying to or sending any RPC that depends on the change.
- [ ] **4.4** Expect `TestPersistFigure8Unreliable` to expose replication bugs that phase 3
      missed. Fix them there, not with timing hacks.

**Tests:** `TestPersistRestartAll`, `TestPersistRepeatedCrashes`,
`TestPersistLeaderAndFollowerCrash`, `TestPersistFigure8`, `TestPersistFigure8Unreliable`
**Gate:** `scripts/stress.sh -n 200 'Election|Replication|Persist'`

Check yourself: What breaks if `votedFor` isn't persisted? What if `persist` runs *after* the reply
is sent?

---

## Phase 5: Snapshots (§7), ~6h

- [ ] **5.1 Read §7** and the `InstallSnapshotArgs` comments in `types.go`.
- [ ] **5.2 Index helpers first.** The log will start at `lastIncludedIndex`, not 0. Route *all*
      index math through two helpers (e.g. `entry(i)`, `lastIndex()`) and rerun phases 2–4 before
      writing any snapshot logic.
- [ ] **5.3 Snapshot(index, data).** Drop entries through `index`, remember `lastIncludedIndex/Term`,
      save state and snapshot together in one `Persister.Save`.
- [ ] **5.4 Leader side.** If `nextIndex[p] <= lastIncludedIndex`, send `InstallSnapshot`
      instead of `AppendEntries`.
- [ ] **5.5 Follower side.** Install a newer snapshot, keep any log suffix after it, deliver
      `ApplyMsg{SnapshotValid: true}` through the applier.
- [ ] **5.6 Restart.** On boot with a snapshot, `commitIndex = lastApplied = lastIncludedIndex`.
- [ ] **5.7 Write the tests** (`raft/snapshot_test.go`). Extend the harness applier to call
      `rf.Snapshot` every 10 entries with the gob-encoded applied commands, and to restore
      `logs[i]` from a `SnapshotValid` message. Scenarios: basic, install to a disconnected
      follower, crash during install, unreliable network with crashes.

**Gate:** `scripts/stress.sh -n 200 .`

---

## Phase 6: Key/value store on Raft, ~8h

- [ ] **6.1** `kv/` package: a server applying `Get`/`Put`/`Append` from `applyCh` to a map.
- [ ] **6.2** A client that retries other servers until it finds the leader.
- [ ] **6.3** Duplicate detection: (client ID, sequence number) so a retried `Append` applies once.
- [ ] **6.4** Wait for the op's own index to apply, and detect losing leadership while waiting.
- [ ] **6.5** Linearizability tests using [Porcupine](https://github.com/anishathalye/porcupine).
- [ ] **6.6** Snapshot the KV map when `persister.RaftStateSize()` passes a threshold.

## Phase 7: Real network and numbers, ~6h

- [ ] **7.1** TCP transport implementing `Transport` (length-prefixed frames over a connection
      pool).
- [ ] **7.2** `cmd/raftkv`: run a 3-node cluster locally from the command line.
- [ ] **7.3** Benchmarks: leader failover time (p50/p99), committed ops/sec at 3 vs 5 nodes, the
      effect of batching.
- [ ] **7.4** Results table in the README.
- [ ] **7.5** Write-up: one bug the stress runs found, what you misunderstood, how you fixed it.

---

## After each phase

1. Update the Status table in `README.md`.
2. Add the phase's tests to `.github/workflows/ci.yml`, e.g. `-run 'Unit|Election'`.
3. Commit with a message saying what passed and how many stress runs.

## Notes kept while building

- Things that surprised me:
- Bugs found only by stress runs (test, frequency, root cause):
- Design decisions and their trade-offs:
