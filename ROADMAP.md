# Roadmap

Each phase ends with a test gate. A phase is done when its tests pass **200 runs in a row under
`-race`** (`scripts/stress.sh -n 200 '<pattern>'`), not when they pass once.

Estimates assume focused evenings. The debugging is the point; budget for it.

---

## Phase 0: Foundation ✅

- [x] `simnet`: in-memory RPC network with disconnects, partitions, crashes (incarnations),
      drops, jitter, long reordering, RPC/byte counters
- [x] `Persister` with crash-safe copy semantics
- [x] gob RPC codec and `Dispatch` (`Transport` interface, byte-level)
- [x] Cluster harness: continuous safety checker, `one`, `wait`, `checkOneLeader`, crash/restart
- [x] Test suites for election, replication, and persistence written up front (they define "done")
- [x] Stress script and CI

## Phase 1: Leader election (§5.2), ~4h

Files: `raft/raft.go` (`Make`, `State`, `RequestVote`, a ticker goroutine).

- [ ] Roles (follower / candidate / leader) and the term rule: any RPC with a higher term turns
      you into a follower of that term
- [ ] Ticker goroutine with a randomized election timeout (e.g. 300–600ms), reset on a valid
      heartbeat and on granting a vote, **not** on every incoming RPC
- [ ] Candidate: increment term, vote for self, send RequestVote in parallel, become leader on a
      majority *if still a candidate in the same term*
- [ ] RequestVote handler: one vote per term, granted only if the candidate's log is at least as
      up to date (§5.4.1: compare last term, then last index)
- [ ] Leader: empty AppendEntries heartbeats ~10/s
- [ ] `RAFT_DEBUG` traces for every role change

**Gate:** `stress.sh -n 200 Election`

Pitfalls: holding `rf.mu` across an RPC (deadlock); counting a reply from an old term; a timer that
fires repeatedly because it was never reset.

## Phase 2: Log replication (§5.3, §5.4), ~6h

Files: `raft/raft.go` (`Start`, `AppendEntries`, a per-peer replicator, an applier goroutine).

- [ ] `Start` appends to the leader's log and triggers replication
- [ ] AppendEntries handler: reject old terms; consistency check on `PrevLogIndex`/`PrevLogTerm`;
      truncate **only on an actual conflict** (a delayed, shorter AppendEntries must not erase
      entries); append; `commitIndex = min(LeaderCommit, index of last new entry)`
- [ ] Fast back-off: follower returns `ConflictTerm`/`ConflictIndex`; leader skips a whole term
      per round trip
- [ ] Leader advances `commitIndex` to N when a majority has `matchIndex >= N` **and
      `log[N].Term == currentTerm`**: the previous-term commit rule (§5.4.2, Figure 8)
- [ ] Applier goroutine delivers entries in order, never holding `rf.mu` while sending on
      `applyCh`
- [ ] Ignore stale replies (term changed, or `nextIndex` moved since the request was sent)

**Gate:** `stress.sh -n 200 'Election|Replication'`

## Phase 3: Crash recovery (§5.1), ~3h

- [ ] `persist`/`readPersist` with gob: `currentTerm`, `votedFor`, `log`
- [ ] Call `persist` after every change to those three, before replying or sending
- [ ] Re-verify Phase 2's back-off under `TestPersistFigure8Unreliable`. This is where latent
      replication bugs surface.

**Gate:** `stress.sh -n 200 'Election|Replication|Persist'`

## Phase 4: Log compaction (§7), ~6h

- [ ] Index translation: the log starts at `lastIncludedIndex`, not 0. Put all index arithmetic
      behind two helpers before anything else.
- [ ] `Snapshot(index, data)`: trim the log, `Persister.Save(state, snapshot)` atomically
- [ ] Leader sends `InstallSnapshot` when `nextIndex[p] <= lastIncludedIndex`
- [ ] Follower installs the snapshot, keeps any log suffix that follows it, and delivers
      `ApplyMsg{SnapshotValid: true}`
- [ ] Restart from a snapshot: `lastApplied = commitIndex = lastIncludedIndex`
- [ ] New tests: `snapshot_test.go` (basic, install to a lagging follower, crash during install,
      unreliable + crash churn). The harness applier already tracks `SnapshotValid`.

**Gate:** `stress.sh -n 200 .`

## Phase 5: Linearizable KV store, ~8h

- [ ] `kv/` package: `Get`/`Put`/`Append` on top of Raft, with a leader-redirecting client
- [ ] Duplicate detection by (client ID, sequence number) for at-most-once semantics
- [ ] Linearizability checking with [Porcupine](https://github.com/anishathalye/porcupine)
- [ ] Snapshots of the KV state triggered by `Persister.RaftStateSize()`

## Phase 6: Make it real, ~6h

- [ ] TCP transport implementing `Transport`; a `cmd/raftkv` binary running a 3-node cluster
      locally
- [ ] Benchmarks: leader failover time (p50/p99), committed ops/sec at 3 vs 5 nodes, effect of
      batching
- [ ] README: results table and a trace of a real failover
- [ ] Write-up: one bug the stress runs found, what was misunderstood, and the fix

---

## Notes kept while building

- Things that surprised me:
- Bugs found only by stress runs (test, frequency, root cause):
- Design decisions and their trade-offs:
