// Package raft implements the Raft consensus protocol: a replicated log that
// a set of servers keep identical as long as a majority of them are up and
// can talk to each other.
//
// A service built on Raft calls Start to propose a command, and receives
// every committed command, in the same order on every server, on its apply
// channel.
package raft

import (
	"sync"
	"sync/atomic"
)

// Raft is one server's view of the replicated log.
type Raft struct {
	mu        sync.Mutex
	me        int // this server's index in 0..n-1
	n         int // cluster size
	transport Transport
	persister *Persister
	applyCh   chan<- ApplyMsg
	dead      atomic.Bool

	// Persistent state on all servers (Figure 2). Must reach stable
	// storage before replying to any RPC that observed or changed it.
	currentTerm int
	votedFor    int        // candidate voted for in currentTerm, or -1
	log         []LogEntry // log[0] is a sentinel so real entries start at index 1

	// Volatile state on all servers.
	commitIndex int // highest index known to be committed
	lastApplied int // highest index sent on applyCh

	// Volatile state on leaders, reinitialized after each election.
	nextIndex  []int // per peer: next log index to send
	matchIndex []int // per peer: highest index known to be replicated
}

// Make creates server `me` of an n-server cluster and returns immediately;
// any long-running work happens in goroutines.
//
// transport reaches the other servers, persister holds this server's stable
// state (and any state it saved before a crash), and every committed entry is
// delivered on applyCh in log order.
func Make(me, n int, transport Transport, persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{
		me:         me,
		n:          n,
		transport:  transport,
		persister:  persister,
		applyCh:    applyCh,
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}},
		nextIndex:  make([]int, n),
		matchIndex: make([]int, n),
	}
	rf.readPersist(persister.ReadRaftState())

	// TODO(election): start the ticker that turns a follower into a
	// candidate when it hears nothing from a leader for a randomized
	// election timeout.
	// TODO(replication): start the goroutine that delivers committed
	// entries on applyCh.
	return rf
}

// State returns the current term and whether this server believes it is the
// leader.
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO(election): track this server's role.
	return rf.currentTerm, false
}

// Start proposes command for the replicated log. If this server is not the
// leader it returns isLeader=false. Otherwise it appends the command and
// returns immediately with the index the command will occupy if it is ever
// committed. Start does not wait for, or promise, commitment: the leader may
// lose its term first.
func (rf *Raft) Start(command any) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO(replication): append to the leader's log and begin replicating.
	return -1, rf.currentTerm, false
}

// Snapshot tells Raft that the service has captured all state up to and
// including index in snapshot, so log entries through index can be
// discarded.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// TODO(snapshots): trim the log and persist the snapshot with the state.
}

// Kill stops the server. Goroutines should check killed and exit, so a
// stopped server stops sending RPCs and applying entries.
func (rf *Raft) Kill() {
	rf.dead.Store(true)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// RequestVote handles a candidate's vote request (§5.2, §5.4.1).
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO(election): grant at most one vote per term, and only to a
	// candidate whose log is at least as up to date as ours.
	reply.Term = rf.currentTerm
}

// AppendEntries handles log replication and heartbeats from the leader
// (§5.3).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO(replication): reject stale terms, check PrevLogIndex/PrevLogTerm,
	// truncate conflicting entries, append, and advance commitIndex.
	reply.Term = rf.currentTerm
}

// InstallSnapshot replaces a far-behind follower's log prefix with the
// leader's snapshot (§7).
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO(snapshots): install the snapshot and deliver it on applyCh.
	reply.Term = rf.currentTerm
}

// persist saves currentTerm, votedFor and the log to stable storage.
// Callers must hold rf.mu.
func (rf *Raft) persist() {
	// TODO(persistence): encode the Figure 2 persistent state and
	// rf.persister.Save it alongside the current snapshot.
}

// readPersist restores state saved by persist. Callers must hold rf.mu or
// be the only goroutine with access to rf.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	// TODO(persistence): decode what persist wrote.
}
