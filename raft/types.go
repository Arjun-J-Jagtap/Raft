package raft

// The RPC and log types below follow Figure 2 of the Raft paper
// (Ongaro & Ousterhout, "In Search of an Understandable Consensus
// Algorithm", extended version). Fields are exported so they can be
// gob-encoded on the wire.

// LogEntry is one entry in a server's log.
type LogEntry struct {
	Term    int // term in which the leader received the command
	Command any
}

// RequestVoteArgs is sent by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int
	LastLogIndex int // index of candidate's last log entry (§5.4)
	LastLogTerm  int // term of candidate's last log entry (§5.4)
}

// RequestVoteReply answers a RequestVote.
type RequestVoteReply struct {
	Term        int // currentTerm, for the candidate to update itself
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries (§5.3)
// and, with no entries, as a heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderID     int // so followers can redirect clients
	PrevLogIndex int // index of the entry immediately preceding Entries
	PrevLogTerm  int // term of the PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

// AppendEntriesReply answers an AppendEntries.
//
// ConflictTerm and ConflictIndex are the optimization sketched at the end of
// §5.3: on a mismatch the follower reports where its log diverges, so the
// leader can back nextIndex up a whole term at a time instead of one entry
// per round trip.
type AppendEntriesReply struct {
	Term    int // currentTerm, for the leader to update itself
	Success bool

	ConflictTerm  int // term of the follower's conflicting entry, or -1 if its log is too short
	ConflictIndex int // first index the follower stores for ConflictTerm, or its log length
}

// InstallSnapshotArgs is sent by the leader to a follower that is so far
// behind that the entries it needs have been compacted away (§7).
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int // the snapshot replaces all entries up to and including this index
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply answers an InstallSnapshot.
type InstallSnapshotReply struct {
	Term int
}

// ApplyMsg is sent on the apply channel for every committed entry, in log
// order, and whenever a snapshot received from the leader should replace the
// service's state.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
