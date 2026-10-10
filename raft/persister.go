package raft

import "sync"

// Persister is a server's stable storage: the Raft state that must survive a
// crash (currentTerm, votedFor, log) and the most recent snapshot.
//
// It is in memory, standing in for a disk. The test harness simulates a crash
// by giving the restarted server a Copy, so writes from the dead instance's
// leftover goroutines cannot leak into the new one. Every value is copied on
// the way in and out, so callers never share buffers with it.
type Persister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

// NewPersister returns an empty Persister.
func NewPersister() *Persister {
	return &Persister{}
}

// Copy returns an independent Persister holding the same data.
func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &Persister{
		raftState: clone(p.raftState),
		snapshot:  clone(p.snapshot),
	}
}

// Save atomically replaces both the Raft state and the snapshot. Writing them
// together matters: a crash between two separate writes could leave a
// snapshot that does not match the log it was taken from.
func (p *Persister) Save(raftState, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(raftState)
	p.snapshot = clone(snapshot)
}

// ReadRaftState returns the saved Raft state, or nil if none was saved.
func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

// RaftStateSize returns the size of the saved Raft state in bytes. A service
// uses this to decide when to snapshot.
func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

// ReadSnapshot returns the saved snapshot, or nil if none was saved.
func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

// SnapshotSize returns the size of the saved snapshot in bytes.
func (p *Persister) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
