package raft

// Unit tests for the parts of the package that are not the protocol: stable
// storage and the RPC codec. Run with: go test -race -run Unit ./raft/

import (
	"errors"
	"testing"
)

func TestUnitPersisterCopiesData(t *testing.T) {
	p := NewPersister()
	state, snap := []byte("state"), []byte("snap")
	p.Save(state, snap)
	state[0], snap[0] = 'X', 'X'

	if got := p.ReadRaftState(); string(got) != "state" {
		t.Fatalf("ReadRaftState = %q after caller modified its buffer", got)
	}
	got := p.ReadSnapshot()
	got[0] = 'Y'
	if again := p.ReadSnapshot(); string(again) != "snap" {
		t.Fatalf("ReadSnapshot = %q after caller modified a returned buffer", again)
	}
	if p.RaftStateSize() != 5 || p.SnapshotSize() != 4 {
		t.Fatalf("sizes = %d, %d; want 5, 4", p.RaftStateSize(), p.SnapshotSize())
	}
}

func TestUnitPersisterCopyIsIndependent(t *testing.T) {
	p := NewPersister()
	p.Save([]byte("before"), nil)
	saved := p.Copy()

	// The crashed instance keeps writing to its old persister.
	p.Save([]byte("after crash"), []byte("snap"))

	if got := saved.ReadRaftState(); string(got) != "before" {
		t.Fatalf("copy sees %q, want %q", got, "before")
	}
	if saved.ReadSnapshot() != nil {
		t.Fatal("copy picked up a snapshot saved after it was taken")
	}
}

// fakeTransport serves every call with handle, so codec tests exercise
// encoding without depending on the protocol handlers.
type fakeTransport struct {
	handle func(method string, req []byte) ([]byte, error)
}

func (f fakeTransport) Call(_ int, method string, req []byte) ([]byte, bool) {
	resp, err := f.handle(method, req)
	return resp, err == nil
}

func TestUnitCodecRoundTrip(t *testing.T) {
	var got AppendEntriesArgs
	rf := &Raft{transport: fakeTransport{func(method string, req []byte) ([]byte, error) {
		if method != methodAppendEntries {
			t.Errorf("method = %q, want %q", method, methodAppendEntries)
		}
		return serve(req, func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			got = *args
			reply.Term = args.Term + 1
			reply.ConflictTerm = -1
		})
	}}}

	args := &AppendEntriesArgs{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 4,
		Entries:      []LogEntry{{Term: 1, Command: 42}, {Term: 2, Command: "put x"}, {Term: 2}},
	}
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(0, args, &reply) {
		t.Fatal("call failed")
	}
	if reply.Term != 4 || reply.ConflictTerm != -1 {
		t.Fatalf("reply = %+v", reply)
	}
	if got.Term != 3 || got.LeaderID != 1 || got.PrevLogIndex != 4 || len(got.Entries) != 3 ||
		got.Entries[0].Command != 42 || got.Entries[1].Command != "put x" || got.Entries[2].Command != nil {
		t.Fatalf("handler received %+v", got)
	}
}

// gob omits zero values, so decoding into a reused reply would keep stale
// fields unless the codec zeroes it first.
func TestUnitCodecZeroesReusedReply(t *testing.T) {
	rf := &Raft{transport: fakeTransport{func(_ string, req []byte) ([]byte, error) {
		return serve(req, func(*RequestVoteArgs, *RequestVoteReply) {}) // all-zero reply
	}}}

	reply := RequestVoteReply{Term: 9, VoteGranted: true}
	if !rf.sendRequestVote(0, &RequestVoteArgs{}, &reply) {
		t.Fatal("call failed")
	}
	if reply.Term != 0 || reply.VoteGranted {
		t.Fatalf("reply kept stale fields: %+v", reply)
	}
}

func TestUnitCodecFailedCall(t *testing.T) {
	rf := &Raft{transport: fakeTransport{func(string, []byte) ([]byte, error) {
		return nil, errors.New("lost")
	}}}
	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(0, &InstallSnapshotArgs{Data: []byte{1}}, &reply) {
		t.Fatal("lost call reported success")
	}
}

func TestUnitDispatchUnknownMethod(t *testing.T) {
	if _, err := (&Raft{}).Dispatch("Raft.Nope", nil); err == nil {
		t.Fatal("unknown method did not return an error")
	}
	if _, err := (&Raft{}).Dispatch(methodAppendEntries, []byte("garbage")); err == nil {
		t.Fatal("undecodable request did not return an error")
	}
}
