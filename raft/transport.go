package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Transport carries encoded RPCs to other servers. Peers are numbered
// 0..n-1. Call returns ok=false if the RPC or its reply was lost; the remote
// handler may or may not have run.
//
// simnet.Client satisfies Transport, and so would a thin wrapper over TCP or
// gRPC: Raft owns the encoding, the transport only moves bytes.
type Transport interface {
	Call(peer int, method string, req []byte) (resp []byte, ok bool)
}

const (
	methodRequestVote     = "Raft.RequestVote"
	methodAppendEntries   = "Raft.AppendEntries"
	methodInstallSnapshot = "Raft.InstallSnapshot"
)

// Dispatch decodes an incoming RPC, runs the matching handler, and encodes
// the reply. Register it as the server's handler on the transport.
func (rf *Raft) Dispatch(method string, req []byte) ([]byte, error) {
	switch method {
	case methodRequestVote:
		return serve(req, rf.RequestVote)
	case methodAppendEntries:
		return serve(req, rf.AppendEntries)
	case methodInstallSnapshot:
		return serve(req, rf.InstallSnapshot)
	default:
		return nil, fmt.Errorf("raft: unknown method %q", method)
	}
}

func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return call(rf.transport, peer, methodRequestVote, args, reply)
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return call(rf.transport, peer, methodAppendEntries, args, reply)
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return call(rf.transport, peer, methodInstallSnapshot, args, reply)
}

func call[A, R any](t Transport, peer int, method string, args *A, reply *R) bool {
	req, err := encode(args)
	if err != nil {
		panic(fmt.Sprintf("raft: encoding %s args: %v", method, err))
	}
	resp, ok := t.Call(peer, method, req)
	if !ok {
		return false
	}
	// gob does not transmit zero values, so decoding into a reused reply
	// would silently keep stale fields. Always start from zero.
	var zero R
	*reply = zero
	return decode(resp, reply) == nil
}

func serve[A, R any](req []byte, handler func(*A, *R)) ([]byte, error) {
	var args A
	if err := decode(req, &args); err != nil {
		return nil, err
	}
	var reply R
	handler(&args, &reply)
	return encode(&reply)
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
