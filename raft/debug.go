package raft

import (
	"fmt"
	"os"
	"time"
)

// Set RAFT_DEBUG=1 to print protocol traces. Traces are the main tool for
// debugging a test that fails one run in a hundred: run it in a loop, keep
// the log of the failing run, and read the interleaving.
var debugEnabled = os.Getenv("RAFT_DEBUG") != ""

var debugStart = time.Now()

// dlog prints a trace line prefixed with milliseconds since start, the
// server and its term. Callers must hold rf.mu.
func (rf *Raft) dlog(format string, args ...any) {
	if !debugEnabled {
		return
	}
	prefix := fmt.Sprintf("%06d S%d T%d ", time.Since(debugStart).Milliseconds(), rf.me, rf.currentTerm)
	fmt.Fprintf(os.Stderr, prefix+format+"\n", args...)
}
