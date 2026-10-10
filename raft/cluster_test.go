package raft

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	mrand "math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Arjun-J-Jagtap/raft/simnet"
)

// electionTimeout is how long tests allow for a cluster to elect a leader
// after a failure. Implementations should elect within well under this:
// heartbeats at most ~10/s, election timeouts a few hundred milliseconds.
const electionTimeout = 1000 * time.Millisecond

// cluster runs n Raft servers on a simulated network and checks, on every
// applied entry, that no two servers ever commit different commands at the
// same index.
type cluster struct {
	t   *testing.T
	n   int
	net *simnet.Network

	mu          sync.Mutex
	rafts       []*Raft
	persisters  []*Persister
	incarnation []int         // bumped on crash; stale appliers check it
	connected   []bool        // as set by connect/disconnect
	logs        []map[int]any // per server: index -> command it applied
	lastApplied []int         // per server: last index applied, for ordering checks
	applyErr    string        // first safety violation seen by an applier
	maxIndex    int           // highest index applied anywhere

	start time.Time
}

func newCluster(t *testing.T, n int, unreliable bool) *cluster {
	t.Helper()
	seed := mrand.Uint64()
	t.Logf("simnet seed %d", seed)

	c := &cluster{
		t:           t,
		n:           n,
		net:         simnet.New(n, seed),
		rafts:       make([]*Raft, n),
		persisters:  make([]*Persister, n),
		incarnation: make([]int, n),
		connected:   make([]bool, n),
		logs:        make([]map[int]any, n),
		lastApplied: make([]int, n),
		start:       time.Now(),
	}
	c.net.SetReliable(!unreliable)
	for i := range n {
		c.persisters[i] = NewPersister()
		c.restart(i)
		c.connect(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

// restart crashes server i if it is running and boots a new instance from
// its persisted state.
func (c *cluster) restart(i int) {
	c.crash(i)

	c.mu.Lock()
	inc := c.incarnation[i]
	c.logs[i] = map[int]any{}
	c.lastApplied[i] = 0
	persister := c.persisters[i]
	c.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	go c.applier(i, inc, applyCh)

	client := c.net.Boot(i)
	rf := Make(i, c.n, client, persister, applyCh)
	c.net.Serve(i, rf.Dispatch)

	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()
}

// crash kills server i. Its persisted state survives in a copy, so writes
// from the dead instance's leftover goroutines cannot reach its successor.
func (c *cluster) crash(i int) {
	c.net.Crash(i)

	c.mu.Lock()
	c.incarnation[i]++
	rf := c.rafts[i]
	c.rafts[i] = nil
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	c.mu.Unlock()

	if rf != nil {
		rf.Kill()
	}
}

func (c *cluster) connect(i int) {
	c.net.Connect(i)
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
}

func (c *cluster) disconnect(i int) {
	c.net.Disconnect(i)
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i]
}

// raft returns server i's running instance, or nil if it is crashed.
func (c *cluster) raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

// applier consumes server i's apply channel for one incarnation, checking
// that entries arrive in order and agree with every other server.
func (c *cluster) applier(i, inc int, applyCh <-chan ApplyMsg) {
	for m := range applyCh {
		c.mu.Lock()
		if c.incarnation[i] != inc {
			// A killed instance is still draining; keep it from
			// blocking, but don't count what it applies.
			c.mu.Unlock()
			continue
		}
		switch {
		case m.CommandValid:
			if m.CommandIndex != c.lastApplied[i]+1 {
				c.fail("server %d applied index %d out of order (last applied %d)", i, m.CommandIndex, c.lastApplied[i])
			}
			for j, log := range c.logs {
				if prev, ok := log[m.CommandIndex]; ok && prev != m.Command {
					c.fail("index %d: server %d applied %v but server %d applied %v", m.CommandIndex, i, m.Command, j, prev)
				}
			}
			c.logs[i][m.CommandIndex] = m.Command
			c.lastApplied[i] = m.CommandIndex
			c.maxIndex = max(c.maxIndex, m.CommandIndex)
		case m.SnapshotValid:
			c.lastApplied[i] = m.SnapshotIndex
		}
		c.mu.Unlock()
	}
}

// fail records the first safety violation. Appliers run outside the test
// goroutine, so the test fails at its next check rather than here.
// Callers must hold c.mu.
func (c *cluster) fail(format string, args ...any) {
	if c.applyErr == "" {
		c.applyErr = fmt.Sprintf(format, args...)
	}
}

func (c *cluster) checkApplyErr() {
	c.t.Helper()
	c.mu.Lock()
	err := c.applyErr
	c.mu.Unlock()
	if err != "" {
		c.t.Fatal(err)
	}
}

// checkOneLeader waits for exactly one connected leader and returns it.
// Two leaders in the same term fail the test immediately.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for range 10 {
		time.Sleep(450*time.Millisecond + mrand.N(100*time.Millisecond))

		leaders := map[int][]int{} // term -> leaders
		for i := range c.n {
			if rf := c.raft(i); rf != nil && c.isConnected(i) {
				if term, isLeader := rf.State(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		lastTerm := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			lastTerm = max(lastTerm, term)
		}
		if lastTerm != -1 {
			return leaders[lastTerm][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

// checkNoLeader fails if any connected server thinks it is the leader.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := range c.n {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			if _, isLeader := rf.State(); isLeader {
				c.t.Fatalf("expected no leader among connected servers, but %d claims to be leader", i)
			}
		}
	}
}

// checkTerms fails unless all connected servers agree on the term, and
// returns it.
func (c *cluster) checkTerms() int {
	c.t.Helper()
	term := -1
	for i := range c.n {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			t, _ := rf.State()
			if term == -1 {
				term = t
			} else if t != term {
				c.t.Fatalf("servers disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted returns how many servers have applied index, and the command
// they applied.
func (c *cluster) nCommitted(index int) (int, any) {
	c.t.Helper()
	c.checkApplyErr()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd any
	for i := range c.n {
		if v, ok := c.logs[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// wait waits for at least n servers to apply index and returns the command.
// It returns -1 if a server moves past startTerm first, since the entry may
// then never commit. Pass startTerm -1 to wait regardless of term.
func (c *cluster) wait(index, n, startTerm int) any {
	c.t.Helper()
	backoff := 10 * time.Millisecond
	for range 30 {
		if got, _ := c.nCommitted(index); got >= n {
			break
		}
		time.Sleep(backoff)
		backoff = min(2*backoff, time.Second)
		if startTerm > -1 {
			for i := range c.n {
				if rf := c.raft(i); rf != nil {
					if t, _ := rf.State(); t > startTerm {
						return -1
					}
				}
			}
		}
	}
	got, cmd := c.nCommitted(index)
	if got < n {
		c.t.Fatalf("only %d servers applied index %d, want %d", got, index, n)
	}
	return cmd
}

// one submits cmd to whichever server is leader and waits until at least
// expected servers have applied it, returning its index. With retry, it
// resubmits if a leader fails before committing; without, it fails the test
// on the first attempt that does not commit. It gives up after 10 seconds.
func (c *cluster) one(cmd any, expected int, retry bool) int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	next := 0
	for time.Now().Before(deadline) {
		index := -1
		for range c.n {
			s := next
			next = (next + 1) % c.n
			if rf := c.raft(s); rf != nil && c.isConnected(s) {
				if i, _, ok := rf.Start(cmd); ok {
					index = i
					break
				}
			}
		}

		if index == -1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		commitDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(commitDeadline) {
			if got, v := c.nCommitted(index); got > 0 && got >= expected && v == cmd {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !retry {
			c.t.Fatalf("one(%v) failed to reach agreement", cmd)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement within 10s", cmd)
	return -1
}

func (c *cluster) rpcTotal() int { return c.net.RPCCount() }

func (c *cluster) cleanup() {
	for i := range c.n {
		c.crash(i)
	}
	c.net.Close()
	if !c.t.Failed() {
		c.checkApplyErr()
	}
	c.mu.Lock()
	maxIndex := c.maxIndex
	c.mu.Unlock()
	c.t.Logf("%.1fs, %d servers, %d RPCs, %d bytes, %d entries committed",
		time.Since(c.start).Seconds(), c.n, c.net.RPCCount(), c.net.Bytes(), maxIndex)
}

func randString(n int) string {
	b := make([]byte, 2*n)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)[:n]
}

func randDuration(max time.Duration) time.Duration {
	return mrand.N(max)
}
