package raft

// Log replication and commitment (§5.3, §5.4). Run with:
// go test -race -run Replication ./raft/

import (
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestReplicationBasic(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)

	for index := 1; index <= 3; index++ {
		if got, _ := c.nCommitted(index); got > 0 {
			t.Fatalf("index %d committed before Start", index)
		}
		if got := c.one(index*100, n, false); got != index {
			t.Fatalf("command committed at index %d, want %d", got, index)
		}
	}
}

// Entries carry their payload once per follower, not once per heartbeat.
func TestReplicationRPCBytes(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)
	c.one(99, n, false)
	before := c.net.Bytes()

	var sent int64
	for index := 2; index < 12; index++ {
		cmd := randString(5000)
		if got := c.one(cmd, n, false); got != index {
			t.Fatalf("command committed at index %d, want %d", got, index)
		}
		sent += int64(len(cmd))
	}

	got := c.net.Bytes() - before
	want := int64(n) * sent // n-1 followers, plus slack for encoding and heartbeats
	if got > want+50000 {
		t.Fatalf("sent %d RPC bytes for %d bytes of commands, want at most %d", got, sent, want+50000)
	}
}

func TestReplicationFollowerFailure(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)
	c.one(101, n, false)

	// One follower down: the other two still form a majority.
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % n)
	c.one(102, n-1, false)
	c.one(103, n-1, false)
	time.Sleep(electionTimeout)
	c.one(104, n-1, false)
	c.one(105, n-1, false)

	// The follower comes back and catches up.
	c.connect((leader + 1) % n)
	c.one(106, n, true)
	time.Sleep(electionTimeout)
	c.one(107, n, true)
}

func TestReplicationNoQuorumNoCommit(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)
	c.one(10, n, false)

	// Three of five followers gone: the leader can append but must not
	// commit.
	leader := c.checkOneLeader()
	for i := 1; i <= 3; i++ {
		c.disconnect((leader + i) % n)
	}
	index, _, ok := c.raft(leader).Start(20)
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 2 {
		t.Fatalf("Start returned index %d, want 2", index)
	}
	time.Sleep(2 * electionTimeout)
	if got, _ := c.nCommitted(index); got > 0 {
		t.Fatalf("%d servers committed index %d without a majority", got, index)
	}

	// Quorum restored: a leader, possibly new, carries on. Index 2 may
	// hold either 20 or the new command depending on who wins.
	for i := 1; i <= 3; i++ {
		c.connect((leader + i) % n)
	}
	leader2 := c.checkOneLeader()
	index2, _, ok := c.raft(leader2).Start(30)
	if !ok {
		t.Fatal("new leader rejected Start")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("Start returned index %d, want 2 or 3", index2)
	}
	c.one(1000, n, true)
}

func TestReplicationConcurrentStarts(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)

	for try := range 5 {
		if try > 0 {
			time.Sleep(3 * time.Second) // let the cluster settle
		}

		leader := c.checkOneLeader()
		rf := c.raft(leader)
		_, term, ok := rf.Start(1)
		if !ok {
			continue
		}

		const iters = 5
		indexes := make(chan int, iters)
		var wg sync.WaitGroup
		for i := range iters {
			wg.Add(1)
			go func() {
				defer wg.Done()
				index, term1, ok := rf.Start(100 + i)
				if ok && term1 == term {
					indexes <- index
				}
			}()
		}
		wg.Wait()
		close(indexes)

		if termChanged(c, term) {
			continue
		}

		var cmds []int
		failed := false
		for index := range indexes {
			cmd, ok := c.wait(index, n, term).(int)
			if !ok {
				t.Fatalf("index %d holds a non-int command", index)
			}
			if cmd == -1 {
				failed = true // term moved on; entries may be lost
				break
			}
			cmds = append(cmds, cmd)
		}
		if failed {
			continue
		}
		for i := range iters {
			if !slices.Contains(cmds, 100+i) {
				t.Fatalf("command %d missing from committed %v", 100+i, cmds)
			}
		}
		return
	}
	t.Fatal("term changed too often")
}

// A deposed leader rejoins holding entries nobody else has. They must be
// overwritten, not committed.
func TestReplicationRejoin(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)
	c.one(101, n, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.raft(leader1).Start(102)
	c.raft(leader1).Start(103)
	c.raft(leader1).Start(104)

	c.one(103, 2, true)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)

	// leader1 returns with its stale, uncommitted entries.
	c.connect(leader1)
	c.one(104, 2, true)

	c.connect(leader2)
	c.one(105, n, true)
}

// Leaders must back nextIndex up quickly over long divergent logs: one
// entry per round trip is too slow to pass within the time limits.
func TestReplicationBackup(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)
	c.one(rand.Int(), n, true)

	// The leader and one follower are cut off and accumulate 50
	// uncommitted entries.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % n)
	c.disconnect((leader1 + 3) % n)
	c.disconnect((leader1 + 4) % n)
	for range 50 {
		c.raft(leader1).Start(rand.Int())
	}
	time.Sleep(electionTimeout / 2)
	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % n)

	// The other three elect a leader and commit 50 different entries.
	c.connect((leader1 + 2) % n)
	c.connect((leader1 + 3) % n)
	c.connect((leader1 + 4) % n)
	for range 50 {
		c.one(rand.Int(), 3, true)
	}

	// That leader is cut off with one follower and appends 50 more that
	// never commit.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % n
	if leader2 == other {
		other = (leader2 + 1) % n
	}
	c.disconnect(other)
	for range 50 {
		c.raft(leader2).Start(rand.Int())
	}
	time.Sleep(electionTimeout / 2)

	// The original leader returns, with a follower and `other`. Their logs
	// diverge from each other by up to 100 entries.
	for i := range n {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % n)
	c.connect(other)
	for range 50 {
		c.one(rand.Int(), 3, true)
	}

	for i := range n {
		c.connect(i)
	}
	c.one(rand.Int(), n, true)
}

// A steady cluster should use few RPCs: no RPC storms on election, one
// round of AppendEntries per batch, and a modest heartbeat rate.
func TestReplicationRPCCount(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)
	c.checkOneLeader()

	if total := c.rpcTotal(); total > 30 || total < 1 {
		t.Fatalf("%d RPCs to elect the initial leader, want 1..30", total)
	}

	var total1, total2 int
	success := false
	for try := range 5 {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		leader := c.checkOneLeader()
		total1 = c.rpcTotal()

		const iters = 10
		start, term, ok := c.raft(leader).Start(1)
		if !ok {
			continue
		}
		var cmds []int
		retry := false
		for i := 1; i < iters+2; i++ {
			x := rand.Int()
			cmds = append(cmds, x)
			index, term1, ok := c.raft(leader).Start(x)
			if !ok || term1 != term {
				retry = true // lost leadership mid-batch
				break
			}
			if index != start+i {
				t.Fatalf("Start returned index %d, want %d", index, start+i)
			}
		}
		if retry {
			continue
		}

		for i := 1; i < iters+1; i++ {
			cmd := c.wait(start+i, n, term)
			if cmd == -1 {
				retry = true
				break
			}
			if cmd != cmds[i-1] {
				t.Fatalf("index %d holds %v, want %v", start+i, cmd, cmds[i-1])
			}
		}
		if retry || termChanged(c, term) {
			continue
		}

		total2 = c.rpcTotal()
		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("%d RPCs to replicate %d entries", total2-total1, iters)
		}
		success = true
		break
	}
	if !success {
		t.Fatal("term changed too often")
	}

	time.Sleep(electionTimeout)
	if idle := c.rpcTotal() - total2; idle > 3*20 {
		t.Fatalf("%d RPCs in one second of idleness, want at most 60", idle)
	}
}

// The Figure 8 scenario from §5.4.2, driven step by step rather than left to
// random crashes, which almost never produce it. A leader that commits an
// entry from an earlier term just because a majority stores it can have that
// "committed" entry overwritten by a later leader. The cluster's applier
// catches the overwrite as two servers applying different commands at index 2.
//
// The scenario needs a cut-off leader to keep believing it leads, as in the
// paper. Implementations that step down without a quorum (check-quorum) skip.
func TestReplicationPrevTermCommitRule(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)
	c.one(1, n, true)

	// 1. Leader a, cut off with only b, appends x at index 2 in its term.
	// x reaches b but no majority.
	a := c.checkOneLeader()
	b := (a + 1) % n
	rest := []int{(a + 2) % n, (a + 3) % n, (a + 4) % n}
	for _, i := range rest {
		c.disconnect(i)
	}
	if index, _, ok := c.raft(a).Start(2); !ok {
		t.Skip("isolated leader stepped down; scenario needs a stale leader")
	} else if index != 2 {
		t.Fatalf("Start returned index %d, want 2", index)
	}
	time.Sleep(electionTimeout)

	// 2. a and b leave. The other three elect e in a later term; e is then
	// cut off and appends y at index 2, which nobody else receives.
	c.disconnect(a)
	c.disconnect(b)
	for _, i := range rest {
		c.connect(i)
	}
	e := c.checkOneLeader()
	for _, i := range rest {
		if i != e {
			c.disconnect(i)
		}
	}
	if _, _, ok := c.raft(e).Start(3); !ok {
		t.Skip("isolated leader stepped down; scenario needs a stale leader")
	}
	c.disconnect(e)

	// 3. a, b and a third server g (which lacks index 2) form a majority.
	// a or b wins and copies x to g, so x is now on a majority, but only in
	// an earlier term. A correct leader must not commit it on that basis.
	var g, d int
	for _, i := range rest {
		if i != e {
			g = i
			break
		}
	}
	for _, i := range rest {
		if i != e && i != g {
			d = i
		}
	}
	c.connect(a)
	c.connect(b)
	c.connect(g)
	c.checkOneLeader()
	time.Sleep(2 * electionTimeout)

	// 4. e returns with d and g. e's last entry has a later term than x, so
	// e can win and overwrite index 2 with y. That is only safe if x was
	// never committed.
	c.disconnect(a)
	c.disconnect(b)
	c.connect(e)
	c.connect(d)
	c.one(4, 3, true)

	for i := range n {
		c.connect(i)
	}
	c.one(5, n, true)
}

// termChanged reports whether any running server has moved past term.
func termChanged(c *cluster, term int) bool {
	for i := range c.n {
		if rf := c.raft(i); rf != nil {
			if t, _ := rf.State(); t != term {
				return true
			}
		}
	}
	return false
}
