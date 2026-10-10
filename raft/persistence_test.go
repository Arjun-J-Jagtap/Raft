package raft

// Crash recovery (§5.1 persistent state) and the commit rules that only bite
// across crashes (§5.4.2, Figure 8). Run with:
// go test -race -run Persist ./raft/

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestPersistRestartAll(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)
	c.one(11, n, true)

	// Everyone crashes and restarts from stable storage.
	for i := range n {
		c.restart(i)
	}
	c.one(12, n, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.restart(leader1)
	c.connect(leader1)
	c.one(13, n, true)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.one(14, n-1, true)
	c.restart(leader2)
	c.connect(leader2)

	c.wait(4, n, -1) // leader2 has caught up

	i3 := (c.checkOneLeader() + 1) % n
	c.disconnect(i3)
	c.one(15, n-1, true)
	c.restart(i3)
	c.connect(i3)

	c.one(16, n, true)
}

func TestPersistRepeatedCrashes(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)

	index := 1
	for range 5 {
		c.one(10+index, n, true)
		index++

		leader1 := c.checkOneLeader()
		c.disconnect((leader1 + 1) % n)
		c.disconnect((leader1 + 2) % n)
		c.one(10+index, n-2, true)
		index++

		c.disconnect(leader1)
		c.disconnect((leader1 + 3) % n)
		c.disconnect((leader1 + 4) % n)

		// Two servers that missed the last entry restart, then a third
		// that has it. Only a server with the entry may win.
		c.restart((leader1 + 1) % n)
		c.restart((leader1 + 2) % n)
		c.connect((leader1 + 1) % n)
		c.connect((leader1 + 2) % n)
		time.Sleep(electionTimeout)
		c.restart((leader1 + 3) % n)
		c.connect((leader1 + 3) % n)

		c.one(10+index, n-2, true)
		index++

		c.connect((leader1 + 4) % n)
		c.connect(leader1)
	}
	c.one(1000, n, true)
}

func TestPersistLeaderAndFollowerCrash(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)
	c.one(101, n, true)

	leader := c.checkOneLeader()
	c.disconnect((leader + 2) % n)
	c.one(102, 2, true)

	c.crash(leader)
	c.crash((leader + 1) % n)
	c.connect((leader + 2) % n)
	c.restart(leader)
	c.one(103, 2, true)

	c.restart((leader + 1) % n)
	c.one(104, n, true)
}

// Figure 8 of the paper: a leader must not count replicas to commit an entry
// from an earlier term, or a later leader can overwrite a "committed" entry.
// Leaders here crash constantly, mid-replication.
func TestPersistFigure8(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)
	c.one(rand.Int(), 1, true)

	up := n
	for range 1000 {
		leader := -1
		for i := range n {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start(rand.Int()); ok {
					leader = i
				}
			}
		}

		if rand.IntN(1000) < 100 {
			time.Sleep(randDuration(electionTimeout / 2))
		} else {
			time.Sleep(randDuration(13 * time.Millisecond))
		}

		if leader != -1 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			if s := rand.IntN(n); c.raft(s) == nil {
				c.restart(s)
				up++
			}
		}
	}

	for i := range n {
		if c.raft(i) == nil {
			c.restart(i)
		}
	}
	c.one(rand.Int(), n, true)
}

// Figure 8 again on an unreliable network with heavily reordered replies.
// Leaders are partitioned rather than crashed.
func TestPersistFigure8Unreliable(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)
	c.one(rand.IntN(10000), 1, true)

	up := n
	for iter := range 1000 {
		if iter == 200 {
			c.net.SetLongReordering(true)
		}
		leader := -1
		for i := range n {
			if _, _, ok := c.raft(i).Start(rand.IntN(10000)); ok && c.isConnected(i) {
				leader = i
			}
		}

		if rand.IntN(1000) < 100 {
			time.Sleep(randDuration(electionTimeout / 2))
		} else {
			time.Sleep(randDuration(13 * time.Millisecond))
		}

		if leader != -1 && rand.IntN(2) == 0 {
			c.disconnect(leader)
			up--
		}
		if up < 3 {
			if s := rand.IntN(n); !c.isConnected(s) {
				c.connect(s)
				up++
			}
		}
	}

	for i := range n {
		c.connect(i)
	}
	c.one(rand.IntN(10000), n, true)
}
