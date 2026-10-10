package raft

// Leader election (§5.2). Run with: go test -race -run Election ./raft/

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestElectionInitial(t *testing.T) {
	c := newCluster(t, 3, false)
	c.checkOneLeader()

	// Give followers time to learn the leader's term, then make sure it
	// holds steady with no failures.
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, but an election should have advanced it past 0", term1)
	}
	time.Sleep(2 * electionTimeout)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestElectionReElect(t *testing.T) {
	const n = 3
	c := newCluster(t, n, false)

	leader1 := c.checkOneLeader()

	// The remaining majority elects a new leader.
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoins and must not disturb the new one.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// With no quorum, nobody can be elected.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % n)
	time.Sleep(2 * electionTimeout)
	c.checkNoLeader()

	// A quorum returns and elects a leader.
	c.connect((leader2 + 1) % n)
	c.checkOneLeader()

	// The last server rejoins without breaking anything.
	c.connect(leader2)
	c.checkOneLeader()
}

func TestElectionMany(t *testing.T) {
	const n = 7
	c := newCluster(t, n, false)
	c.checkOneLeader()

	for range 10 {
		// Disconnect three random servers; four of seven is still a
		// majority, so a leader must emerge whichever three they are.
		down := rand.Perm(n)[:3]
		for _, i := range down {
			c.disconnect(i)
		}
		c.checkOneLeader()
		for _, i := range down {
			c.connect(i)
		}
	}
	c.checkOneLeader()
}

func TestElectionPartitionedLeaderStepsDown(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)
	leader := c.checkOneLeader()

	// The old leader is cut off with one follower: a minority. The other
	// three elect a new leader in a higher term.
	minority := []int{leader, (leader + 1) % n}
	majority := []int{(leader + 2) % n, (leader + 3) % n, (leader + 4) % n}
	c.net.Partition(minority, majority)
	time.Sleep(2 * electionTimeout)

	newLeader := -1
	newTerm := 0
	for _, i := range majority {
		if term, isLeader := c.raft(i).State(); isLeader {
			newLeader, newTerm = i, term
		}
	}
	if newLeader == -1 {
		t.Fatal("majority partition did not elect a leader")
	}

	// When the partition heals, the stale leader must learn of the higher
	// term and step down, leaving exactly one leader.
	c.net.Heal()
	c.checkOneLeader()
	if term, _ := c.raft(leader).State(); term < newTerm {
		t.Fatalf("stale leader %d still in term %d after the partition healed; leader %d is in term %d",
			leader, term, newLeader, newTerm)
	}
}
