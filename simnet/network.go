// Package simnet is an in-memory RPC network for testing distributed
// protocols.
//
// It moves opaque byte payloads between numbered servers and can inject the
// failures a real network produces: disconnected servers, partitions, crashed
// processes, dropped requests and replies, random delays, and long reply
// reordering. Payloads are copied on every hop, so a sender and receiver can
// never share memory by accident.
//
// Each server has an incarnation. Crash and Boot start a new one, which
// invalidates every Client created for the old incarnation and drops any
// reply the old incarnation was still computing. This models a process that
// restarts with fresh sockets: goroutines left over from a dead instance can
// no longer talk to anyone.
package simnet

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Handler serves one RPC. It receives the method name and the encoded
// request, and returns the encoded reply. A non-nil error is reported to the
// caller as a failed call.
type Handler func(method string, req []byte) (resp []byte, err error)

// Failure-injection parameters. The values are deliberately close to what a
// congested datacenter network looks like to a protocol with ~100ms heartbeats.
const (
	dropRate         = 0.10                   // per direction, when unreliable
	maxShortDelay    = 27 * time.Millisecond  // added to every call, when unreliable
	maxTimeout       = 100 * time.Millisecond // how long a call to an unreachable server takes to fail
	maxLongTimeout   = 2 * time.Second        // same, with long delays enabled
	reorderRate      = 2.0 / 3.0              // fraction of replies held back, with long reordering
	minReorderDelay  = 200 * time.Millisecond
	maxReorderJitter = 2 * time.Second
)

// Network is a simulated network of n servers numbered 0..n-1.
// The zero value is not usable; call New.
type Network struct {
	mu  sync.Mutex
	rng *rand.Rand

	handlers  []Handler // nil while a server is down or not yet serving
	gen       []int     // incarnation number, bumped on Crash and Boot
	connected []bool
	group     []int // partition group; servers only reach their own group

	reliable       bool
	longDelays     bool
	longReordering bool
	closed         bool

	rpcs     int
	received []int
	bytes    int64
}

// New returns a reliable, fully connected network of n servers. No server is
// serving yet; call Boot and Serve for each one.
func New(n int, seed uint64) *Network {
	return &Network{
		rng:       rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		handlers:  make([]Handler, n),
		gen:       make([]int, n),
		connected: fillBool(n, true),
		group:     make([]int, n),
		reliable:  true,
		received:  make([]int, n),
	}
}

// Boot starts a new incarnation of server id and returns the Client it should
// send RPCs through. The server does not receive RPCs until Serve is called,
// which lets the caller construct its server with the client before
// registering the server's handler.
func (n *Network) Boot(id int) *Client {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.gen[id]++
	n.handlers[id] = nil
	return &Client{net: n, from: id, gen: n.gen[id]}
}

// Serve registers h as the handler for the current incarnation of server id.
func (n *Network) Serve(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Crash kills the current incarnation of server id. Calls to it fail, calls
// from its clients fail, and replies it was still computing are dropped.
func (n *Network) Crash(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.gen[id]++
	n.handlers[id] = nil
}

// Connect reattaches server id to the network.
func (n *Network) Connect(id int) { n.setConnected(id, true) }

// Disconnect detaches server id from the network in both directions. The
// server keeps running; it just cannot reach or be reached by anyone.
func (n *Network) Disconnect(id int) { n.setConnected(id, false) }

func (n *Network) setConnected(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = up
}

// Partition splits the network into the given groups. Servers can only reach
// servers in the same group; servers not listed form one extra group of their
// own. Partition is independent of Connect/Disconnect.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := range n.group {
		n.group[i] = -1
	}
	for g, members := range groups {
		for _, id := range members {
			n.group[id] = g
		}
	}
}

// Heal removes any partition.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := range n.group {
		n.group[i] = 0
	}
}

// SetReliable turns random drops and delays off (true) or on (false).
func (n *Network) SetReliable(reliable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = reliable
}

// SetLongDelays makes calls to unreachable servers take up to seconds to fail
// instead of up to 100ms, like a TCP connect timeout.
func (n *Network) SetLongDelays(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longDelays = on
}

// SetLongReordering holds most replies back for a random, often long, time,
// so replies arrive far out of order relative to the requests that caused
// them.
func (n *Network) SetLongReordering(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longReordering = on
}

// Close shuts the network down. Every later call fails immediately.
func (n *Network) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
}

// RPCCount returns the number of calls attempted on the network.
func (n *Network) RPCCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcs
}

// ReceivedCount returns the number of calls delivered to server id.
func (n *Network) ReceivedCount(id int) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.received[id]
}

// Bytes returns the total request and reply payload bytes sent.
func (n *Network) Bytes() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.bytes
}

// Client sends RPCs on behalf of one incarnation of one server.
type Client struct {
	net  *Network
	from int
	gen  int
}

// Call sends req to server `to` and returns its reply. ok is false if the
// request or reply was lost, either server is unreachable or crashed, or the
// handler returned an error. Like a real RPC, a false ok does not mean the
// handler did not run.
func (c *Client) Call(to int, method string, req []byte) (resp []byte, ok bool) {
	n := c.net

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, false
	}
	n.rpcs++
	n.bytes += int64(len(req))
	senderAlive := n.gen[c.from] == c.gen
	h := n.handlers[to]
	targetGen := n.gen[to]
	reachable := senderAlive && h != nil && n.linked(c.from, to)
	reliable := n.reliable
	timeout := n.randDuration(maxTimeout)
	if n.longDelays {
		timeout = n.randDuration(maxLongTimeout)
	}
	delay := n.randDuration(maxShortDelay)
	dropRequest := n.rng.Float64() < dropRate
	if reachable {
		n.received[to]++
	}
	n.mu.Unlock()

	if !reachable {
		// Fail slowly, like a timeout, so a sender that retries in a loop
		// does not spin.
		time.Sleep(timeout)
		return nil, false
	}
	if !reliable {
		time.Sleep(delay)
		if dropRequest {
			return nil, false
		}
	}

	resp, err := h(method, clone(req))
	if err != nil {
		return nil, false
	}

	n.mu.Lock()
	// The reply only gets back if both ends are still the same, connected
	// incarnations that sent and served the request.
	delivered := !n.closed && n.gen[c.from] == c.gen && n.gen[to] == targetGen && n.linked(c.from, to)
	dropReply := !n.reliable && n.rng.Float64() < dropRate
	var hold time.Duration
	if n.longReordering && n.rng.Float64() < reorderRate {
		hold = minReorderDelay + n.randDuration(n.randDuration(maxReorderJitter))
	}
	if delivered && !dropReply {
		n.bytes += int64(len(resp))
	}
	n.mu.Unlock()

	if !delivered || dropReply {
		return nil, false
	}
	time.Sleep(hold)
	return clone(resp), true
}

// linked reports whether a and b can currently exchange messages.
// Callers must hold n.mu.
func (n *Network) linked(a, b int) bool {
	return n.connected[a] && n.connected[b] && n.group[a] == n.group[b]
}

// randDuration returns a uniform duration in [0, max]. Callers must hold n.mu.
func (n *Network) randDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(n.rng.Int64N(int64(max) + 1))
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func fillBool(n int, v bool) []bool {
	s := make([]bool, n)
	for i := range s {
		s[i] = v
	}
	return s
}
