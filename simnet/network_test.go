package simnet

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func echo(method string, req []byte) ([]byte, error) {
	return append([]byte(method+":"), req...), nil
}

// newNet boots n servers that all serve echo and returns their clients.
func newNet(t *testing.T, n int) (*Network, []*Client) {
	t.Helper()
	net := New(n, 1)
	t.Cleanup(net.Close)
	clients := make([]*Client, n)
	for i := range clients {
		clients[i] = net.Boot(i)
		net.Serve(i, echo)
	}
	return net, clients
}

func mustCall(t *testing.T, c *Client, to int, want bool) {
	t.Helper()
	_, ok := c.Call(to, "M", []byte("x"))
	if ok != want {
		t.Fatalf("call %d -> %d: ok = %v, want %v", c.from, to, ok, want)
	}
}

func TestCall(t *testing.T) {
	_, c := newNet(t, 2)
	resp, ok := c[0].Call(1, "Echo", []byte("hello"))
	if !ok || string(resp) != "Echo:hello" {
		t.Fatalf("got %q, %v; want %q, true", resp, ok, "Echo:hello")
	}
}

func TestHandlerError(t *testing.T) {
	net, c := newNet(t, 2)
	net.Serve(1, func(string, []byte) ([]byte, error) { return nil, errors.New("boom") })
	mustCall(t, c[0], 1, false)
}

func TestDisconnect(t *testing.T) {
	net, c := newNet(t, 3)
	net.Disconnect(1)
	mustCall(t, c[0], 1, false) // into a disconnected server
	mustCall(t, c[1], 2, false) // out of a disconnected server
	mustCall(t, c[0], 2, true)  // others unaffected
	net.Connect(1)
	mustCall(t, c[0], 1, true)
	mustCall(t, c[1], 2, true)
}

func TestPartition(t *testing.T) {
	net, c := newNet(t, 5)
	net.Partition([]int{0, 1}, []int{2, 3})
	mustCall(t, c[0], 1, true)
	mustCall(t, c[2], 3, true)
	mustCall(t, c[0], 2, false)
	mustCall(t, c[3], 1, false)
	mustCall(t, c[4], 0, false) // unlisted servers form their own group
	net.Heal()
	mustCall(t, c[0], 2, true)
	mustCall(t, c[4], 0, true)
}

func TestCrashInvalidatesOldIncarnation(t *testing.T) {
	net, c := newNet(t, 2)
	net.Crash(1)
	mustCall(t, c[0], 1, false) // crashed server is unreachable
	mustCall(t, c[1], 0, false) // and its clients are dead

	fresh := net.Boot(1)
	mustCall(t, c[0], 1, false) // booted but not serving yet
	net.Serve(1, echo)
	mustCall(t, c[0], 1, true)
	mustCall(t, fresh, 0, true)
	mustCall(t, c[1], 0, false) // the pre-crash client stays dead
}

func TestCrashDropsInFlightReply(t *testing.T) {
	net, c := newNet(t, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	net.Serve(1, func(string, []byte) ([]byte, error) {
		close(entered)
		<-release
		return []byte("late"), nil
	})

	done := make(chan bool)
	go func() {
		_, ok := c[0].Call(1, "Slow", nil)
		done <- ok
	}()
	<-entered
	net.Crash(1)
	close(release)
	if <-done {
		t.Fatal("reply from a crashed incarnation was delivered")
	}
}

func TestDisconnectDropsInFlightReply(t *testing.T) {
	net, c := newNet(t, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	net.Serve(1, func(string, []byte) ([]byte, error) {
		close(entered)
		<-release
		return nil, nil
	})

	done := make(chan bool)
	go func() {
		_, ok := c[0].Call(1, "Slow", nil)
		done <- ok
	}()
	<-entered
	net.Disconnect(0)
	close(release)
	if <-done {
		t.Fatal("reply crossed a link that went down mid-call")
	}
}

func TestPayloadsAreCopied(t *testing.T) {
	net, c := newNet(t, 2)
	var kept []byte
	net.Serve(1, func(_ string, req []byte) ([]byte, error) {
		req[0] = 'X' // a handler scribbling on its request must not reach the caller
		kept = []byte("reply")
		return kept, nil
	})

	req := []byte("abc")
	resp, ok := c[0].Call(1, "M", req)
	if !ok {
		t.Fatal("call failed")
	}
	if string(req) != "abc" {
		t.Fatalf("caller's request was modified: %q", req)
	}
	resp[0] = 'Y'
	if string(kept) != "reply" {
		t.Fatalf("handler's reply buffer was modified: %q", kept)
	}
}

func TestUnreachableCallTimesOut(t *testing.T) {
	net, c := newNet(t, 2)
	net.Disconnect(1)
	var total time.Duration
	const calls = 20
	for range calls {
		start := time.Now()
		mustCall(t, c[0], 1, false)
		total += time.Since(start)
	}
	// Timeouts are uniform in [0, 100ms]; an average this low means calls
	// are failing fast, which lets retry loops spin.
	if avg := total / calls; avg < 20*time.Millisecond {
		t.Fatalf("unreachable calls failed in %v on average; want a timeout-like delay", avg)
	}
}

func TestUnreliableDropsSomeCalls(t *testing.T) {
	net, c := newNet(t, 2)
	net.SetReliable(false)

	const calls = 400
	var succeeded atomic.Int64
	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := c[0].Call(1, "M", nil); ok {
				succeeded.Add(1)
			}
		}()
	}
	wg.Wait()

	// Each call survives two 10% drops: expect ~81% to succeed.
	rate := float64(succeeded.Load()) / calls
	if rate < 0.70 || rate > 0.92 {
		t.Fatalf("success rate %.2f, want about 0.81", rate)
	}
}

func TestLongReorderingDelaysReplies(t *testing.T) {
	net, c := newNet(t, 2)
	net.SetLongReordering(true)

	const calls = 30
	var held atomic.Int64
	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			c[0].Call(1, "M", nil)
			if time.Since(start) >= minReorderDelay {
				held.Add(1)
			}
		}()
	}
	wg.Wait()
	if h := held.Load(); h < calls/3 || h == calls {
		t.Fatalf("%d of %d replies held back, want about two thirds", h, calls)
	}
}

func TestStats(t *testing.T) {
	net, c := newNet(t, 3)
	c[0].Call(1, "M", []byte("1234")) // 4 bytes out, "M:1234" = 6 back
	c[2].Call(1, "M", nil)            // 0 out, "M:" = 2 back
	net.Disconnect(2)
	c[0].Call(2, "M", []byte("12")) // attempted but undelivered

	if got := net.RPCCount(); got != 3 {
		t.Errorf("RPCCount = %d, want 3", got)
	}
	if got := net.ReceivedCount(1); got != 2 {
		t.Errorf("ReceivedCount(1) = %d, want 2", got)
	}
	if got := net.ReceivedCount(2); got != 0 {
		t.Errorf("ReceivedCount(2) = %d, want 0", got)
	}
	if got := net.Bytes(); got != 4+6+0+2+2 {
		t.Errorf("Bytes = %d, want 14", got)
	}
}

func TestClose(t *testing.T) {
	net, c := newNet(t, 2)
	net.Close()
	start := time.Now()
	mustCall(t, c[0], 1, false)
	if d := time.Since(start); d > 10*time.Millisecond {
		t.Fatalf("call on a closed network took %v, want immediate failure", d)
	}
}
