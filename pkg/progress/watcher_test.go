package progress

import (
	"encoding/json"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// fakeClock fires due timers synchronously inside Advance.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	c       *fakeClock
	at      time.Time
	f       func()
	stopped bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		sort.Slice(c.timers, func(i, j int) bool { return c.timers[i].at.Before(c.timers[j].at) })
		var due *fakeTimer
		for i, t := range c.timers {
			if t.stopped {
				continue
			}
			if !t.at.After(target) {
				due = t
				c.timers = append(c.timers[:i], c.timers[i+1:]...)
			}
			break
		}
		if due == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		due.stopped = true
		c.now = due.at
		c.mu.Unlock()
		due.f()
	}
}

func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped {
			n++
		}
	}
	return n
}

func expired(w *Watcher) bool {
	select {
	case <-w.Expired():
		return true
	default:
		return false
	}
}

const window = 10 * time.Second

func TestExpiresAfterWindow(t *testing.T) {
	clk := newFakeClock()
	w := newWatcher(window, clk)
	clk.Advance(window - time.Second)
	if expired(w) {
		t.Fatal("expired before window")
	}
	clk.Advance(time.Second)
	if !expired(w) {
		t.Fatal("not expired at window")
	}
	if clk.pending() != 0 {
		t.Fatal("timer left pending after expiry")
	}
}

func TestPingDoesNotExtend(t *testing.T) {
	clk := newFakeClock()
	w := newWatcher(window, clk)
	ping := &wire.AgentEvent{Type: wire.EventPing}
	for i := 1; i <= int(2*window/time.Second); i++ {
		clk.Advance(time.Second)
		w.Observe(ping)
		if got := expired(w); got != (i >= int(window/time.Second)) {
			t.Fatalf("at %ds expired=%v", i, got)
		}
	}
}

func TestProgressExtends(t *testing.T) {
	clk := newFakeClock()
	w := newWatcher(window, clk)
	clk.Advance(window - time.Second)
	w.Observe(&wire.AgentEvent{Type: wire.EventText, TextDelta: "x"})
	clk.Advance(time.Second)
	if expired(w) {
		t.Fatal("expired at window despite progress")
	}
	if w.Idle() != time.Second {
		t.Fatalf("Idle = %v, want 1s", w.Idle())
	}
	clk.Advance(window - 2*time.Second) // 2×window−2s
	if expired(w) {
		t.Fatal("expired before 2×window−1s")
	}
	clk.Advance(time.Second) // 2×window−1s
	if !expired(w) {
		t.Fatal("not expired at 2×window−1s")
	}
}

func TestZeroWindowNeverExpires(t *testing.T) {
	clk := newFakeClock()
	w := newWatcher(0, clk)
	clk.Advance(24 * time.Hour)
	if expired(w) {
		t.Fatal("zero window expired")
	}
	if clk.pending() != 0 {
		t.Fatal("zero window must not start a timer")
	}
	w.Stop()
}

func TestLastStage(t *testing.T) {
	clk := newFakeClock()
	w := newWatcher(window, clk)
	payload, _ := json.Marshal(wire.ProgressPayload{Stage: "onedrive", Source: "files"})
	w.Observe(&wire.AgentEvent{Type: wire.EventData, Kind: wire.DataKindProgress, Payload: payload})
	if w.LastStage() != "onedrive" {
		t.Fatalf("LastStage = %q, want onedrive", w.LastStage())
	}
	w.Observe(&wire.AgentEvent{Type: wire.EventText, TextDelta: "x"})
	if w.LastStage() != "onedrive" {
		t.Fatalf("text frame changed stage to %q", w.LastStage())
	}
	w.Touch("subagent")
	if w.LastStage() != "subagent" {
		t.Fatalf("LastStage = %q, want subagent", w.LastStage())
	}
}

func TestStopIsIdempotent(t *testing.T) {
	clk := newFakeClock()
	w := newWatcher(window, clk)
	w.Stop()
	w.Stop()
	if clk.pending() != 0 {
		t.Fatal("timer still pending after Stop")
	}
	clk.Advance(2 * window)
	if expired(w) {
		t.Fatal("stopped watcher expired")
	}
	w.Touch("x")
	w.Observe(&wire.AgentEvent{Type: wire.EventText})
}

// Real clock, concurrent callers: must be race-free and close Expired once.
func TestConcurrentUse(t *testing.T) {
	w := NewWatcher(20 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				switch j % 3 {
				case 0:
					w.Observe(&wire.AgentEvent{Type: wire.EventText})
				case 1:
					w.Touch("s")
				default:
					_ = w.Idle()
					_ = w.LastStage()
				}
			}
		}(i)
	}
	wg.Wait()
	select {
	case <-w.Expired():
	case <-time.After(2 * time.Second):
		t.Fatal("never expired")
	}
	w.Stop()
	w.Stop()
}
