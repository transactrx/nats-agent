package rotation

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeClock hands out channels that fire when Advance moves past their
// deadline. Nothing sleeps for real.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

var t0 = time.Unix(1_000_000, 0)

func newFakeClock() *fakeClock { return &fakeClock{now: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, &waiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// Advance moves the clock by d and fires every waiter now due.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(c.now) {
			kept = append(kept, w)
			continue
		}
		w.ch <- c.now
	}
	c.waiters = kept
}

// fireNext advances to the earliest pending deadline.
func (c *fakeClock) fireNext() {
	c.mu.Lock()
	sort.Slice(c.waiters, func(i, j int) bool { return c.waiters[i].at.Before(c.waiters[j].at) })
	d := c.waiters[0].at.Sub(c.now)
	c.mu.Unlock()
	c.Advance(d)
}

// waitPending waits (in real time, briefly) until n waiters are registered
// or done closes; false when done closed first.
func waitPending(t *testing.T, c *fakeClock, n int, done <-chan struct{}) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.pending() < n {
		select {
		case <-done:
			return false
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotator never waited (pending %d, want %d)", c.pending(), n)
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// recorder collects the clock times of Redeem/Push/Retire calls.
type recorder struct {
	mu      sync.Mutex
	redeems []time.Duration
	pushes  []Token
	pushAt  []time.Duration
	retired []Token
	retAt   []time.Duration
}

func (r *recorder) snapshot() (redeems []time.Duration, pushes []Token, pushAt []time.Duration, retired []Token, retAt []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.redeems...), append([]Token(nil), r.pushes...),
		append([]time.Duration(nil), r.pushAt...), append([]Token(nil), r.retired...), append([]time.Duration(nil), r.retAt...)
}

type harness struct {
	t      *testing.T
	clock  *fakeClock
	rec    *recorder
	cfg    Config
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returned; err is then set
	err    error
	pushed chan Token
}

// newHarness builds a config whose Redeem returns tokens living life from
// now; redeemErrs/pushErrs fail the first N calls (or every call when < 0).
func newHarness(t *testing.T, life time.Duration, redeemErrs, pushErrs int) *harness {
	h := &harness{t: t, clock: newFakeClock(), rec: &recorder{}, pushed: make(chan Token, 64)}
	n := 0
	h.cfg = Config{
		TTL:          15 * time.Minute,
		RotateBefore: 8 * time.Minute,
		GiveUpBefore: 2 * time.Minute,
		Redeem: func(ctx context.Context, ttl time.Duration) (Token, error) {
			now := h.clock.Now()
			h.rec.mu.Lock()
			defer h.rec.mu.Unlock()
			h.rec.redeems = append(h.rec.redeems, now.Sub(t0))
			if redeemErrs < 0 || len(h.rec.redeems) <= redeemErrs {
				return Token{}, errors.New("identity down")
			}
			n++
			return Token{Value: "tok" + string(rune('0'+n)), ExpiresAt: now.Add(life), RunSessionID: "rs1"}, nil
		},
		Push: func(ctx context.Context, tok Token) error {
			now := h.clock.Now()
			h.rec.mu.Lock()
			h.rec.pushes = append(h.rec.pushes, tok)
			h.rec.pushAt = append(h.rec.pushAt, now.Sub(t0))
			fail := pushErrs < 0 || len(h.rec.pushes) <= pushErrs
			h.rec.mu.Unlock()
			if fail {
				return errors.New("agent unreachable")
			}
			h.pushed <- tok
			return nil
		},
	}
	return h
}

func (h *harness) start(first Token) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan struct{})
	go func() {
		h.err = run(ctx, h.cfg, first, h.clock)
		close(h.done)
	}()
	h.t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
		}
	})
}

func (h *harness) waitPushed() Token {
	h.t.Helper()
	select {
	case tok := <-h.pushed:
		return tok
	case <-time.After(5 * time.Second):
		h.t.Fatal("no successful push")
		return Token{}
	}
}

func (h *harness) stop() error {
	h.t.Helper()
	h.cancel()
	select {
	case <-h.done:
		return h.err
	case <-time.After(5 * time.Second):
		h.t.Fatal("Run did not return after cancel")
		return nil
	}
}

func firstToken() Token {
	return Token{Value: "tok0", ExpiresAt: t0.Add(15 * time.Minute), RunSessionID: "rs1"}
}

func TestRotatesAtThreshold(t *testing.T) {
	h := newHarness(t, 15*time.Minute, 0, 0)
	h.start(firstToken())
	waitPending(t, h.clock, 1, h.done)
	h.clock.Advance(7*time.Minute - time.Nanosecond)
	if r, _, _, _, _ := h.rec.snapshot(); len(r) != 0 {
		t.Fatalf("redeemed early at %v", r)
	}
	h.clock.Advance(time.Nanosecond)
	tok := h.waitPushed()
	redeems, pushes, pushAt, _, _ := h.rec.snapshot()
	if len(redeems) != 1 || redeems[0] != 7*time.Minute {
		t.Fatalf("redeems at %v, want [7m]", redeems)
	}
	if len(pushes) != 1 || tok.Value != "tok1" || pushAt[0] != 7*time.Minute {
		t.Fatalf("pushes %+v at %v", pushes, pushAt)
	}
	if !tok.ExpiresAt.Equal(t0.Add(22 * time.Minute)) {
		t.Fatalf("pushed ExpiresAt = %v", tok.ExpiresAt)
	}
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
}

// Identity may ignore the requested TTL: the next rotation follows the
// expiry it returned, not cfg.TTL.
func TestSchedulesFromReturnedExpiry(t *testing.T) {
	h := newHarness(t, 60*time.Minute, 0, 0)
	h.start(firstToken())
	waitPending(t, h.clock, 1, h.done)
	h.clock.Advance(7 * time.Minute)
	h.waitPushed()
	waitPending(t, h.clock, 1, h.done)
	h.clock.Advance(52*time.Minute - time.Nanosecond)
	if r, _, _, _, _ := h.rec.snapshot(); len(r) != 1 {
		t.Fatalf("second redeem too early: %v", r)
	}
	h.clock.Advance(time.Nanosecond)
	h.waitPushed()
	redeems, _, _, _, _ := h.rec.snapshot()
	if len(redeems) != 2 || redeems[1] != 59*time.Minute {
		t.Fatalf("redeems at %v, want [7m 59m]", redeems)
	}
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t, 15*time.Minute, 2, 0)
	h.start(firstToken())
	for i := 0; i < 3; i++ {
		waitPending(t, h.clock, 1, h.done)
		h.clock.fireNext()
	}
	h.waitPushed()
	redeems, pushes, _, _, _ := h.rec.snapshot()
	want := []time.Duration{7 * time.Minute, 7*time.Minute + 5*time.Second, 7*time.Minute + 15*time.Second}
	if len(redeems) != 3 || redeems[0] != want[0] || redeems[1] != want[1] || redeems[2] != want[2] {
		t.Fatalf("redeems at %v, want %v", redeems, want)
	}
	if len(pushes) != 1 {
		t.Fatalf("pushes = %d, want 1", len(pushes))
	}
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
}

func TestGivesUp(t *testing.T) {
	h := newHarness(t, 15*time.Minute, -1, 0)
	h.start(firstToken())
	var err error
	for {
		if !waitPending(t, h.clock, 1, h.done) {
			err = h.err
			break
		}
		h.clock.fireNext()
	}
	if !errors.Is(err, ErrRotationFailed) {
		t.Fatalf("Run = %v, want ErrRotationFailed", err)
	}
	if got := h.clock.Now().Sub(t0); got != 13*time.Minute {
		t.Fatalf("gave up at %v, want 13m (expiry - 2m)", got)
	}
	redeems, pushes, _, _, _ := h.rec.snapshot()
	// 7:00, then backoff 5s, 10s, 20s, 30s (cap)… until 13:00.
	want := []time.Duration{7 * time.Minute, 7*time.Minute + 5*time.Second, 7*time.Minute + 15*time.Second, 7*time.Minute + 35*time.Second}
	for i, w := range want {
		if redeems[i] != w {
			t.Fatalf("redeem %d at %v, want %v (all %v)", i, redeems[i], w, redeems)
		}
	}
	for i := len(want); i < len(redeems); i++ {
		if d := redeems[i] - redeems[i-1]; d != 30*time.Second {
			t.Fatalf("backoff %d = %v, want 30s cap (all %v)", i, d, redeems)
		}
	}
	if last := redeems[len(redeems)-1]; last >= 13*time.Minute {
		t.Fatalf("redeemed at %v, after give-up point", last)
	}
	if len(pushes) != 0 {
		t.Fatalf("pushed %d tokens", len(pushes))
	}
}

// A token that does not outlive the current one is a failed attempt.
func TestStaleTokenCountsAsFailure(t *testing.T) {
	h := newHarness(t, 15*time.Minute, 0, 0)
	calls := 0
	h.cfg.Redeem = func(ctx context.Context, ttl time.Duration) (Token, error) {
		calls++
		if calls == 1 {
			return Token{Value: "stale", ExpiresAt: t0.Add(15 * time.Minute)}, nil
		}
		return Token{Value: "fresh", ExpiresAt: h.clock.Now().Add(15 * time.Minute)}, nil
	}
	h.start(firstToken())
	waitPending(t, h.clock, 1, h.done)
	h.clock.fireNext()
	waitPending(t, h.clock, 1, h.done)
	h.clock.fireNext()
	tok := h.waitPushed()
	if tok.Value != "fresh" || calls != 2 {
		t.Fatalf("pushed %q after %d redeems, want fresh after 2", tok.Value, calls)
	}
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
}

// A failed push keeps the redeemed token and retries delivering it.
func TestPushFailureRetried(t *testing.T) {
	h := newHarness(t, 15*time.Minute, 0, 1)
	h.start(firstToken())
	waitPending(t, h.clock, 1, h.done)
	h.clock.fireNext()
	waitPending(t, h.clock, 1, h.done)
	h.clock.fireNext()
	tok := h.waitPushed()
	redeems, pushes, pushAt, _, _ := h.rec.snapshot()
	if len(pushes) != 2 || pushAt[0] != 7*time.Minute || pushAt[1] != 7*time.Minute+5*time.Second {
		t.Fatalf("pushes at %v, want [7m 7m5s]", pushAt)
	}
	if pushes[0].Value != pushes[1].Value || tok.Value != "tok1" {
		t.Fatalf("retried push changed token: %+v", pushes)
	}
	if len(redeems) != 1 {
		t.Fatalf("redeems = %v, want one", redeems)
	}
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
}

func TestRetireCalledAfterOverlap(t *testing.T) {
	h := newHarness(t, 60*time.Minute, 0, 0)
	h.cfg.Overlap = 8 * time.Minute
	retired := make(chan Token, 4)
	h.cfg.Retire = func(ctx context.Context, old Token) {
		now := h.clock.Now()
		h.rec.mu.Lock()
		h.rec.retired = append(h.rec.retired, old)
		h.rec.retAt = append(h.rec.retAt, now.Sub(t0))
		h.rec.mu.Unlock()
		retired <- old
	}
	h.start(firstToken())
	waitPending(t, h.clock, 1, h.done)
	h.clock.Advance(7 * time.Minute)
	h.waitPushed()
	// Next rotation (59m) and the retire (15m) are both pending.
	waitPending(t, h.clock, 2, h.done)
	h.clock.Advance(8*time.Minute - time.Nanosecond)
	if _, _, _, r, _ := h.rec.snapshot(); len(r) != 0 {
		t.Fatalf("retired before overlap: %+v", r)
	}
	h.clock.Advance(time.Nanosecond)
	select {
	case old := <-retired:
		if old.Value != "tok0" {
			t.Fatalf("retired %q, want tok0", old.Value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Retire not called after overlap")
	}
	_, _, _, r, at := h.rec.snapshot()
	if len(r) != 1 || at[0] != 15*time.Minute {
		t.Fatalf("retired %+v at %v, want once at 15m", r, at)
	}
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if _, _, _, r, _ := h.rec.snapshot(); len(r) != 1 {
		t.Fatalf("Retire called %d times", len(r))
	}
}

func TestStopsOnContextCancel(t *testing.T) {
	h := newHarness(t, 15*time.Minute, 0, 0)
	h.start(firstToken())
	waitPending(t, h.clock, 1, h.done)
	if err := h.stop(); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if r, _, _, _, _ := h.rec.snapshot(); len(r) != 0 {
		t.Fatalf("redeemed after cancel: %v", r)
	}
}

func TestFirstWithoutExpiry(t *testing.T) {
	h := newHarness(t, 15*time.Minute, 0, 0)
	err := Run(context.Background(), h.cfg, Token{Value: "tok0"})
	if err == nil {
		t.Fatal("Run accepted a first token without expiry")
	}
}
