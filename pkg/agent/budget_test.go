package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// One embedded server for this package's stream tests.
var (
	embedOnce sync.Once
	embedURL  string
)

func streamConn(t *testing.T) *nats.Conn {
	t.Helper()
	embedOnce.Do(func() {
		srv, err := server.NewServer(&server.Options{Port: -1})
		if err != nil {
			panic(err)
		}
		go srv.Start()
		if !srv.ReadyForConnections(10 * time.Second) {
			panic("embedded nats-server did not become ready")
		}
		embedURL = srv.ClientURL()
	})
	nc, err := nats.Connect(embedURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// collect subscribes to subject and returns a func that flushes and lists the
// events received so far.
func collect(t *testing.T, nc *nats.Conn, subject string) func() []wire.AgentEvent {
	t.Helper()
	var mu sync.Mutex
	var evs []wire.AgentEvent
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var ev wire.AgentEvent
		if err := json.Unmarshal(m.Data, &ev); err == nil {
			mu.Lock()
			evs = append(evs, ev)
			mu.Unlock()
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return func() []wire.AgentEvent {
		_ = nc.Flush()
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		return append([]wire.AgentEvent(nil), evs...)
	}
}

func progressFrames(evs []wire.AgentEvent) []wire.AgentEvent {
	var out []wire.AgentEvent
	for _, ev := range evs {
		if ev.Type == wire.EventData && ev.Kind == wire.DataKindProgress {
			out = append(out, ev)
		}
	}
	return out
}

func testTurn(budget *wire.Budget) *Turn {
	return &Turn{ChatRequest: wire.ChatRequest{Budget: budget}, RunID: "run_t"}
}

func TestNoBudgetNoDeadline(t *testing.T) {
	ctx, cancel := runContext(testTurn(nil))
	defer cancel(nil)
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("run ctx has a deadline without a budget")
	}
	// Zero/negative budget behaves like no budget.
	ctx2, cancel2 := runContext(testTurn(&wire.Budget{RemainingMs: 0}))
	defer cancel2(nil)
	if _, ok := ctx2.Deadline(); ok {
		t.Fatal("run ctx has a deadline with RemainingMs 0")
	}
	// Cancel keeps today's meaning: plain cancellation.
	cancel(nil)
	if !errors.Is(context.Cause(ctx), context.Canceled) {
		t.Fatalf("cause after cancel = %v, want context.Canceled", context.Cause(ctx))
	}

	a := &Agent{cfg: Config{Name: "x", Description: "d"}}
	if a.ackCaps() != nil {
		t.Fatal("ack caps set without capabilities")
	}
	data, _ := json.Marshal(wire.ChatAck{Accepted: true, Caps: a.ackCaps()})
	if strings.Contains(string(data), "caps") {
		t.Fatalf("ack JSON carries caps: %s", data)
	}
}

func TestBudgetSetsDeadline(t *testing.T) {
	start := time.Now()
	ctx, cancel := runContext(testTurn(&wire.Budget{RemainingMs: 60_000}))
	defer cancel(nil)
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("run ctx has no deadline with a budget")
	}
	want := start.Add(60*time.Second - BudgetMargin)
	if d := dl.Sub(want); d < -time.Second || d > time.Second {
		t.Fatalf("deadline = %v, want ≈ %v (off by %v)", dl, want, d)
	}

	// A budget smaller than the margin still gets at least 1s, then fires
	// with cause ErrTimeCap.
	start = time.Now()
	short, cancelShort := runContext(testTurn(&wire.Budget{RemainingMs: 2_000}))
	defer cancelShort(nil)
	dl, _ = short.Deadline()
	if d := dl.Sub(start); d < time.Second-50*time.Millisecond || d > time.Second+500*time.Millisecond {
		t.Fatalf("short deadline in %v, want ≈ 1s floor", d)
	}
	select {
	case <-short.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("budget deadline did not fire")
	}
	if !errors.Is(context.Cause(short), ErrTimeCap) {
		t.Fatalf("cause = %v, want ErrTimeCap", context.Cause(short))
	}
	if !errors.Is(short.Err(), context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", short.Err())
	}
}

func TestCapsAdvertisedOnlyWhenOptedIn(t *testing.T) {
	a := &Agent{cfg: Config{Name: "x", Description: "d"}}
	card := a.Card()
	if card.Capabilities.Progress || card.Capabilities.Renew {
		t.Fatalf("card caps on without opt-in: %+v", card.Capabilities)
	}
	if a.ackCaps() != nil {
		t.Fatal("ack caps set without opt-in")
	}

	a = &Agent{cfg: Config{Name: "x", Description: "d", Progress: true}}
	card = a.Card()
	if !card.Capabilities.Progress || card.Capabilities.Renew {
		t.Fatalf("card caps = %+v, want progress only", card.Capabilities)
	}
	caps := a.ackCaps()
	if caps == nil || !caps.Progress {
		t.Fatalf("ack caps = %+v, want progress", caps)
	}

	a = &Agent{cfg: Config{Name: "x", Description: "d", Renew: true}}
	if !a.Card().Capabilities.Renew {
		t.Fatal("card renew cap not advertised")
	}
	if a.ackCaps() == nil {
		t.Fatal("ack caps nil with renew opted in")
	}
}

func TestProgressThrottled(t *testing.T) {
	nc := streamConn(t)
	subject := "test.progress.throttle"
	got := collect(t, nc, subject)

	now := time.Now()
	s := newStream(nc, subject, "run_p", log.Default(), true)
	s.now = func() time.Time { return now }

	for i := 0; i < 10; i++ {
		s.Progress("model", "")
		now = now.Add(100 * time.Millisecond) // 10 calls within 1s
	}
	if n := len(progressFrames(got())); n != 1 {
		t.Fatalf("progress frames after 10 calls in 1s = %d, want 1", n)
	}

	now = now.Add(15 * time.Second)
	s.Progress("tool", "search")
	frames := progressFrames(got())
	if len(frames) != 2 {
		t.Fatalf("progress frames after 15s = %d, want 2", len(frames))
	}
	var p wire.ProgressPayload
	if err := json.Unmarshal(frames[1].Payload, &p); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if p.Stage != "tool" || p.Detail != "search" {
		t.Fatalf("payload = %+v, want stage tool detail search", p)
	}
}

func TestProgressNoopWithoutCapability(t *testing.T) {
	nc := streamConn(t)
	subject := "test.progress.off"
	got := collect(t, nc, subject)

	s := newStream(nc, subject, "run_q", log.Default(), false)
	s.Progress("model", "")
	if evs := got(); len(evs) != 0 {
		t.Fatalf("events published without capability: %+v", evs)
	}
}

func TestDoneAndErrorWithReason(t *testing.T) {
	nc := streamConn(t)
	got := collect(t, nc, "test.reason.done")
	s := newStream(nc, "test.reason.done", "run_r", log.Default(), false)
	s.DoneWithReason(wire.StopEndTurn, wire.ReasonTimeCap, nil)
	evs := got()
	if len(evs) != 1 || evs[0].Type != wire.EventDone || evs[0].Reason != wire.ReasonTimeCap {
		t.Fatalf("events = %+v, want one done with reason time_cap", evs)
	}

	got = collect(t, nc, "test.reason.error")
	s = newStream(nc, "test.reason.error", "run_s", log.Default(), false)
	s.ErrorWithReason("cut", wire.CodeInternal, wire.ReasonInterrupted)
	evs = got()
	if len(evs) != 1 || evs[0].Type != wire.EventError || evs[0].Reason != wire.ReasonInterrupted || evs[0].Error != "cut" {
		t.Fatalf("events = %+v, want one error with reason interrupted", evs)
	}
}
