package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// waitCtxHandler signals started, waits for the run ctx to end (at most 5s,
// so a broken runtime fails instead of hanging), reports its cause on causes
// and returns ret(ctx) without a terminal frame.
func waitCtxHandler(started chan<- struct{}, causes chan<- error, ret func(ctx context.Context) error) ChatHandler {
	return func(ctx context.Context, turn *Turn, stream *Stream) error {
		started <- struct{}{}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		causes <- context.Cause(ctx)
		return ret(ctx)
	}
}

func terminal(t *testing.T, run *agentclient.Run) wire.AgentEvent {
	t.Helper()
	evs := drainRun(run)
	if len(evs) == 0 {
		t.Fatal("no events")
	}
	return evs[len(evs)-1]
}

func waitStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
}

func TestInterruptAllNoRuns(t *testing.T) {
	a, _, _ := startRenewAgent(t, testAgentOpts{})
	if n := a.InterruptAll(); n != 0 {
		t.Fatalf("InterruptAll with no runs = %d, want 0", n)
	}
}

func TestInterruptAllCancelsWithCause(t *testing.T) {
	started, causes := make(chan struct{}, 2), make(chan error, 2)
	a, _, _ := startRenewAgent(t, testAgentOpts{handler: waitCtxHandler(started, causes, func(context.Context) error { return nil })})
	r1 := chatAs(t, a, "")
	r2 := chatAs(t, a, "")
	waitStarted(t, started)
	waitStarted(t, started)
	if n := a.InterruptAll(); n != 2 {
		t.Fatalf("InterruptAll = %d, want 2", n)
	}
	for i := 0; i < 2; i++ {
		if c := <-causes; !errors.Is(c, ErrInterrupted) {
			t.Fatalf("cause = %v, want ErrInterrupted", c)
		}
	}
	drainRun(r1)
	drainRun(r2)
}

func TestInterruptEmitsReason(t *testing.T) {
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	a, _, _ := startRenewAgent(t, testAgentOpts{handler: waitCtxHandler(started, causes, func(ctx context.Context) error { return ctx.Err() })})
	run := chatAs(t, a, "")
	waitStarted(t, started)
	a.InterruptAll()
	ev := terminal(t, run)
	if ev.Type != wire.EventError || ev.Reason != wire.ReasonInterrupted || ev.Error != "interrupted" || ev.ErrorStatus != wire.CodeInternal {
		t.Fatalf("terminal = %+v, want error interrupted with reason interrupted", ev)
	}
}

func TestTimeCapFallbackReason(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ret     func(ctx context.Context) error
		wantErr string
	}{
		// The message is fixed: the handler's raw error text never leaks.
		{"handler returns ctx error", func(ctx context.Context) error { return ctx.Err() }, "time limit reached"},
		{"handler returns own error", func(context.Context) error { return errors.New("upstream: secret detail") }, "time limit reached"},
		{"handler returns nil", func(context.Context) error { return nil }, "time limit reached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, causes := make(chan struct{}, 1), make(chan error, 1)
			a, _, _ := startRenewAgent(t, testAgentOpts{handler: waitCtxHandler(started, causes, tc.ret)})
			c := agentclient.NewFromConn(streamConn(t))
			run, err := c.Chat(context.Background(), a.cfg.Name, wire.ChatRequest{
				Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}},
				Budget:  &wire.Budget{RemainingMs: 2_000}, // → 1s deadline floor
			})
			if err != nil {
				t.Fatalf("chat: %v", err)
			}
			waitStarted(t, started)
			if cause := <-causes; !errors.Is(cause, ErrTimeCap) {
				t.Fatalf("cause = %v, want ErrTimeCap", cause)
			}
			ev := terminal(t, run)
			if ev.Type != wire.EventError || ev.Reason != wire.ReasonTimeCap || ev.Error != tc.wantErr || ev.ErrorStatus != wire.CodeInternal {
				t.Fatalf("terminal = %+v, want error %q with reason time_cap", ev, tc.wantErr)
			}
		})
	}
}
