package agent

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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

// cutProbeHandler waits for the run ctx to end and reports CutReason(ctx)
// and the run's user-cancel flag, then returns without a terminal frame.
func cutProbeHandler(ap *atomic.Pointer[Agent], started chan<- struct{}, seen chan<- [2]any) ChatHandler {
	return func(ctx context.Context, turn *Turn, stream *Stream) error {
		started <- struct{}{}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		a := ap.Load()
		a.mu.Lock()
		flag := a.runs[turn.RunID].cancelled
		a.mu.Unlock()
		seen <- [2]any{CutReason(ctx), flag}
		return ctx.Err()
	}
}

func cancelRun(t *testing.T, run *agentclient.Run, reason string) {
	t.Helper()
	ok, err := run.CancelWithReason(context.Background(), reason)
	if err != nil || !ok {
		t.Fatalf("cancel = %v %v, want cancelled:true", ok, err)
	}
}

func TestCancelWithoutReasonIsUserCancel(t *testing.T) {
	var ap atomic.Pointer[Agent]
	started, seen := make(chan struct{}, 1), make(chan [2]any, 1)
	a, _, _ := startRenewAgent(t, testAgentOpts{handler: cutProbeHandler(&ap, started, seen)})
	ap.Store(a)
	run := chatAs(t, a, "")
	waitStarted(t, started)
	ok, err := run.Cancel(context.Background())
	if err != nil || !ok {
		t.Fatalf("Cancel = %v %v", ok, err)
	}
	got := <-seen
	if got[0] != "" || got[1] != true {
		t.Fatalf("CutReason=%q cancelled=%v, want \"\" true", got[0], got[1])
	}
	if ev := terminal(t, run); ev.Type != wire.EventDone || ev.StopReason != wire.StopCancelled || ev.Reason != "" {
		t.Fatalf("terminal = %+v, want done/cancelled", ev)
	}
}

func TestCancelUnknownReasonIsUserCancel(t *testing.T) {
	var ap atomic.Pointer[Agent]
	started, seen := make(chan struct{}, 1), make(chan [2]any, 1)
	a, _, _ := startRenewAgent(t, testAgentOpts{handler: cutProbeHandler(&ap, started, seen)})
	ap.Store(a)
	run := chatAs(t, a, "")
	waitStarted(t, started)
	cancelRun(t, run, "user_was_bored")
	got := <-seen
	if got[0] != "" || got[1] != true {
		t.Fatalf("CutReason=%q cancelled=%v, want plain cancel", got[0], got[1])
	}
	if ev := terminal(t, run); ev.Type != wire.EventDone || ev.StopReason != wire.StopCancelled {
		t.Fatalf("terminal = %+v, want done/cancelled", ev)
	}
}

func TestCancelWithReasonIsCut(t *testing.T) {
	for reason, msg := range map[string]string{
		wire.ReasonProgressIdle:  "stopped: no progress",
		wire.ReasonTokenRotation: "stopped: access could not be renewed",
		wire.ReasonInterrupted:   "interrupted",
		wire.ReasonTimeCap:       "time limit reached",
		wire.ReasonUnavailable:   "stopped: a required service was unavailable",
	} {
		t.Run(reason, func(t *testing.T) {
			var ap atomic.Pointer[Agent]
			started, seen := make(chan struct{}, 1), make(chan [2]any, 1)
			a, _, _ := startRenewAgent(t, testAgentOpts{handler: cutProbeHandler(&ap, started, seen)})
			ap.Store(a)
			run := chatAs(t, a, "")
			waitStarted(t, started)
			cancelRun(t, run, reason)
			got := <-seen
			if got[0] != reason || got[1] != false {
				t.Fatalf("CutReason=%q cancelled=%v, want %q false", got[0], got[1], reason)
			}
			ev := terminal(t, run)
			if ev.Type != wire.EventError || ev.Reason != reason || ev.Error != msg || ev.ErrorStatus != wire.CodeInternal {
				t.Fatalf("terminal = %+v, want error %q reason %s", ev, msg, reason)
			}
		})
	}
}

func TestCutReasonMapping(t *testing.T) {
	cases := []struct {
		cause error
		want  string
	}{
		{ErrTimeCap, wire.ReasonTimeCap},
		{ErrInterrupted, wire.ReasonInterrupted},
		{&CutError{Reason: wire.ReasonProgressIdle}, wire.ReasonProgressIdle},
		{&CutError{Reason: wire.ReasonUnavailable}, wire.ReasonUnavailable},
		{fmt.Errorf("wrapped: %w", &CutError{Reason: wire.ReasonTokenRotation}), wire.ReasonTokenRotation},
		{errors.New("other"), ""},
		{nil, ""},
	}
	for _, tc := range cases {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(tc.cause)
		if got := CutReason(ctx); got != tc.want {
			t.Fatalf("CutReason(cause %v) = %q, want %q", tc.cause, got, tc.want)
		}
	}
	if got := CutReason(context.Background()); got != "" {
		t.Fatalf("live ctx: %q", got)
	}
	if e := (&CutError{Reason: "progress_idle"}).Error(); e != "turn cut: progress_idle" {
		t.Fatalf("Error() = %q", e)
	}
}

// The first cause of a run's end decides its terminal frame: a cut followed
// by a user cancel stays a cut; a user cancel followed by a cut stays a
// user cancel.
func TestFirstCauseDecidesReason(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second string // cancel reasons ("" = user cancel)
		wantCut       bool
	}{
		{"cut then user cancel", wire.ReasonProgressIdle, "", true},
		{"user cancel then cut", "", wire.ReasonProgressIdle, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, ended, release := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
			a, _, _ := startRenewAgent(t, testAgentOpts{handler: func(ctx context.Context, turn *Turn, stream *Stream) error {
				started <- struct{}{}
				<-ctx.Done()
				ended <- struct{}{}
				<-release // stays running so the second cancel reaches it
				return ctx.Err()
			}})
			run := chatAs(t, a, "")
			waitStarted(t, started)
			cancelRun(t, run, tc.first)
			waitStarted(t, ended)
			cancelRun(t, run, tc.second)
			close(release)
			ev := terminal(t, run)
			if tc.wantCut {
				if ev.Type != wire.EventError || ev.Reason != wire.ReasonProgressIdle {
					t.Fatalf("terminal = %+v, want error with reason progress_idle", ev)
				}
			} else if ev.Type != wire.EventDone || ev.StopReason != wire.StopCancelled || ev.Reason != "" {
				t.Fatalf("terminal = %+v, want done/cancelled", ev)
			}
		})
	}
}
