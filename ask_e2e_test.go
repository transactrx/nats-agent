package natsagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/ask"
	"github.com/transactrx/nats-agent/pkg/wire"
)

var e2eAsk = ask.Ask{
	Title: "Pick a day",
	Fields: []ask.Field{
		{Name: "day", Label: "Day", Type: ask.TypeDate, Required: true},
	},
}

type e2eAskResult struct {
	ans ask.Answer
	err error
	vin []agent.ValidInput
}

// startAskE2EAgent: the handler asks e2eAsk once (unless the turn carries
// valid inputs), reports the outcome, then ends (Park when parked).
func startAskE2EAgent(t *testing.T, userInput bool, wait time.Duration, hold ...chan struct{}) (string, chan e2eAskResult) {
	t.Helper()
	results := make(chan e2eAskResult, 8)
	name := "askE2E" + strings.ReplaceAll(t.Name(), "/", "_")
	a, err := agent.New(agent.Config{Name: name, Description: "ask e2e", NATSURL: testURL, UserInput: userInput, AskWait: wait})
	if err != nil {
		t.Fatal(err)
	}
	a.OnChat(func(ctx context.Context, turn *agent.Turn, s *agent.Stream) error {
		if len(turn.ValidInputs) > 0 {
			results <- e2eAskResult{vin: turn.ValidInputs}
			s.Done(wire.StopEndTurn, nil)
			return nil
		}
		ans, err := s.AskUser(ctx, e2eAsk)
		results <- e2eAskResult{ans: ans, err: err}
		for _, h := range hold {
			<-h
		}
		if errors.Is(err, agent.ErrParked) {
			s.Park(ans.AskID)
			return nil
		}
		s.Done(wire.StopEndTurn, nil)
		return nil
	})
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	return name, results
}

func e2eChat(t *testing.T, name string, o agentclient.ChatOptions, content string) *agentclient.Run {
	t.Helper()
	req := wire.ChatRequest{SessionID: "s1", Message: wire.Message{Role: "user"}}
	if content != "" {
		req.Message.Content = []wire.ContentBlock{{Text: content}}
	}
	run, err := agentclient.NewFromConn(testConn(t)).ChatWith(context.Background(), name, req, o)
	if err != nil {
		t.Fatalf("ChatWith: %v", err)
	}
	return run
}

// askIDOf reads events until the askOpened event and returns its id.
func askIDOf(t *testing.T, run *agentclient.Run) string {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-run.Events:
			if !ok {
				t.Fatal("stream ended before askOpened")
			}
			if ev.Type == wire.EventData && ev.Kind == wire.DataKindAsk {
				var p struct {
					AskID string `json:"askId"`
				}
				if err := json.Unmarshal(ev.Payload, &p); err != nil || p.AskID == "" {
					t.Fatalf("askOpened payload %s: %v", ev.Payload, err)
				}
				return p.AskID
			}
		case <-timeout:
			t.Fatal("no askOpened event")
		}
	}
}

func drainRun(run *agentclient.Run) []wire.AgentEvent {
	var evs []wire.AgentEvent
	for ev := range run.Events {
		evs = append(evs, ev)
	}
	return evs
}

func TestRunAnswerRoundTrip(t *testing.T) {
	hold := make(chan struct{})
	name, results := startAskE2EAgent(t, true, time.Minute, hold)
	ctx := context.Background()

	// Submit: invalid first, then valid, then a double submit.
	run := e2eChat(t, name, agentclient.ChatOptions{UserInput: true}, "hi")
	if run.Caps.InputSubject == "" {
		t.Fatalf("no input subject: %+v", run.Caps)
	}
	id := askIDOf(t, run)
	resp, err := run.Answer(ctx, id, wire.InputActionSubmit, map[string]any{"day": "not-a-date"})
	if err != nil || resp.Accepted || resp.Reason != wire.InputReasonInvalid || resp.Errors["day"] == "" {
		t.Fatalf("invalid answer = %+v, %v", resp, err)
	}
	resp, err = run.Answer(ctx, id, wire.InputActionSubmit, map[string]any{"day": "2026-11-22"})
	if err != nil || !resp.Accepted {
		t.Fatalf("submit = %+v, %v", resp, err)
	}
	resp, err = run.Answer(ctx, id, wire.InputActionSubmit, map[string]any{"day": "2026-11-23"})
	if err != nil || resp.Accepted || resp.Reason != wire.InputReasonClosed {
		t.Fatalf("double submit = %+v, %v", resp, err)
	}
	r := recv(t, results)
	close(hold)
	if r.err != nil || r.ans.Status != ask.StatusSubmitted || r.ans.Values["day"] != "2026-11-22" {
		t.Fatalf("handler got %+v", r)
	}
	drainRun(run)

	// Unknown ask id.
	run = e2eChat(t, name, agentclient.ChatOptions{UserInput: true}, "hi")
	id = askIDOf(t, run)
	resp, err = run.Answer(ctx, "nope", wire.InputActionSkip, nil)
	if err != nil || resp.Accepted || resp.Reason != wire.InputReasonUnknownAsk {
		t.Fatalf("unknown ask = %+v, %v", resp, err)
	}

	// Skip.
	resp, err = run.Answer(ctx, id, wire.InputActionSkip, nil)
	if err != nil || !resp.Accepted {
		t.Fatalf("skip = %+v, %v", resp, err)
	}
	r = recv(t, results)
	if !errors.Is(r.err, agent.ErrSkipped) {
		t.Fatalf("handler err = %v, want ErrSkipped", r.err)
	}
	drainRun(run)

	// Run over: no responders.
	if _, err := run.Answer(ctx, id, wire.InputActionSkip, nil); !errors.Is(err, agentclient.ErrRunGone) {
		t.Fatalf("after run end err = %v, want ErrRunGone", err)
	}
}

func TestChatWithEmptyMessageNeedsInputs(t *testing.T) {
	c := agentclient.NewFromConn(testConn(t))
	req := wire.ChatRequest{Message: wire.Message{Role: "user"}}
	if _, err := c.ChatWith(context.Background(), "whoever", req, agentclient.ChatOptions{}); err == nil {
		t.Fatal("empty message without inputs accepted")
	}
	if _, err := c.ChatWith(context.Background(), "whoever", req, agentclient.ChatOptions{UserInput: true}); err == nil {
		t.Fatal("empty message with only UserInput accepted")
	}
}

func TestParkThenInputsResume(t *testing.T) {
	name, results := startAskE2EAgent(t, true, 150*time.Millisecond)

	run := e2eChat(t, name, agentclient.ChatOptions{UserInput: true}, "hi")
	id := askIDOf(t, run)
	r := recv(t, results)
	if !errors.Is(r.err, agent.ErrParked) {
		t.Fatalf("handler err = %v, want ErrParked", r.err)
	}
	var term wire.AgentEvent
	for _, ev := range drainRun(run) {
		term = ev
	}
	if term.Type != wire.EventDone || term.Reason != wire.ReasonAwaitingInput {
		t.Fatalf("terminal = %+v, want done awaiting_input", term)
	}

	raw, _ := json.Marshal(e2eAsk)
	in := wire.AskInput{AskID: id, Action: wire.InputActionSubmit, Values: map[string]any{"day": "2026-11-22"}, Ask: raw}
	run2 := e2eChat(t, name, agentclient.ChatOptions{UserInput: true, Inputs: []wire.AskInput{in}}, "")
	r = recv(t, results)
	if len(r.vin) != 1 || r.vin[0].AskID != id || r.vin[0].Answer.Values["day"] != "2026-11-22" {
		t.Fatalf("resume inputs = %+v", r.vin)
	}
	drainRun(run2)
}

func TestLegacyCallerNoUserInput(t *testing.T) {
	name, results := startAskE2EAgent(t, true, time.Minute)
	run := e2eChat(t, name, agentclient.ChatOptions{}, "hi")
	if run.Caps.InputSubject != "" {
		t.Fatalf("input subject %q for a caller that did not opt in", run.Caps.InputSubject)
	}
	r := recv(t, results)
	if !errors.Is(r.err, agent.ErrUnsupported) {
		t.Fatalf("AskUser err = %v, want ErrUnsupported", r.err)
	}
	drainRun(run)
	if _, err := run.Answer(context.Background(), "x", wire.InputActionSkip, nil); !errors.Is(err, agentclient.ErrNoInputSubject) {
		t.Fatalf("Answer err = %v, want ErrNoInputSubject", err)
	}
}

// Answer-only turns (empty message + inputs) are accepted only by an opted-in
// agent and only when at least one input is valid.
func TestAnswerOnlyTurnGate(t *testing.T) {
	raw, _ := json.Marshal(e2eAsk)
	good := wire.AskInput{AskID: "ask_1", Action: wire.InputActionSkip, Ask: raw}
	bad := wire.AskInput{AskID: "ask_2", Action: "maybe", Ask: raw}
	chat := func(name string, in ...wire.AskInput) error {
		req := wire.ChatRequest{SessionID: "s1", Message: wire.Message{Role: "user"}}
		_, err := agentclient.NewFromConn(testConn(t)).ChatWith(context.Background(), name, req, agentclient.ChatOptions{UserInput: true, Inputs: in})
		return err
	}
	isMissing := func(err error) bool {
		var se *agentclient.ServiceError
		return err != nil && errors.As(err, &se) && se.ApiStatusCode == wire.CodeMissingField
	}

	t.Run("nonOptedRejects", func(t *testing.T) {
		name, _ := startAskE2EAgent(t, false, time.Minute)
		if err := chat(name, good); !isMissing(err) {
			t.Fatalf("err = %v, want missing_field", err)
		}
	})
	t.Run("invalidOnlyRejects", func(t *testing.T) {
		name, _ := startAskE2EAgent(t, true, time.Minute)
		if err := chat(name, bad); !isMissing(err) {
			t.Fatalf("err = %v, want missing_field", err)
		}
	})
	t.Run("oneValidAccepted", func(t *testing.T) {
		name, results := startAskE2EAgent(t, true, time.Minute)
		if err := chat(name, bad, good); err != nil {
			t.Fatalf("err = %v", err)
		}
		if r := recv(t, results); len(r.vin) != 1 || r.vin[0].AskID != "ask_1" {
			t.Fatalf("inputs = %+v", r.vin)
		}
	})
}
