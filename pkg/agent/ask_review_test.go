package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// newParseAgent builds an agent (not started) for parseTurn tests.
func newParseAgent(t *testing.T, userInput, enforce bool) *Agent {
	t.Helper()
	streamConn(t)
	cfg := Config{
		Name:          "parse" + strings.ReplaceAll(t.Name(), "/", "_"),
		Description:   "parse test",
		NATSURL:       embedURL,
		UserInput:     userInput,
		IDTValidation: &IDTValidation{},
	}
	if enforce {
		cfg.Access = &wire.AgentAccess{AppID: "appT", FunctionID: "fnT"}
		cfg.IDTValidation = &IDTValidation{Enabled: true, Subject: "trx.identity.none", Timeout: 200 * time.Millisecond}
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	return a
}

func parseMsg(t *testing.T, req wire.ChatRequest) *nats_service.NatsMessage {
	t.Helper()
	body, _ := json.Marshal(req)
	return &nats_service.NatsMessage{Body: body, Logger: log.Default()}
}

// countValidate swaps validateInputs for a counting wrapper.
func countValidate(t *testing.T) *int {
	t.Helper()
	n := 0
	orig := validateInputs
	validateInputs = func(in []wire.AskInput, l *log.Logger) []ValidInput {
		n++
		return orig(in, l)
	}
	t.Cleanup(func() { validateInputs = orig })
	return &n
}

// I1: inputs are validated (regex compiled) only after Authorize.
func TestParseTurnValidatesInputsAfterAuth(t *testing.T) {
	calls := countValidate(t)
	a := newParseAgent(t, true, true)
	raw, _ := json.Marshal(testAsk)
	req := wire.ChatRequest{SessionID: "s1", StreamSubject: "x",
		Inputs: []wire.AskInput{{AskID: "ask_a", Action: wire.InputActionSkip, Ask: raw}}}
	_, err := a.parseTurn(parseMsg(t, req), true)
	if err == nil {
		t.Fatal("unauthenticated request accepted")
	}
	if err.ApiStatusCode == wire.CodeMissingField {
		t.Fatalf("err = %+v, want an auth error", err)
	}
	if *calls != 0 {
		t.Fatalf("validateInputs ran %d times before auth", *calls)
	}
}

// I1: the empty-message gate stays: no opt-in → missing field before auth;
// opted in but no valid input → same error after validation.
func TestParseTurnEmptyMessageGate(t *testing.T) {
	raw, _ := json.Marshal(testAsk)
	bad := []wire.AskInput{{AskID: "ask_a", Action: "maybe", Ask: raw}}
	t.Run("noOptIn", func(t *testing.T) {
		calls := countValidate(t)
		a := newParseAgent(t, false, true)
		_, err := a.parseTurn(parseMsg(t, wire.ChatRequest{SessionID: "s1", StreamSubject: "x", Inputs: bad}), true)
		if err == nil || err.ApiStatusCode != wire.CodeMissingField {
			t.Fatalf("err = %+v, want missing field", err)
		}
		if *calls != 0 {
			t.Fatal("validated inputs without opt-in")
		}
	})
	t.Run("noValidInput", func(t *testing.T) {
		a := newParseAgent(t, true, false)
		_, err := a.parseTurn(parseMsg(t, wire.ChatRequest{SessionID: "s1", StreamSubject: "x", Inputs: bad}), true)
		if err == nil || err.ApiStatusCode != wire.CodeMissingField {
			t.Fatalf("err = %+v, want missing field", err)
		}
	})
	t.Run("validInput", func(t *testing.T) {
		a := newParseAgent(t, true, false)
		in := []wire.AskInput{{AskID: "ask_a", Action: wire.InputActionSkip, Ask: raw}}
		turn, err := a.parseTurn(parseMsg(t, wire.ChatRequest{SessionID: "s1", StreamSubject: "x", Inputs: in}), true)
		if err != nil || len(turn.ValidInputs) != 1 {
			t.Fatalf("turn = %+v, err = %+v", turn, err)
		}
	})
}

// I1: at most maxInputs valid inputs are kept.
func TestParseTurnCapsInputs(t *testing.T) {
	a := newParseAgent(t, true, false)
	raw, _ := json.Marshal(testAsk)
	var in []wire.AskInput
	for i := 0; i < 8; i++ {
		in = append(in, wire.AskInput{AskID: fmt.Sprintf("ask_%d", i), Action: wire.InputActionSkip, Ask: raw})
	}
	turn, err := a.parseTurn(parseMsg(t, wire.ChatRequest{SessionID: "s1", StreamSubject: "x", Inputs: in}), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(turn.ValidInputs) != maxInputs || turn.ValidInputs[0].AskID != "ask_0" {
		t.Fatalf("ValidInputs = %d (first %q), want %d", len(turn.ValidInputs), turn.ValidInputs[0].AskID, maxInputs)
	}
}

// I2: action supersede on the input subject closes the ask as superseded and
// AskUser returns ErrSuperseded with the text.
func TestInputActionSupersede(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	if r := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSupersede}); r.Accepted || r.Reason != wire.InputReasonInvalid {
		t.Fatalf("supersede without text = %+v, want invalid", r)
	}
	if r := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSupersede, Values: map[string]any{"text": "last week instead"}}); !r.Accepted {
		t.Fatalf("supersede = %+v", r)
	}
	r := waitResult(t, results)
	if !errors.Is(r.err, ErrSuperseded) || r.ans.Values["text"] != "last week instead" {
		t.Fatalf("result = %+v, %v", r.ans, r.err)
	}
	var c wire.AskClosedEvent
	_ = json.Unmarshal(nextKind(t, run, wire.DataKindAskClosed).Payload, &c)
	if c.Status != wire.AskClosedSuperseded || c.AskID != p.AskID {
		t.Fatalf("askClosed = %+v", c)
	}
	drainRun(run)
}

// I2: Stream.AnswerAsk accepts supersede too.
func TestStreamAnswerAskSupersede(t *testing.T) {
	streams := make(chan *Stream, 1)
	results := make(chan askResult, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		streams <- s
		ans, err := s.AskUser(ctx, testAsk)
		results <- askResult{ans, err}
		return nil
	}})
	run := chatInput(t, a, true)
	s := <-streams
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	if r := s.AnswerAsk(p.AskID, wire.InputActionSupersede, map[string]any{"text": "no"}); !r.Accepted {
		t.Fatalf("supersede = %+v", r)
	}
	if r := waitResult(t, results); !errors.Is(r.err, ErrSuperseded) || r.ans.Values["text"] != "no" {
		t.Fatalf("result = %+v, %v", r.ans, r.err)
	}
	drainRun(run)
}

// Minor 1: while OnOpen runs the ask is opening: not open to InterruptAsk,
// OpenAsk or answers.
func TestAskOpeningIgnoredUntilOnOpenReturns(t *testing.T) {
	type probe struct {
		interrupted bool
		open        bool
		resp        wire.InputResponse
	}
	probes := make(chan probe, 1)
	streams := make(chan *Stream, 1)
	results := make(chan askResult, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		streams <- s
		ans, err := s.AskUserWith(ctx, testAsk, AskOptions{OnOpen: func(id string) error {
			_, open := s.OpenAsk()
			resp := s.AnswerAsk(id, wire.InputActionSkip, nil)
			probes <- probe{interrupted: s.InterruptAsk("x"), open: open, resp: resp}
			return nil
		}})
		results <- askResult{ans, err}
		return nil
	}})
	run := chatInput(t, a, true)
	<-streams
	pr := <-probes
	if pr.interrupted || pr.open || pr.resp.Accepted || pr.resp.Reason != wire.InputReasonUnknownAsk {
		t.Fatalf("during OnOpen: %+v", pr)
	}
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSkip})
	if r := waitResult(t, results); !errors.Is(r.err, ErrSkipped) {
		t.Fatalf("err = %v", r.err)
	}
	drainRun(run)
}

// Minor 6: Park emits askClosed only for the pending parked ask.
func TestParkUnknownAskNoAskClosed(t *testing.T) {
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(_ context.Context, _ *Turn, s *Stream) error {
		s.Park("ask_notparked")
		return nil
	}})
	evs := drainRun(chatInput(t, a, true))
	if kinds := dataKinds(evs); len(kinds) != 0 {
		t.Fatalf("data events = %v, want none", kinds)
	}
	last := evs[len(evs)-1]
	if last.Type != wire.EventDone || last.Reason != wire.ReasonAwaitingInput {
		t.Fatalf("terminal = %+v", last)
	}
}
