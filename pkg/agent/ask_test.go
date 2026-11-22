package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/ask"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// testAsk is a two-field form: a required date and an optional name.
var testAsk = ask.Ask{
	Title: "Pick a day",
	Fields: []ask.Field{
		{Name: "day", Label: "Day", Type: ask.TypeDate, Required: true},
		{Name: "who", Label: "Who", Type: ask.TypeText},
	},
}

type askResult struct {
	ans ask.Answer
	err error
}

type askAgentOpts struct {
	userInput bool          // Config.UserInput
	wait      time.Duration // Config.AskWait
	handler   ChatHandler   // nil = ask testAsk once, report on results
	hold      chan struct{} // default handler: keep the run open until closed
}

// startAskAgent starts an agent on the embedded server. The default handler
// asks testAsk once and sends the outcome to results.
func startAskAgent(t *testing.T, o askAgentOpts) (*Agent, chan askResult) {
	t.Helper()
	streamConn(t)
	results := make(chan askResult, 256)
	cfg := Config{
		Name:          "ask" + strings.ReplaceAll(t.Name(), "/", "_"),
		Description:   "ask test",
		NATSURL:       embedURL,
		UserInput:     o.userInput,
		AskWait:       o.wait,
		IDTValidation: &IDTValidation{},
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := o.handler
	if h == nil {
		h = func(ctx context.Context, _ *Turn, s *Stream) error {
			ans, err := s.AskUser(ctx, testAsk)
			results <- askResult{ans, err}
			if o.hold != nil {
				<-o.hold
			}
			return nil
		}
	}
	a.OnChat(h)
	if err := a.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	return a, results
}

// chatInput starts a run; supported sets ChatRequest.userInput.
func chatInput(t *testing.T, a *Agent, supported bool, inputs ...wire.AskInput) *agentclient.Run {
	t.Helper()
	req := wire.ChatRequest{
		SessionID: "s1",
		Message:   wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}},
		Inputs:    inputs,
	}
	if supported {
		req.UserInput = &wire.UserInputSupport{Supported: true}
	}
	run, err := agentclient.NewFromConn(streamConn(t)).Chat(context.Background(), a.cfg.Name, req)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	return run
}

func inputSubject(run *agentclient.Run) string {
	if run.Ack.Caps == nil {
		return ""
	}
	return run.Ack.Caps.InputSubject
}

// answer posts one InputRequest to the run's input subject.
func answer(t *testing.T, run *agentclient.Run, req wire.InputRequest) wire.InputResponse {
	t.Helper()
	return answerOn(t, streamConn(t), run, req)
}

// answerOn is answer on an existing connection (no connect latency).
func answerOn(t *testing.T, nc *nats.Conn, run *agentclient.Run, req wire.InputRequest) wire.InputResponse {
	t.Helper()
	resp, err := answerErr(nc, run, req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// answerErr is answerOn without the testing.T, safe off the test goroutine.
func answerErr(nc *nats.Conn, run *agentclient.Run, req wire.InputRequest) (wire.InputResponse, error) {
	subj := inputSubject(run)
	if subj == "" {
		return wire.InputResponse{}, fmt.Errorf("ack has no inputSubject: %+v", run.Ack.Caps)
	}
	if req.RunID == "" {
		req.RunID = run.Ack.RunID
	}
	data, _ := json.Marshal(req)
	msg, err := nc.Request(subj, data, 3*time.Second)
	if err != nil {
		return wire.InputResponse{}, fmt.Errorf("input request: %w", err)
	}
	var resp wire.InputResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return wire.InputResponse{}, fmt.Errorf("input reply %q: %w", msg.Data, err)
	}
	return resp, nil
}

type askEventPayload struct {
	AskID  string  `json:"askId"`
	WaitMs int64   `json:"waitMs"`
	Origin string  `json:"origin"`
	Ask    ask.Ask `json:"ask"`
	Status string  `json:"status"`
}

func payloadOf(t *testing.T, ev wire.AgentEvent) askEventPayload {
	t.Helper()
	var p askEventPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("payload of %s: %v", ev.Kind, err)
	}
	return p
}

// nextKind reads events until a data event of kind; fails at terminal.
func nextKind(t *testing.T, run *agentclient.Run, kind string) wire.AgentEvent {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-run.Events:
			if !ok {
				t.Fatalf("stream ended before a %q event", kind)
			}
			if ev.Type == wire.EventData && ev.Kind == kind {
				return ev
			}
			if ev.IsTerminal() {
				t.Fatalf("terminal %s (%s) before a %q event", ev.Type, ev.Error, kind)
			}
		case <-timeout:
			t.Fatalf("no %q event", kind)
		}
	}
}

func waitResult(t *testing.T, results chan askResult) askResult {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("AskUser never returned")
		return askResult{}
	}
}

func dataKinds(evs []wire.AgentEvent) []string {
	var out []string
	for _, ev := range evs {
		if ev.Type == wire.EventData {
			out = append(out, ev.Kind)
		}
	}
	return out
}

func TestAskUnsupportedWithoutRequestFlag(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, false)
	r := waitResult(t, results)
	if !errors.Is(r.err, ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", r.err)
	}
	if kinds := dataKinds(drainRun(run)); len(kinds) != 0 {
		t.Fatalf("data events %v, want none", kinds)
	}
}

func TestAskUnsupportedWithoutCardFlag(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: false})
	if a.Card().Capabilities.UserInput {
		t.Fatal("card advertises userInput without Config.UserInput")
	}
	run := chatInput(t, a, true)
	if s := inputSubject(run); s != "" {
		t.Fatalf("inputSubject %q without the card flag", s)
	}
	r := waitResult(t, results)
	if !errors.Is(r.err, ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", r.err)
	}
	if kinds := dataKinds(drainRun(run)); len(kinds) != 0 {
		t.Fatalf("data events %v, want none", kinds)
	}
}

func TestAskCardCapability(t *testing.T) {
	a := &Agent{cfg: Config{Name: "x", Description: "d", UserInput: true}}
	if !a.Card().Capabilities.UserInput {
		t.Fatal("card lacks userInput")
	}
}

func TestAskSubmitWithinWait(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, true)
	prefix := wire.AgentSubject(a.cfg.Name, "input.")
	if !strings.HasPrefix(inputSubject(run), prefix) || len(inputSubject(run)) != len(prefix)+22 {
		t.Fatalf("inputSubject = %q, want %s<22 chars>", inputSubject(run), prefix)
	}
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	if !strings.HasPrefix(p.AskID, "ask_") || len(p.AskID) != 4+16 {
		t.Fatalf("askId = %q, want ask_<16 chars>", p.AskID)
	}
	if p.WaitMs != (5*time.Minute).Milliseconds() || p.Origin != a.cfg.Name || p.Ask.Title != testAsk.Title {
		t.Fatalf("ask payload = %+v", p)
	}
	resp := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSubmit, Values: map[string]any{"day": "2026-11-22", "who": "Ann"}})
	if !resp.Accepted {
		t.Fatalf("submit rejected: %+v", resp)
	}
	r := waitResult(t, results)
	if r.err != nil {
		t.Fatalf("AskUser err = %v", r.err)
	}
	if r.ans.AskID != p.AskID || r.ans.Status != ask.StatusSubmitted || r.ans.Values["day"] != "2026-11-22" || r.ans.Display["day"] != "Nov 22, 2026" {
		t.Fatalf("answer = %+v", r.ans)
	}
	c := payloadOf(t, nextKind(t, run, wire.DataKindAskClosed))
	if c.AskID != p.AskID || c.Status != "answered" {
		t.Fatalf("askClosed = %+v", c)
	}
	drainRun(run)
}

func TestAskSkip(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	if resp := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSkip}); !resp.Accepted {
		t.Fatalf("skip rejected: %+v", resp)
	}
	r := waitResult(t, results)
	if !errors.Is(r.err, ErrSkipped) || r.ans.Status != ask.StatusSkipped || r.ans.AskID != p.AskID {
		t.Fatalf("skip result = %+v, %v", r.ans, r.err)
	}
	if c := payloadOf(t, nextKind(t, run, wire.DataKindAskClosed)); c.Status != "skipped" {
		t.Fatalf("askClosed = %+v", c)
	}
	drainRun(run)
}

func TestAskInvalidThenValid(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	resp := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSubmit, Values: map[string]any{"day": "not-a-date"}})
	if resp.Accepted || resp.Reason != wire.InputReasonInvalid || resp.Errors["day"] == "" {
		t.Fatalf("invalid submit reply = %+v", resp)
	}
	if resp := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: "maybe"}); resp.Accepted || resp.Reason != wire.InputReasonInvalid {
		t.Fatalf("unknown action reply = %+v", resp)
	}
	select {
	case r := <-results:
		t.Fatalf("AskUser returned after an invalid answer: %+v", r)
	default:
	}
	if resp := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSubmit, Values: map[string]any{"day": "2026-01-02"}}); !resp.Accepted {
		t.Fatalf("valid submit rejected: %+v", resp)
	}
	if r := waitResult(t, results); r.err != nil || r.ans.Values["day"] != "2026-01-02" {
		t.Fatalf("result = %+v, %v", r.ans, r.err)
	}
	drainRun(run)
}

func TestAskUnknownAndWrongRun(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	if resp := answer(t, run, wire.InputRequest{AskID: "ask_other", Action: wire.InputActionSkip}); resp.Reason != wire.InputReasonUnknownAsk {
		t.Fatalf("other askId reply = %+v", resp)
	}
	if resp := answer(t, run, wire.InputRequest{RunID: "run_other", AskID: p.AskID, Action: wire.InputActionSkip}); resp.Reason != wire.InputReasonUnknownAsk {
		t.Fatalf("other runId reply = %+v", resp)
	}
	answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSkip})
	waitResult(t, results)
	drainRun(run)
}

// Review Focus 2: two answers at once, only one wins.
func TestAskDoubleSubmitSecondClosed(t *testing.T) {
	hold := make(chan struct{})
	a, results := startAskAgent(t, askAgentOpts{userInput: true, hold: hold})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	nc := streamConn(t)
	var wg sync.WaitGroup
	replies := make([]wire.InputResponse, 2)
	errs := make([]error, 2)
	for i := range replies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			replies[i], errs[i] = answerErr(nc, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSubmit, Values: map[string]any{"day": "2026-11-22"}})
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	accepted, closed := 0, 0
	for _, r := range replies {
		switch {
		case r.Accepted:
			accepted++
		case r.Reason == wire.InputReasonClosed:
			closed++
		}
	}
	if accepted != 1 || closed != 1 {
		t.Fatalf("replies = %+v, want one accepted and one closed", replies)
	}
	if r := waitResult(t, results); r.err != nil {
		t.Fatalf("AskUser err = %v", r.err)
	}
	close(hold)
	drainRun(run)
}

func TestAskParksAfterWait(t *testing.T) {
	results := make(chan askResult, 1)
	parkNow := make(chan struct{})
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, wait: 200 * time.Millisecond, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		ans, err := s.AskUser(ctx, testAsk)
		results <- askResult{ans, err}
		<-parkNow
		if errors.Is(err, ErrParked) {
			s.Park(ans.AskID)
		}
		return nil
	}})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	if p.WaitMs != 200 {
		t.Fatalf("waitMs = %d, want 200", p.WaitMs)
	}
	r := waitResult(t, results)
	if !errors.Is(r.err, ErrParked) || r.ans.AskID != p.AskID {
		t.Fatalf("result = %+v, %v; want ErrParked", r.ans, r.err)
	}
	if resp := answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSkip}); resp.Accepted || resp.Reason != wire.InputReasonClosed {
		t.Fatalf("late answer reply = %+v", resp)
	}
	close(parkNow)
	c := payloadOf(t, nextKind(t, run, wire.DataKindAskClosed))
	if c.AskID != p.AskID || c.Status != "parked" {
		t.Fatalf("askClosed = %+v", c)
	}
	evs := drainRun(run)
	last := evs[len(evs)-1]
	if last.Type != wire.EventDone || last.Reason != wire.ReasonAwaitingInput || last.StopReason != wire.StopEndTurn {
		t.Fatalf("terminal = %+v, want done/awaiting_input", last)
	}
}

// Review Focus 3: a submit landing at the wait boundary is either accepted
// (AskUser returns it) or refused as closed (AskUser parks): never both,
// never neither.
func TestAskSubmitRacingParkExactlyOnce(t *testing.T) {
	const n = 200
	const wait = 20 * time.Millisecond
	results := make(chan askResult, 1)
	release := make(chan struct{})
	// One run asks n times in a row.
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, wait: wait, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		for range n {
			ans, err := s.AskUser(ctx, testAsk)
			results <- askResult{ans, err}
		}
		<-release // keep the input subject until the last reply is in
		return nil
	}})
	run := chatInput(t, a, true)
	nc := streamConn(t)
	live, parked := 0, 0
	closedCount := map[string]int{}
	countClosed := func(ev wire.AgentEvent) {
		if ev.Kind == wire.DataKindAskClosed {
			closedCount[payloadOf(t, ev).AskID]++
		}
	}
	var asked []string
	for i := range n {
		var p askEventPayload
		for p.AskID == "" {
			ev, ok := <-run.Events
			if !ok || ev.IsTerminal() {
				t.Fatalf("iteration %d: stream ended: %+v", i, ev)
			}
			countClosed(ev)
			if ev.Kind == wire.DataKindAsk {
				p = payloadOf(t, ev)
			}
		}
		asked = append(asked, p.AskID)
		// Sweep the send time across the expiry (0–29ms vs a 20ms wait,
		// counted from receipt of the ask event).
		time.Sleep(time.Duration(i%30) * time.Millisecond)
		resp := answerOn(t, nc, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSubmit, Values: map[string]any{"day": "2026-11-22"}})
		r := waitResult(t, results)
		if r.ans.AskID != p.AskID {
			t.Fatalf("iteration %d: result for %q, want %q", i, r.ans.AskID, p.AskID)
		}
		switch {
		case resp.Accepted && r.err == nil && r.ans.Values["day"] == "2026-11-22":
			live++
		case !resp.Accepted && resp.Reason == wire.InputReasonClosed && errors.Is(r.err, ErrParked):
			parked++
		default:
			t.Fatalf("iteration %d: reply %+v with result %+v, %v", i, resp, r.ans, r.err)
		}
	}
	t.Logf("live %d, parked %d", live, parked)
	close(release)
	// Every ask is closed exactly once, parked ones included.
	for _, ev := range drainRun(run) {
		countClosed(ev)
	}
	for _, id := range asked {
		if closedCount[id] != 1 {
			t.Fatalf("ask %s closed %d times, want 1", id, closedCount[id])
		}
	}
}

func TestAskSecondConcurrentBusy(t *testing.T) {
	second := make(chan error, 1)
	results := make(chan askResult, 2)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		go func() {
			ans, err := s.AskUser(ctx, testAsk)
			results <- askResult{ans, err}
		}()
		for {
			if _, ok := s.OpenAsk(); ok {
				break
			}
			time.Sleep(time.Millisecond)
		}
		_, err := s.AskUser(ctx, testAsk)
		second <- err
		for {
			if _, ok := s.OpenAsk(); !ok {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
	}})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	select {
	case err := <-second:
		if !errors.Is(err, ErrBusy) {
			t.Fatalf("second ask err = %v, want ErrBusy", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second AskUser never returned")
	}
	answer(t, run, wire.InputRequest{AskID: p.AskID, Action: wire.InputActionSkip})
	if r := waitResult(t, results); !errors.Is(r.err, ErrSkipped) {
		t.Fatalf("first ask err = %v", r.err)
	}
	drainRun(run)
}

func TestInterruptAskSuperseded(t *testing.T) {
	streams := make(chan *Stream, 1)
	results := make(chan askResult, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		streams <- s
		ans, err := s.AskUser(ctx, testAsk)
		results <- askResult{ans, err}
		return nil
	}})
	run := chatInput(t, a, true)
	p := payloadOf(t, nextKind(t, run, wire.DataKindAsk))
	s := <-streams
	if id, ok := s.OpenAsk(); !ok || id != p.AskID {
		t.Fatalf("OpenAsk = %q, %v", id, ok)
	}
	if !s.InterruptAsk("use last week instead") {
		t.Fatal("InterruptAsk = false with an open ask")
	}
	if s.InterruptAsk("again") {
		t.Fatal("InterruptAsk = true with no open ask")
	}
	if _, ok := s.OpenAsk(); ok {
		t.Fatal("OpenAsk still true after interrupt")
	}
	r := waitResult(t, results)
	if !errors.Is(r.err, ErrSuperseded) || r.ans.Values["text"] != "use last week instead" || r.ans.AskID != p.AskID {
		t.Fatalf("result = %+v, %v", r.ans, r.err)
	}
	if c := payloadOf(t, nextKind(t, run, wire.DataKindAskClosed)); c.Status != "superseded" {
		t.Fatalf("askClosed = %+v", c)
	}
	drainRun(run)
}

func TestAskCtxCancelled(t *testing.T) {
	a, results := startAskAgent(t, askAgentOpts{userInput: true})
	run := chatInput(t, a, true)
	nextKind(t, run, wire.DataKindAsk)
	if _, err := run.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	r := waitResult(t, results)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", r.err)
	}
	// The card must not stay open: askClosed{parked}, then the terminal.
	evs := drainRun(run)
	if kinds := dataKinds(evs); len(kinds) != 1 || kinds[0] != wire.DataKindAskClosed {
		t.Fatalf("data events after cancel = %v, want [askClosed]", kinds)
	}
	assertParkedThenTerminal(t, evs, wire.EventDone)
}

// assertParkedThenTerminal: the last two events are askClosed{parked} and a
// terminal of type term.
func assertParkedThenTerminal(t *testing.T, evs []wire.AgentEvent, term string) {
	t.Helper()
	if len(evs) < 2 {
		t.Fatalf("events = %+v", evs)
	}
	closed, last := evs[len(evs)-2], evs[len(evs)-1]
	if closed.Kind != wire.DataKindAskClosed || payloadOf(t, closed).Status != "parked" {
		t.Fatalf("second to last event = %+v, want askClosed{parked}", closed)
	}
	if last.Type != term {
		t.Fatalf("terminal = %+v, want %s", last, term)
	}
}

// An expired ask whose handler never called Park is still closed as parked
// before the terminal event, whoever emits it.
func TestAskParkedWithoutParkClosedAtRunEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(s *Stream)
		term string
	}{
		{"runtimeFallback", func(*Stream) {}, wire.EventDone},
		{"handlerDone", func(s *Stream) { s.Done(wire.StopEndTurn, nil) }, wire.EventDone},
		{"handlerError", func(s *Stream) { s.Error("boom", wire.CodeInternal) }, wire.EventError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := make(chan askResult, 1)
			a, _ := startAskAgent(t, askAgentOpts{userInput: true, wait: 50 * time.Millisecond, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
				ans, err := s.AskUser(ctx, testAsk)
				results <- askResult{ans, err}
				tc.end(s)
				return nil
			}})
			run := chatInput(t, a, true)
			if r := waitResult(t, results); !errors.Is(r.err, ErrParked) {
				t.Fatalf("err = %v, want ErrParked", r.err)
			}
			evs := drainRun(run)
			n := 0
			for _, k := range dataKinds(evs) {
				if k == wire.DataKindAskClosed {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("askClosed events = %d, want 1", n)
			}
			assertParkedThenTerminal(t, evs, tc.term)
		})
	}
}

// Park after ErrParked emits exactly one askClosed (no second one at run end).
func TestAskParkEmitsOnce(t *testing.T) {
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, wait: 50 * time.Millisecond, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		if ans, err := s.AskUser(ctx, testAsk); errors.Is(err, ErrParked) {
			s.Park(ans.AskID)
		}
		return nil
	}})
	evs := drainRun(chatInput(t, a, true))
	if kinds := dataKinds(evs); len(kinds) != 2 || kinds[1] != wire.DataKindAskClosed {
		t.Fatalf("data events = %v, want [ask askClosed]", kinds)
	}
	assertParkedThenTerminal(t, evs, wire.EventDone)
}

func TestAskOnTerminalStream(t *testing.T) {
	results := make(chan askResult, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		s.Done(wire.StopEndTurn, nil)
		start := time.Now()
		ans, err := s.AskUser(ctx, testAsk)
		if time.Since(start) > time.Second {
			err = fmt.Errorf("AskUser waited %s on an ended stream", time.Since(start))
		}
		results <- askResult{ans, err}
		return nil
	}})
	run := chatInput(t, a, true)
	if r := waitResult(t, results); !errors.Is(r.err, ErrRunEnded) {
		t.Fatalf("err = %v, want ErrRunEnded", r.err)
	}
	if kinds := dataKinds(drainRun(run)); len(kinds) != 0 {
		t.Fatalf("data events %v, want none", kinds)
	}
}

func TestAskMalformedNoEvent(t *testing.T) {
	results := make(chan askResult, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, s *Stream) error {
		ans, err := s.AskUser(ctx, ask.Ask{Title: "x", Fields: []ask.Field{{Name: "a", Type: "bogus"}}})
		results <- askResult{ans, err}
		return nil
	}})
	run := chatInput(t, a, true)
	r := waitResult(t, results)
	if r.err == nil || errors.Is(r.err, ErrUnsupported) {
		t.Fatalf("err = %v, want a check error", r.err)
	}
	if kinds := dataKinds(drainRun(run)); len(kinds) != 0 {
		t.Fatalf("data events %v, want none", kinds)
	}
}

func TestInputsValidatedOnTurn(t *testing.T) {
	turns := make(chan *Turn, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(_ context.Context, turn *Turn, _ *Stream) error {
		turns <- turn
		return nil
	}})
	raw, _ := json.Marshal(testAsk)
	run := chatInput(t, a, true,
		wire.AskInput{AskID: "ask_good", Action: wire.InputActionSubmit, Values: map[string]any{"day": "2026-11-22"}, Ask: raw},
		wire.AskInput{AskID: "ask_skip", Action: wire.InputActionSkip, Ask: raw},
		wire.AskInput{AskID: "ask_badvalues", Action: wire.InputActionSubmit, Values: map[string]any{"day": "nope"}, Ask: raw},
		wire.AskInput{AskID: "ask_badaction", Action: "maybe", Ask: raw},
		wire.AskInput{AskID: "ask_badask", Action: wire.InputActionSkip, Ask: json.RawMessage(`{"title":"x","fields":[{"name":"a","type":"bogus"}]}`)},
		wire.AskInput{AskID: "ask_noask", Action: wire.InputActionSkip},
		wire.AskInput{AskID: "", Action: wire.InputActionSkip, Ask: raw},
	)
	turn := waitTurn(t, turns)
	drainRun(run)
	if len(turn.ValidInputs) != 2 {
		t.Fatalf("ValidInputs = %+v, want 2", turn.ValidInputs)
	}
	good, skip := turn.ValidInputs[0], turn.ValidInputs[1]
	if good.AskID != "ask_good" || good.Answer.Display["day"] != "Nov 22, 2026" || good.Ask.Title != testAsk.Title || good.Answer.AskID != "ask_good" {
		t.Fatalf("good input = %+v", good)
	}
	if skip.AskID != "ask_skip" || skip.Answer.Status != ask.StatusSkipped {
		t.Fatalf("skip input = %+v", skip)
	}
}

func TestInputsIgnoredWithoutCardFlag(t *testing.T) {
	turns := make(chan *Turn, 1)
	a, _ := startAskAgent(t, askAgentOpts{userInput: false, handler: func(_ context.Context, turn *Turn, _ *Stream) error {
		turns <- turn
		return nil
	}})
	raw, _ := json.Marshal(testAsk)
	run := chatInput(t, a, true, wire.AskInput{AskID: "ask_good", Action: wire.InputActionSkip, Ask: raw})
	turn := waitTurn(t, turns)
	drainRun(run)
	if len(turn.ValidInputs) != 0 {
		t.Fatalf("ValidInputs = %+v without Config.UserInput", turn.ValidInputs)
	}
}

func TestAckHasNoInputSubjectForV16Caller(t *testing.T) {
	release := make(chan struct{})
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, _ *Stream) error {
		<-release
		return nil
	}})
	subsBefore := a.nc.NumSubscriptions()
	run := chatInput(t, a, false)
	if run.Ack.Caps != nil {
		t.Fatalf("ack caps = %+v, want none for a v1.6 caller", run.Ack.Caps)
	}
	if n := a.nc.NumSubscriptions(); n != subsBefore {
		t.Fatalf("subscriptions %d → %d for a v1.6 caller", subsBefore, n)
	}
	close(release)
	drainRun(run)
}

func TestInputSubjectClosedAfterRun(t *testing.T) {
	release := make(chan struct{})
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, handler: func(ctx context.Context, _ *Turn, _ *Stream) error {
		<-release
		return nil
	}})
	subsBefore := a.nc.NumSubscriptions()
	run := chatInput(t, a, true)
	if n := a.nc.NumSubscriptions(); n != subsBefore+1 {
		t.Fatalf("subscriptions %d → %d, want one input subscription", subsBefore, n)
	}
	close(release)
	drainRun(run)
	deadline := time.Now().Add(2 * time.Second)
	for a.nc.NumSubscriptions() != subsBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := a.nc.NumSubscriptions(); n != subsBefore {
		t.Fatalf("input subscription leaked: %d → %d", subsBefore, n)
	}
	data, _ := json.Marshal(wire.InputRequest{RunID: run.Ack.RunID, AskID: "ask_x", Action: wire.InputActionSkip})
	if _, err := streamConn(t).Request(inputSubject(run), data, 300*time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("input after run end: err = %v, want a timeout (no reply)", err)
	}
}

func TestAskWaitResolution(t *testing.T) {
	cases := []struct {
		cfg  time.Duration
		env  string
		want time.Duration
	}{
		{0, "", 5 * time.Minute},
		{0, "2s", 2 * time.Second},
		{0, "nonsense", 5 * time.Minute},
		{0, "-1s", 5 * time.Minute},
		{3 * time.Second, "2s", 3 * time.Second},
	}
	for _, c := range cases {
		t.Setenv("AGENT_ASK_WAIT", c.env)
		if got := resolveAskWait(c.cfg); got != c.want {
			t.Errorf("resolveAskWait(%s, env %q) = %s, want %s", c.cfg, c.env, got, c.want)
		}
	}
}
