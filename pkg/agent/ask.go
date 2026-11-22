package agent

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/ask"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// Errors returned by Stream.AskUser.
var (
	// ErrUnsupported: this run cannot ask (the agent or the caller did not
	// opt in). Returned at once, with no event and no wait.
	ErrUnsupported = errors.New("asking the user is not supported in this run")
	// ErrSkipped: the user skipped the ask. Returned with the Answer.
	ErrSkipped = errors.New("the user skipped the question")
	// ErrParked: the live wait ran out. End the turn with Stream.Park.
	ErrParked = errors.New("no answer within the live wait; the ask is parked")
	// ErrBusy: another ask of this run is still open.
	ErrBusy = errors.New("another ask is already open in this run")
	// ErrSuperseded: the user sent a message instead of answering
	// (Stream.InterruptAsk). The Answer's Values["text"] carries it.
	ErrSuperseded = errors.New("the user replied with a message instead")
)

// askClosed statuses (data event askClosed).
const (
	askClosedAnswered   = "answered"
	askClosedSkipped    = "skipped"
	askClosedParked     = "parked"
	askClosedSuperseded = "superseded"
)

// StatusSuperseded is the Answer.Status AskUser returns with ErrSuperseded.
const StatusSuperseded = "superseded"

// defaultAskWait is the live wait when neither Config.AskWait nor
// AGENT_ASK_WAIT sets one.
const defaultAskWait = 5 * time.Minute

// resolveAskWait: cfg when > 0, else env AGENT_ASK_WAIT (a positive
// time.ParseDuration), else 5m.
func resolveAskWait(cfg time.Duration) time.Duration {
	if cfg > 0 {
		return cfg
	}
	if v := strings.TrimSpace(os.Getenv("AGENT_ASK_WAIT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultAskWait
}

// ValidInput is one ChatRequest.inputs entry that passed validation: the
// original ask and the answer built from it.
type ValidInput struct {
	wire.AskInput
	Ask    ask.Ask
	Answer ask.Answer
}

// validInputs checks each entry of inputs: the ask must parse and pass
// CheckAsk, the action must be submit or skip, and submitted values must
// validate. Bad entries are logged and dropped.
func validInputs(inputs []wire.AskInput, logger *log.Logger) []ValidInput {
	var out []ValidInput
	for _, in := range inputs {
		v, reason := validInput(in)
		if reason != "" {
			logger.Printf("dropping input askId=%q: %s", in.AskID, reason)
			continue
		}
		out = append(out, v)
	}
	return out
}

func validInput(in wire.AskInput) (ValidInput, string) {
	if in.AskID == "" {
		return ValidInput{}, "no askId"
	}
	if len(in.Ask) == 0 {
		return ValidInput{}, "no ask"
	}
	var a ask.Ask
	if err := json.Unmarshal(in.Ask, &a); err != nil {
		return ValidInput{}, "ask does not parse: " + err.Error()
	}
	if err := ask.CheckAsk(a); err != nil {
		return ValidInput{}, "ask is invalid: " + err.Error()
	}
	switch in.Action {
	case wire.InputActionSkip:
		return ValidInput{AskInput: in, Ask: a, Answer: ask.Answer{AskID: in.AskID, Status: ask.StatusSkipped}}, ""
	case wire.InputActionSubmit:
		if errs := ask.Validate(a, in.Values); len(errs) > 0 {
			return ValidInput{}, "values are invalid: " + errs[0].Field + ": " + errs[0].Message
		}
		return ValidInput{AskInput: in, Ask: a, Answer: ask.BuildAnswer(a, in.AskID, in.Values)}, ""
	}
	return ValidInput{}, "unknown action " + in.Action
}

// askOutcome is how an open ask was closed by someone other than AskUser.
type askOutcome struct {
	ans    ask.Answer
	err    error
	status string // askClosed status
}

// openAsk is the one ask of a run that may be open.
type openAsk struct {
	id     string
	ask    ask.Ask
	closed bool
	done   chan askOutcome // buffered 1; sent once, by the closer
}

// asker is a run's ask state. One mutex guards it; the first close
// (submit, skip, interrupt, wait expiry, ctx done) wins and every later one
// sees closed.
type asker struct {
	runID  string
	origin string
	wait   time.Duration
	logger *log.Logger

	mu     sync.Mutex
	cur    *openAsk
	closed map[string]bool // ids of asks closed earlier in this run
}

func newAskID() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "ask_" + base64.RawURLEncoding.EncodeToString(b), nil
}

// newInputSubject is a run's private input subject:
// trx.agent.<name>.input.<22 random base64url chars>.
func newInputSubject(agentName string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return wire.AgentSubject(agentName, "input."+base64.RawURLEncoding.EncodeToString(b)), nil
}

// subscribeInput opens turn's private input subject on this instance's
// connection. The caller unsubscribes when the run ends.
func (a *Agent) subscribeInput(turn *Turn) (string, *asker, *nats.Subscription, error) {
	subject, err := newInputSubject(a.cfg.Name)
	if err != nil {
		return "", nil, nil, err
	}
	logger := turn.Logger
	if logger == nil {
		logger = log.Default()
	}
	k := &asker{runID: turn.RunID, origin: a.cfg.Name, wait: a.cfg.AskWait, logger: logger, closed: map[string]bool{}}
	sub, err := a.nc.Subscribe(subject, func(m *nats.Msg) {
		resp := k.apply(m.Data)
		if m.Reply == "" {
			return
		}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	if err != nil {
		return "", nil, nil, err
	}
	return subject, k, sub, nil
}

func refuse(reason string) wire.InputResponse {
	return wire.InputResponse{Accepted: false, Reason: reason}
}

// apply decides one InputRequest.
func (k *asker) apply(body []byte) wire.InputResponse {
	var req wire.InputRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return refuse(wire.InputReasonInvalid)
	}
	if req.RunID != k.runID {
		return refuse(wire.InputReasonUnknownAsk)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	cur := k.cur
	if cur == nil || cur.id != req.AskID {
		if k.closed[req.AskID] {
			return refuse(wire.InputReasonClosed)
		}
		return refuse(wire.InputReasonUnknownAsk)
	}
	if cur.closed {
		return refuse(wire.InputReasonClosed)
	}
	var out askOutcome
	switch req.Action {
	case wire.InputActionSkip:
		out = askOutcome{ans: ask.Answer{AskID: cur.id, Status: ask.StatusSkipped}, err: ErrSkipped, status: askClosedSkipped}
	case wire.InputActionSubmit:
		if errs := ask.Validate(cur.ask, req.Values); len(errs) > 0 {
			resp := refuse(wire.InputReasonInvalid)
			resp.Errors = map[string]string{}
			for _, e := range errs {
				if _, dup := resp.Errors[e.Field]; !dup {
					resp.Errors[e.Field] = e.Message
				}
			}
			return resp
		}
		out = askOutcome{ans: ask.BuildAnswer(cur.ask, cur.id, req.Values), status: askClosedAnswered}
	default:
		return refuse(wire.InputReasonInvalid)
	}
	k.closeLocked(cur, out)
	return wire.InputResponse{Accepted: true}
}

// closeLocked marks cur closed and hands out to the waiting AskUser.
// k.mu must be held and cur open.
func (k *asker) closeLocked(cur *openAsk, out askOutcome) {
	cur.closed = true
	k.closed[cur.id] = true
	cur.done <- out
}

// claim closes cur on AskUser's own side (wait expiry or ctx done); false
// when another closer won, whose outcome is then on cur.done.
func (k *asker) claim(cur *openAsk) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if cur.closed {
		return false
	}
	cur.closed = true
	k.closed[cur.id] = true
	return true
}

// AskUser shows the user a form and waits for the answer. It returns
//   - ErrUnsupported at once when the run cannot ask (no event);
//   - the CheckAsk error when the ask is malformed (no event): report it to
//     the model as a tool error;
//   - ErrBusy when another ask of this run is open;
//   - the answer (nil error) on submit, ErrSkipped (with the answer) on skip,
//     ErrSuperseded (Values["text"]) on InterruptAsk;
//   - ErrParked after the live wait: end the turn with Park;
//   - ctx.Err() when the run ctx ends first.
//
// The ask goes out as data/ask {askId, waitMs, origin, ask}; every close but
// ctx done and the wait expiry emits data/askClosed {askId, status} (Park
// emits it for the expiry).
func (s *Stream) AskUser(ctx context.Context, a ask.Ask) (ask.Answer, error) {
	k := s.asker
	if k == nil {
		return ask.Answer{}, ErrUnsupported
	}
	if err := ask.CheckAsk(a); err != nil {
		return ask.Answer{}, err
	}
	id, err := newAskID()
	if err != nil {
		return ask.Answer{}, err
	}
	cur := &openAsk{id: id, ask: a, done: make(chan askOutcome, 1)}
	k.mu.Lock()
	if k.cur != nil && !k.cur.closed {
		k.mu.Unlock()
		return ask.Answer{}, ErrBusy
	}
	k.cur = cur
	k.mu.Unlock()

	timer := time.NewTimer(k.wait)
	defer timer.Stop()
	s.Data(wire.DataKindAsk, struct {
		AskID  string  `json:"askId"`
		WaitMs int64   `json:"waitMs"`
		Origin string  `json:"origin"`
		Ask    ask.Ask `json:"ask"`
	}{id, k.wait.Milliseconds(), k.origin, a})

	var out askOutcome
	select {
	case out = <-cur.done:
	case <-timer.C:
		if k.claim(cur) {
			return ask.Answer{AskID: id}, ErrParked
		}
		out = <-cur.done
	case <-ctx.Done():
		if k.claim(cur) {
			return ask.Answer{AskID: id}, ctx.Err()
		}
		out = <-cur.done
	}
	s.askClosed(id, out.status)
	return out.ans, out.err
}

func (s *Stream) askClosed(askID, status string) {
	s.Data(wire.DataKindAskClosed, struct {
		AskID  string `json:"askId"`
		Status string `json:"status"`
	}{askID, status})
}

// InterruptAsk closes the open ask as superseded by the user's message text:
// AskUser returns ErrSuperseded with Values["text"] = text. False when no
// ask is open.
func (s *Stream) InterruptAsk(text string) bool {
	k := s.asker
	if k == nil {
		return false
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	cur := k.cur
	if cur == nil || cur.closed {
		return false
	}
	k.closeLocked(cur, askOutcome{
		ans:    ask.Answer{AskID: cur.id, Status: StatusSuperseded, Values: map[string]any{"text": text}},
		err:    ErrSuperseded,
		status: askClosedSuperseded,
	})
	return true
}

// OpenAsk is the id of this run's open ask, if any.
func (s *Stream) OpenAsk() (string, bool) {
	k := s.asker
	if k == nil {
		return "", false
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.cur == nil || k.cur.closed {
		return "", false
	}
	return k.cur.id, true
}

// Park ends the turn after AskUser returned ErrParked: it emits
// askClosed{parked} and done with reason awaiting_input.
func (s *Stream) Park(askID string) {
	s.askClosed(askID, askClosedParked)
	s.DoneWithReason(wire.StopEndTurn, wire.ReasonAwaitingInput, nil)
}
