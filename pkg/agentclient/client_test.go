package agentclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"

	"github.com/transactrx/nats-agent/internal/progressclock"
	"github.com/transactrx/nats-agent/pkg/progress"
	"github.com/transactrx/nats-agent/pkg/toolclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// One embedded server for this package's tests.
var (
	embedOnce sync.Once
	embedURL  string
)

func testConn(t *testing.T) *nats.Conn {
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

// respondOK replies the way a nats-service endpoint does on success.
func respondOK(t *testing.T, m *nats.Msg, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		t.Errorf("marshal reply: %v", err)
		return
	}
	reply := &nats.Msg{Subject: m.Reply, Data: data, Header: nats.Header{}}
	reply.Header.Set(nats_service_common.STATUS, "200")
	if err := m.RespondMsg(reply); err != nil {
		t.Errorf("respond: %v", err)
	}
}

// fakeAgent answers chat on a unique agent name. script runs after the ack
// and publishes events on the stream subject.
type fakeAgent struct {
	name    string
	nc      *nats.Conn
	caps    *wire.AckCaps
	script  func(publish func(wire.AgentEvent))
	mu      sync.Mutex
	idts    []string
	cancels chan string
	reasons chan string // reason of each cancel, same order as cancels
}

var agentSeq atomic.Int64

func startFakeAgent(t *testing.T, caps *wire.AckCaps, script func(publish func(wire.AgentEvent))) *fakeAgent {
	t.Helper()
	nc := testConn(t)
	f := &fakeAgent{
		name:    fmt.Sprintf("fake%d", agentSeq.Add(1)),
		nc:      nc,
		caps:    caps,
		script:  script,
		cancels: make(chan string, 4),
		reasons: make(chan string, 4),
	}
	const runID = "run-1"
	if _, err := nc.Subscribe(wire.AgentSubject(f.name, "chat"), func(m *nats.Msg) {
		var req wire.ChatRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("chat request: %v", err)
			return
		}
		f.mu.Lock()
		f.idts = append(f.idts, m.Header.Get(wire.HeaderIDT))
		f.mu.Unlock()
		respondOK(t, m, wire.ChatAck{Accepted: true, RunID: runID, Agent: f.name, StreamSubject: req.StreamSubject, Caps: f.caps})
		if f.script != nil {
			go f.script(func(ev wire.AgentEvent) {
				ev.RunID = runID
				data, _ := json.Marshal(ev)
				_ = nc.Publish(req.StreamSubject, data)
			})
		}
	}); err != nil {
		t.Fatalf("subscribe chat: %v", err)
	}
	if _, err := nc.Subscribe(wire.AgentSubject(f.name, "cancel"), func(m *nats.Msg) {
		var req wire.CancelRequest
		_ = json.Unmarshal(m.Data, &req)
		f.reasons <- req.Reason
		f.cancels <- req.RunID
	}); err != nil {
		t.Fatalf("subscribe cancel: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return f
}

func (f *fakeAgent) seenIDTs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.idts...)
}

// progressCaps is the ack of an agent that declares progress (needed for
// the client's idle cut).
var progressCaps = &wire.AckCaps{Progress: true}

func chatReq() wire.ChatRequest {
	return wire.ChatRequest{Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}}}
}

// pingsUntil publishes pings every 20ms until stop closes.
func pingsUntil(stop <-chan struct{}, publish func(wire.AgentEvent)) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			publish(wire.AgentEvent{Type: wire.EventPing})
		}
	}
}

func drain(t *testing.T, run *Run, within time.Duration) []wire.AgentEvent {
	t.Helper()
	var evs []wire.AgentEvent
	deadline := time.After(within)
	for {
		select {
		case ev, ok := <-run.Events:
			if !ok {
				return evs
			}
			evs = append(evs, ev)
		case <-deadline:
			t.Fatalf("run did not end within %s; got %+v", within, evs)
			return nil
		}
	}
}

func TestChatWithIdleCut(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) { pingsUntil(stop, publish) })
	c := NewFromConn(testConn(t))

	const window = 300 * time.Millisecond
	start := time.Now()
	run, err := c.ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: window})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	evs := drain(t, run, 5*time.Second)
	elapsed := time.Since(start)
	if len(evs) != 1 {
		t.Fatalf("want only the terminal event (pings not delivered), got %+v", evs)
	}
	ev := evs[0]
	if ev.Type != wire.EventError || ev.Reason != wire.ReasonProgressIdle || ev.RunID != "run-1" {
		t.Fatalf("terminal event = %+v", ev)
	}
	if !strings.Contains(ev.Error, window.String()) {
		t.Fatalf("message should name the window: %q", ev.Error)
	}
	if elapsed < window {
		t.Fatalf("cut too early: %s < %s", elapsed, window)
	}
	select {
	case id := <-f.cancels:
		if id != "run-1" {
			t.Fatalf("cancel for %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no cancel published on idle cut")
	}
}

func TestChatWithIdleCutNamesStage(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) {
		payload, _ := json.Marshal(wire.ProgressPayload{Stage: "searching"})
		publish(wire.AgentEvent{Type: wire.EventData, Kind: wire.DataKindProgress, Payload: payload})
		pingsUntil(stop, publish)
	})
	c := NewFromConn(testConn(t))

	run, err := c.ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	evs := drain(t, run, 5*time.Second)
	last := evs[len(evs)-1]
	if last.Reason != wire.ReasonProgressIdle || !strings.Contains(last.Error, "searching") {
		t.Fatalf("terminal event = %+v", last)
	}
}

func TestChatWithIdleExtendsOnProgress(t *testing.T) {
	const window = 200 * time.Millisecond
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) {
		// Progress every 80ms for 3x the window, pings in between, then done.
		end := time.Now().Add(3 * window)
		for time.Now().Before(end) {
			publish(wire.AgentEvent{Type: wire.EventPing})
			time.Sleep(40 * time.Millisecond)
			publish(wire.AgentEvent{Type: wire.EventText, TextDelta: "x"})
			time.Sleep(40 * time.Millisecond)
		}
		publish(wire.AgentEvent{Type: wire.EventDone, StopReason: "end_turn"})
	})
	c := NewFromConn(testConn(t))

	run, err := c.ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: window})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	evs := drain(t, run, 5*time.Second)
	last := evs[len(evs)-1]
	if last.Type != wire.EventDone {
		t.Fatalf("want done, got %+v", last)
	}
	for _, ev := range evs {
		if ev.Type == wire.EventPing {
			t.Fatal("ping delivered to caller")
		}
	}
	select {
	case <-f.cancels:
		t.Fatal("cancel published although progress kept the run alive")
	default:
	}
}

func TestChatUnchangedWithoutOptions(t *testing.T) {
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) {
		// Only pings for a while: no idle window, so no cut.
		end := time.Now().Add(400 * time.Millisecond)
		for time.Now().Before(end) {
			publish(wire.AgentEvent{Type: wire.EventPing})
			time.Sleep(20 * time.Millisecond)
		}
		publish(wire.AgentEvent{Type: wire.EventDone, StopReason: "end_turn"})
	})
	c := NewFromConn(testConn(t))

	run, err := c.Chat(context.Background(), f.name, chatReq())
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if run.Caps != (wire.AckCaps{}) {
		t.Fatalf("caps on old agent = %+v", run.Caps)
	}
	evs := drain(t, run, 5*time.Second)
	if len(evs) != 1 || evs[0].Type != wire.EventDone {
		t.Fatalf("want only done, got %+v", evs)
	}
}

func TestRunCapsFromAck(t *testing.T) {
	caps := &wire.AckCaps{Progress: true, RenewSubject: "trx.agent.x.renew.abc"}
	f := startFakeAgent(t, caps, func(publish func(wire.AgentEvent)) {
		publish(wire.AgentEvent{Type: wire.EventDone})
	})
	run, err := NewFromConn(testConn(t)).Chat(context.Background(), f.name, chatReq())
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if run.Caps != *caps {
		t.Fatalf("run.Caps = %+v, want %+v", run.Caps, *caps)
	}
	drain(t, run, 5*time.Second)
}

func TestRenewUnsupported(t *testing.T) {
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) {
		publish(wire.AgentEvent{Type: wire.EventDone})
	})
	run, err := NewFromConn(testConn(t)).Chat(context.Background(), f.name, chatReq())
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	drain(t, run, 5*time.Second)
	if err := run.Renew(context.Background(), "tok"); !errors.Is(err, ErrRenewUnsupported) {
		t.Fatalf("Renew = %v, want ErrRenewUnsupported", err)
	}
	if err := Renew(context.Background(), testConn(t), "", wire.RenewRequest{RunID: "r", IDT: "tok"}); !errors.Is(err, ErrRenewUnsupported) {
		t.Fatalf("package Renew = %v, want ErrRenewUnsupported", err)
	}
}

// renewResponder answers RenewRequests on subject with resp and records them.
func renewResponder(t *testing.T, subject string, resp wire.RenewResponse) chan wire.RenewRequest {
	t.Helper()
	nc := testConn(t)
	got := make(chan wire.RenewRequest, 4)
	if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var req wire.RenewRequest
		_ = json.Unmarshal(m.Data, &req)
		got <- req
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	}); err != nil {
		t.Fatalf("subscribe renew: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return got
}

func TestRenewRejected(t *testing.T) {
	subject := wire.AgentSubject("agentR", "renew.Q2x0Yk9fX-abcdefghijk")
	got := renewResponder(t, subject, wire.RenewResponse{Applied: false, Reason: "identity_mismatch"})
	run := &Run{Ack: wire.ChatAck{RunID: "run-9"}, Caps: wire.AckCaps{RenewSubject: subject}, client: NewFromConn(testConn(t)), agent: "agentR"}

	err := run.Renew(context.Background(), "new-token")
	if !errors.Is(err, ErrRenewRejected) || !strings.Contains(err.Error(), "identity_mismatch") {
		t.Fatalf("Renew = %v, want ErrRenewRejected with reason", err)
	}
	if req := <-got; req.RunID != "run-9" || req.IDT != "new-token" || req.ExpiresAtMs != 0 {
		t.Fatalf("request = %+v", req)
	}
}

func TestRenewApplied(t *testing.T) {
	subject := wire.AgentSubject("agentR", "renew.Q2x0Yk9fX-abcdefghijk")
	got := renewResponder(t, subject, wire.RenewResponse{Applied: true})
	run := &Run{Ack: wire.ChatAck{RunID: "run-9"}, Caps: wire.AckCaps{RenewSubject: subject}, client: NewFromConn(testConn(t)), agent: "agentR"}

	exp := time.UnixMilli(1_900_000_000_123)
	if err := run.RenewWithExpiry(context.Background(), "new-token", exp); err != nil {
		t.Fatalf("RenewWithExpiry: %v", err)
	}
	if req := <-got; req.RunID != "run-9" || req.IDT != "new-token" || req.ExpiresAtMs != 1_900_000_000_123 {
		t.Fatalf("request = %+v", req)
	}
	if err := run.RenewWithExpiry(context.Background(), "t2", time.Time{}); err != nil {
		t.Fatalf("RenewWithExpiry zero: %v", err)
	}
	if req := <-got; req.ExpiresAtMs != 0 {
		t.Fatalf("zero expiry sent as %d", req.ExpiresAtMs)
	}
}

func TestRenewNoResponder(t *testing.T) {
	err := Renew(context.Background(), testConn(t), nats.NewInbox(), wire.RenewRequest{RunID: "r", IDT: "tok"})
	if err == nil || errors.Is(err, ErrRenewRejected) || errors.Is(err, ErrRenewUnsupported) {
		t.Fatalf("Renew with nobody listening = %v, want a distinct transport error", err)
	}
}

func TestIDTSourceReadAtCallTime(t *testing.T) {
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) {
		publish(wire.AgentEvent{Type: wire.EventDone})
	})
	c := NewFromConn(testConn(t))

	var current atomic.Value
	current.Store("token-1")
	ctx := WithIDTSource(WithIDT(context.Background(), "static"), func() string { return current.Load().(string) })

	for _, want := range []string{"token-1", "token-2"} {
		current.Store(want)
		if got := IDTFromContext(ctx); got != want {
			t.Fatalf("IDTFromContext = %q, want %q", got, want)
		}
		run, err := c.Chat(ctx, f.name, chatReq())
		if err != nil {
			t.Fatalf("chat: %v", err)
		}
		drain(t, run, 5*time.Second)
	}
	if got := f.seenIDTs(); len(got) != 2 || got[0] != "token-1" || got[1] != "token-2" {
		t.Fatalf("headers seen = %v", got)
	}

	// An empty source falls back to the WithIDT value.
	current.Store("")
	if got := IDTFromContext(ctx); got != "static" {
		t.Fatalf("fallback = %q, want static", got)
	}
	// No source: unchanged behavior.
	if got := IDTFromContext(WithIDT(context.Background(), "plain")); got != "plain" {
		t.Fatalf("no source = %q", got)
	}
}

func TestIDTSourceToolCall(t *testing.T) {
	nc := testConn(t)
	name := fmt.Sprintf("faketool%d", agentSeq.Add(1))
	var mu sync.Mutex
	var seen []string
	if _, err := nc.Subscribe(wire.ToolSubject(name, "run"), func(m *nats.Msg) {
		mu.Lock()
		seen = append(seen, m.Header.Get(wire.HeaderIDT))
		mu.Unlock()
		respondOK(t, m, wire.ToolRunResponse{})
	}); err != nil {
		t.Fatalf("subscribe tool: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	tc := toolclient.NewFromConn(testConn(t))

	var current atomic.Value
	current.Store("token-1")
	ctx := WithIDTSource(context.Background(), func() string { return current.Load().(string) })
	for _, tok := range []string{"token-1", "token-2"} {
		current.Store(tok)
		if _, err := tc.Run(ctx, name, wire.ToolRunRequest{}, 2*time.Second); err != nil {
			t.Fatalf("tool run: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != "token-1" || seen[1] != "token-2" {
		t.Fatalf("tool headers seen = %v", seen)
	}
}

// When the idle window expires while frames are still queued (the consumer
// was slow), the queued frames win: they are delivered in order and a
// queued terminal ends the run normally, with no idle cut and no cancel.
func TestIdleExpiryPrefersQueuedTerminal(t *testing.T) {
	const texts = 80 // more than the Events buffer, so the reader blocks
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) {
		for i := 0; i < texts; i++ {
			publish(wire.AgentEvent{Type: wire.EventText, TextDelta: fmt.Sprint(i)})
		}
		publish(wire.AgentEvent{Type: wire.EventDone, StopReason: wire.StopEndTurn})
	})
	c := NewFromConn(testConn(t))
	const window = 200 * time.Millisecond
	run, err := c.ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: window})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	time.Sleep(3 * window) // nobody reads: the watcher expires with frames queued
	evs := drain(t, run, 5*time.Second)
	if len(evs) != texts+1 {
		t.Fatalf("got %d events, want %d texts + done; last = %+v", len(evs), texts, evs[len(evs)-1])
	}
	for i := 0; i < texts; i++ {
		if evs[i].Type != wire.EventText || evs[i].TextDelta != fmt.Sprint(i) {
			t.Fatalf("event %d = %+v, want text %d in order", i, evs[i], i)
		}
	}
	if last := evs[texts]; last.Type != wire.EventDone || last.Reason != "" {
		t.Fatalf("terminal = %+v, want the agent's done", last)
	}
	select {
	case id := <-f.cancels:
		t.Fatalf("cancel %q published although the run ended normally", id)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRenewTransportError(t *testing.T) {
	// No responders.
	err := Renew(context.Background(), testConn(t), nats.NewInbox(), wire.RenewRequest{RunID: "r", IDT: "tok"})
	if !errors.Is(err, ErrRenewTransport) || !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("no responders: %v, want ErrRenewTransport wrapping ErrNoResponders", err)
	}
	if errors.Is(err, ErrRenewRejected) || errors.Is(err, ErrRenewUnsupported) {
		t.Fatalf("transport error matches another sentinel: %v", err)
	}
	// Connection error.
	nc := testConn(t)
	nc.Close()
	err = Renew(context.Background(), nc, nats.NewInbox(), wire.RenewRequest{RunID: "r", IDT: "tok"})
	if !errors.Is(err, ErrRenewTransport) || !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("closed conn: %v, want ErrRenewTransport wrapping ErrConnectionClosed", err)
	}
	// Caller cancelled: the ctx error, not the sentinel.
	subject := nats.NewInbox()
	silent := testConn(t)
	if _, err := silent.Subscribe(subject, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	_ = silent.Flush()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	err = Renew(ctx, testConn(t), subject, wire.RenewRequest{RunID: "r", IDT: "tok"})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrRenewTransport) {
		t.Fatalf("cancelled ctx: %v, want context.Canceled without ErrRenewTransport", err)
	}
	// Rejected and unsupported unchanged.
	if err := Renew(context.Background(), testConn(t), "", wire.RenewRequest{}); !errors.Is(err, ErrRenewUnsupported) || errors.Is(err, ErrRenewTransport) {
		t.Fatalf("unsupported: %v", err)
	}
	rs := nats.NewInbox()
	renewResponder(t, rs, wire.RenewResponse{Applied: false, Reason: "wrong_run"})
	if err := Renew(context.Background(), testConn(t), rs, wire.RenewRequest{}); !errors.Is(err, ErrRenewRejected) || errors.Is(err, ErrRenewTransport) {
		t.Fatalf("rejected: %v", err)
	}
}

// End to end: progress frames queued behind a slow reader when the idle
// window expires do not cut the turn, whichever path handles them.
func TestIdleExpiryQueuedProgressEndToEnd(t *testing.T) {
	const texts = 80 // more than the Events buffer, so the reader blocks
	sendDone := make(chan struct{})
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) {
		for i := 0; i < texts; i++ {
			publish(wire.AgentEvent{Type: wire.EventText, TextDelta: fmt.Sprint(i)})
		}
		<-sendDone
		publish(wire.AgentEvent{Type: wire.EventDone, StopReason: wire.StopEndTurn})
	})
	c := NewFromConn(testConn(t))
	const window = 300 * time.Millisecond
	run, err := c.ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: window})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	time.Sleep(2 * window) // nobody reads: the watcher expires with text frames queued
	var evs []wire.AgentEvent
	timeout := time.After(5 * time.Second)
	for len(evs) < texts {
		select {
		case ev, ok := <-run.Events:
			if !ok {
				t.Fatalf("run ended after %d events: %+v", len(evs), evs[len(evs)-1])
			}
			if ev.Type != wire.EventText {
				t.Fatalf("event %d = %+v, want text (no idle cut)", len(evs), ev)
			}
			evs = append(evs, ev)
		case <-timeout:
			t.Fatalf("only %d texts", len(evs))
		}
	}
	close(sendDone)
	rest := drain(t, run, 5*time.Second)
	if len(rest) != 1 || rest[0].Type != wire.EventDone || rest[0].Reason != "" {
		t.Fatalf("after texts got %+v, want the agent's done only", rest)
	}
	for i, ev := range evs {
		if ev.TextDelta != fmt.Sprint(i) {
			t.Fatalf("text %d = %q, out of order", i, ev.TextDelta)
		}
	}
	select {
	case id := <-f.cancels:
		t.Fatalf("cancel %q published although progress was queued", id)
	case <-time.After(200 * time.Millisecond):
	}
}

// Only pings queued at expiry: still idle, the cut happens.
func TestIdleExpiryWithOnlyQueuedPingsCuts(t *testing.T) {
	const texts = 65 // the reader blocks on the last one
	stop := make(chan struct{})
	defer close(stop)
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) {
		for i := 0; i < texts; i++ {
			publish(wire.AgentEvent{Type: wire.EventText, TextDelta: fmt.Sprint(i)})
		}
		pingsUntil(stop, publish)
	})
	c := NewFromConn(testConn(t))
	const window = 200 * time.Millisecond
	run, err := c.ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: window})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	time.Sleep(3 * window) // pings queue up behind the blocked reader
	evs := drain(t, run, 5*time.Second)
	if len(evs) != texts+1 {
		t.Fatalf("got %d events, want %d texts + idle cut", len(evs), texts)
	}
	if last := evs[texts]; last.Type != wire.EventError || last.Reason != wire.ReasonProgressIdle {
		t.Fatalf("terminal = %+v, want progress_idle cut", last)
	}
	select {
	case id := <-f.cancels:
		if id != "run-1" {
			t.Fatalf("cancel for %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no cancel published on idle cut")
	}
}

// testReader builds a stream reader whose idle watcher has already expired.
// readerWindow is the reader tests' idle window on a manual clock.
const readerWindow = 20 * time.Second

// newClockReader builds a stream reader whose watchers run on a manual
// clock; nothing in these tests waits on real time.
func newClockReader(t *testing.T) (r *streamReader, fc *progressclock.Fake, msgs chan *nats.Msg, out chan wire.AgentEvent, cancels *atomic.Int32) {
	t.Helper()
	fc = progressclock.NewFake()
	msgs = make(chan *nats.Msg, 8)
	out = make(chan wire.AgentEvent, 8)
	cancels = &atomic.Int32{}
	r = newStreamReaderWith("run-1", readerWindow, msgs, out, func(w time.Duration) *progress.Watcher {
		return progressclock.NewWatcher(w, fc).(*progress.Watcher)
	})
	r.cancelRemote = func(string) { cancels.Add(1) }
	t.Cleanup(r.stop)
	return r, fc, msgs, out, cancels
}

// testReader is newClockReader with the watcher already expired.
func testReader(t *testing.T) (r *streamReader, msgs chan *nats.Msg, out chan wire.AgentEvent, cancels *atomic.Int32) {
	t.Helper()
	r, fc, msgs, out, cancels := newClockReader(t)
	fc.Advance(readerWindow)
	if !idleReady(r) {
		t.Fatal("watcher did not expire at the window")
	}
	return r, msgs, out, cancels
}

func frame(ev wire.AgentEvent) *nats.Msg {
	ev.RunID = "run-1"
	data, _ := json.Marshal(ev)
	return &nats.Msg{Data: data}
}

func idleReady(r *streamReader) bool {
	select {
	case <-r.idle:
		return true
	default:
		return false
	}
}

// One progress frame queued at expiry, taken by the normal message path:
// the watcher is revived, so the following loop turn cannot cut.
func TestProgressAfterExpiryOnMainPathDoesNotCut(t *testing.T) {
	r, msgs, out, cancels := testReader(t)
	msgs <- frame(wire.AgentEvent{Type: wire.EventText, TextDelta: "a"})
	if r.onMsg(<-msgs) {
		t.Fatal("text ended the run")
	}
	if idleReady(r) {
		t.Fatal("watcher still expired after progress on the main path: next loop turn would cut")
	}
	if r.onMsg(frame(wire.AgentEvent{Type: wire.EventDone, StopReason: wire.StopEndTurn})) != true {
		t.Fatal("done did not end the run")
	}
	close(out)
	var evs []wire.AgentEvent
	for ev := range out {
		evs = append(evs, ev)
	}
	if len(evs) != 2 || evs[0].Type != wire.EventText || evs[1].Type != wire.EventDone || evs[1].Reason != "" {
		t.Fatalf("events = %+v, want text then done", evs)
	}
	if n := cancels.Load(); n != 0 {
		t.Fatalf("cancel published %d times", n)
	}
}

// Progress frames queued at expiry, taken by the idle branch's drain: no
// cut, watcher revived.
func TestIdleExpiryWithQueuedProgressDoesNotCut(t *testing.T) {
	r, msgs, out, cancels := testReader(t)
	msgs <- frame(wire.AgentEvent{Type: wire.EventPing})
	msgs <- frame(wire.AgentEvent{Type: wire.EventText, TextDelta: "a"})
	if r.onIdle() {
		t.Fatal("idle branch ended the run although progress was queued")
	}
	if idleReady(r) {
		t.Fatal("watcher not revived by queued progress")
	}
	if r.onMsg(frame(wire.AgentEvent{Type: wire.EventDone, StopReason: wire.StopEndTurn})) != true {
		t.Fatal("done did not end the run")
	}
	close(out)
	var evs []wire.AgentEvent
	for ev := range out {
		evs = append(evs, ev)
	}
	if len(evs) != 2 || evs[0].Type != wire.EventText || evs[1].Type != wire.EventDone {
		t.Fatalf("events = %+v, want text then done (ping not delivered)", evs)
	}
	if n := cancels.Load(); n != 0 {
		t.Fatalf("cancel published %d times", n)
	}
}

// Only pings queued (or nothing): the idle branch cuts.
func TestIdleBranchCutsWithoutProgress(t *testing.T) {
	for _, queued := range [][]wire.AgentEvent{nil, {{Type: wire.EventPing}, {Type: wire.EventPing}}} {
		r, msgs, out, cancels := testReader(t)
		for _, ev := range queued {
			msgs <- frame(ev)
		}
		if !r.onIdle() {
			t.Fatalf("queued %+v: no cut", queued)
		}
		close(out)
		var evs []wire.AgentEvent
		for ev := range out {
			evs = append(evs, ev)
		}
		if len(evs) != 1 || evs[0].Reason != wire.ReasonProgressIdle {
			t.Fatalf("queued %+v: events = %+v, want one progress_idle cut", queued, evs)
		}
		if cancels.Load() != 1 {
			t.Fatalf("queued %+v: cancels = %d, want 1", queued, cancels.Load())
		}
	}
}

// The window can end between the expiry check and Observe (Observe is then
// a no-op on the expired watcher). The progress frame must still revive it.
func TestProgressAtWindowEdgeRevives(t *testing.T) {
	r, fc, _, out, cancels := newClockReader(t)
	fc.Advance(readerWindow - time.Millisecond)
	if idleReady(r) {
		t.Fatal("expired before the window")
	}
	fired := false
	r.beforeObserve = func() {
		if !fired {
			fired = true
			fc.Advance(time.Millisecond) // the timer fires right at the edge
		}
	}
	if r.onMsg(frame(wire.AgentEvent{Type: wire.EventText, TextDelta: "a"})) {
		t.Fatal("text ended the run")
	}
	if !fired {
		t.Fatal("edge hook not reached")
	}
	if idleReady(r) {
		t.Fatal("progress at the window edge left the watcher expired: next loop turn would cut")
	}
	if ev := <-out; ev.Type != wire.EventText {
		t.Fatalf("delivered %+v", ev)
	}
	// The revived window is a full one from the frame.
	fc.Advance(readerWindow - time.Millisecond)
	if idleReady(r) {
		t.Fatal("revived watcher expired early")
	}
	fc.Advance(time.Millisecond)
	if !idleReady(r) {
		t.Fatal("revived watcher never expires")
	}
	if cancels.Load() != 0 {
		t.Fatalf("cancels = %d", cancels.Load())
	}
}

// The client's own idle cut tells the agent why: reason progress_idle.
func TestClientIdleCutSendsReason(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	f := startFakeAgent(t, progressCaps, func(publish func(wire.AgentEvent)) { pingsUntil(stop, publish) })
	run, err := NewFromConn(testConn(t)).ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	drain(t, run, 5*time.Second)
	select {
	case reason := <-f.reasons:
		if reason != wire.ReasonProgressIdle {
			t.Fatalf("cancel reason = %q, want progress_idle", reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no cancel published on idle cut")
	}
}

func TestCancelWithReasonSendsReason(t *testing.T) {
	f := startFakeAgent(t, nil, nil)
	c := NewFromConn(testConn(t))
	if _, err := c.CancelWithReason(context.Background(), f.name, "run-1", wire.ReasonTokenRotation); err != nil {
		t.Fatalf("CancelWithReason: %v", err)
	}
	if r := <-f.reasons; r != wire.ReasonTokenRotation {
		t.Fatalf("reason = %q", r)
	}
	run := &Run{Ack: wire.ChatAck{RunID: "run-1"}, client: c, agent: f.name}
	if _, err := run.CancelWithReason(context.Background(), wire.ReasonInterrupted); err != nil {
		t.Fatalf("Run.CancelWithReason: %v", err)
	}
	if r := <-f.reasons; r != wire.ReasonInterrupted {
		t.Fatalf("reason = %q", r)
	}
	if _, err := run.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if r := <-f.reasons; r != "" {
		t.Fatalf("plain Cancel sent reason %q", r)
	}
}

// An agent whose ack does not declare progress only promised pings: the
// idle window is ignored for its run (as plain Chat).
func TestIdleWindowIgnoredWithoutProgressCap(t *testing.T) {
	for _, caps := range []*wire.AckCaps{nil, {RenewSubject: "trx.agent.x.renew.abc"}} {
		stop := make(chan struct{})
		f := startFakeAgent(t, caps, func(publish func(wire.AgentEvent)) {
			pingsUntil(stop, publish)
			publish(wire.AgentEvent{Type: wire.EventDone, StopReason: wire.StopEndTurn})
		})
		const window = 150 * time.Millisecond
		run, err := NewFromConn(testConn(t)).ChatWith(context.Background(), f.name, chatReq(), ChatOptions{IdleWindow: window})
		if err != nil {
			t.Fatalf("chat: %v", err)
		}
		time.Sleep(4 * window)
		close(stop)
		evs := drain(t, run, 5*time.Second)
		if len(evs) != 1 || evs[0].Type != wire.EventDone {
			t.Fatalf("caps %+v: events = %+v, want only the agent's done (no idle cut)", caps, evs)
		}
		select {
		case id := <-f.cancels:
			t.Fatalf("caps %+v: cancel %q published", caps, id)
		default:
		}
	}
}

func TestValidRenewSubject(t *testing.T) {
	good := "trx.agent.copay.renew.Q2x0Yk9fX-abcdefghijk"
	for _, tc := range []struct {
		agent, subject string
		want           bool
	}{
		{"copay", good, true},
		{"copay_east", good, true}, // regional alias: subject built from the base name
		{"copay_assist", "trx.agent.copay_assist.renew.abc", true},
		{"other", good, false},
		{"copay", "_RENEW.Q2x0Yk9fX-abcdefghijk", false},
		{"copay", "_INBOX.Q2x0Yk9fX", false},
		{"copay", "trx.agent.copay.renew.*", false},
		{"copay", "trx.agent.copay.renew.>", false},
		{"copay", "trx.agent.copay.renew.ab*c", false},
		{"copay", "trx.agent.copay.renew.", false},
		{"copay", "", false},
		{"", good, false},
		{"copay", "trx.agent.copay.renew.abc.def", false},
		{"copay", "trx.agent.copay.renew.ab c", false},
		{"copay", "trx.agent.copay.chat", false},
		{"copay", "trx.agent.copay_east.renew.abc", false},
		{"copay", "x.trx.agent.copay.renew.abc", false},
	} {
		if got := ValidRenewSubject(tc.agent, tc.subject); got != tc.want {
			t.Errorf("ValidRenewSubject(%q, %q) = %v, want %v", tc.agent, tc.subject, got, tc.want)
		}
	}
}

// Run.Renew sends only to a renew subject of the agent the run was started
// on; anything else (another agent's space, old _RENEW subjects) is refused
// before anything is published.
func TestRunRenewRefusesForeignSubject(t *testing.T) {
	c := NewFromConn(testConn(t))
	for _, subject := range []string{
		wire.AgentSubject("otherAgent", "renew.abc"),
		"_RENEW.Q2x0Yk9fX-abcdefghijk",
		"_INBOX.abc",
	} {
		got := renewResponder(t, subject, wire.RenewResponse{Applied: true})
		run := &Run{Ack: wire.ChatAck{RunID: "run-9"}, Caps: wire.AckCaps{RenewSubject: subject}, client: c, agent: "agentR"}
		if err := run.Renew(context.Background(), "tok"); !errors.Is(err, ErrRenewUnsupported) {
			t.Fatalf("subject %q: Renew = %v, want ErrRenewUnsupported", subject, err)
		}
		select {
		case req := <-got:
			t.Fatalf("subject %q: token sent: %+v", subject, req)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// The agent's renew handler may make two fresh identity checks (5 s each).
func TestRenewTimeoutCoversTwoIdentityChecks(t *testing.T) {
	if RenewTimeout != 12*time.Second {
		t.Fatalf("RenewTimeout = %s, want 12s", RenewTimeout)
	}
}
