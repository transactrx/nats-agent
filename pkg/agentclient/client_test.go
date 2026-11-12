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
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) { pingsUntil(stop, publish) })
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
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) {
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
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) {
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
	caps := &wire.AckCaps{Progress: true, RenewSubject: "_RENEW.abc"}
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
	subject := nats.NewInbox()
	got := renewResponder(t, subject, wire.RenewResponse{Applied: false, Reason: "identity_mismatch"})
	run := &Run{Ack: wire.ChatAck{RunID: "run-9"}, Caps: wire.AckCaps{RenewSubject: subject}, client: NewFromConn(testConn(t))}

	err := run.Renew(context.Background(), "new-token")
	if !errors.Is(err, ErrRenewRejected) || !strings.Contains(err.Error(), "identity_mismatch") {
		t.Fatalf("Renew = %v, want ErrRenewRejected with reason", err)
	}
	if req := <-got; req.RunID != "run-9" || req.IDT != "new-token" || req.ExpiresAtMs != 0 {
		t.Fatalf("request = %+v", req)
	}
}

func TestRenewApplied(t *testing.T) {
	subject := nats.NewInbox()
	got := renewResponder(t, subject, wire.RenewResponse{Applied: true})
	run := &Run{Ack: wire.ChatAck{RunID: "run-9"}, Caps: wire.AckCaps{RenewSubject: subject}, client: NewFromConn(testConn(t))}

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
	f := startFakeAgent(t, nil, func(publish func(wire.AgentEvent)) {
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
