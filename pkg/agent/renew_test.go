package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// fakeIdentity answers validateInternalToken on the embedded server. users
// maps token → {userId, accountId}; other tokens are invalid. revoke makes a
// token invalid from then on.
type fakeIdentity struct {
	mu    sync.Mutex
	users map[string][2]string
	noFn  map[string]bool // valid tokens whose function is not granted
	calls int
}

func (f *fakeIdentity) revoke(token string) {
	f.mu.Lock()
	delete(f.users, token)
	f.mu.Unlock()
}

func (f *fakeIdentity) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func startFakeIdentity(t *testing.T, subject string) *fakeIdentity {
	t.Helper()
	f := &fakeIdentity{users: map[string][2]string{
		"IDT-alice1.c":      {"alice", "acc1"},
		"IDT-alice2.c":      {"alice", "acc1"},
		"IDT-bob.c":         {"bob", "acc1"},
		"IDT-aliceOther.c":  {"alice", "acc2"},
		"IDT-aliceCached.c": {"alice", "acc1"},
		"IDT-bobNoFn.c":     {"bob", "acc1"},
		"IDT-aliceNoFn.c":   {"alice", "acc1"},
	}, noFn: map[string]bool{"IDT-bobNoFn.c": true, "IDT-aliceNoFn.c": true}}
	nc := streamConn(t)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var req struct {
			IDT string `json:"idt"`
		}
		_ = json.Unmarshal(m.Data, &req)
		f.mu.Lock()
		f.calls++
		u, ok := f.users[req.IDT]
		noFn := f.noFn[req.IDT]
		f.mu.Unlock()
		resp := map[string]any{"valid": false, "functionGranted": false, "reason": "INVALID_IDT"}
		if ok {
			resp = map[string]any{"valid": true, "functionGranted": !noFn, "userId": u[0], "accountId": u[1]}
			if noFn {
				resp["reason"] = "DENIED_FN"
			}
		}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	if err != nil {
		t.Fatalf("fake identity: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

type testAgentOpts struct {
	renew    bool
	observe  bool
	identity string // validate subject; "" = IDT validation off
	// handler overrides the default (block until release or ctx done).
	handler ChatHandler
}

// startRenewAgent starts a full agent on the embedded server. The default
// handler hands its Turn to turns and blocks until release closes.
func startRenewAgent(t *testing.T, o testAgentOpts) (a *Agent, turns chan *Turn, release chan struct{}) {
	t.Helper()
	streamConn(t) // starts the embedded server
	cfg := Config{
		Name:          "renew" + strings.ReplaceAll(t.Name(), "/", "_"),
		Description:   "renew test",
		NATSURL:       embedURL,
		Renew:         o.renew,
		IDTValidation: &IDTValidation{},
	}
	if o.identity != "" {
		cfg.Access = &wire.AgentAccess{AppID: "appT", FunctionID: "fnT"}
		cfg.IDTValidation = &IDTValidation{Enabled: true, ObserveOnly: o.observe, Subject: o.identity, Timeout: 2 * time.Second, CacheTTL: time.Minute}
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	turns = make(chan *Turn, 4)
	release = make(chan struct{})
	h := o.handler
	if h == nil {
		h = func(ctx context.Context, turn *Turn, stream *Stream) error {
			turns <- turn
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		}
	}
	a.OnChat(h)
	if err := a.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	return a, turns, release
}

func chatAs(t *testing.T, a *Agent, token string) *agentclient.Run {
	t.Helper()
	c := agentclient.NewFromConn(streamConn(t))
	ctx := context.Background()
	if token != "" {
		ctx = agentclient.WithIDT(ctx, token)
	}
	run, err := c.Chat(ctx, a.cfg.Name, wire.ChatRequest{
		SessionID: "s1",
		Message:   wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}},
	})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	return run
}

func waitTurn(t *testing.T, turns chan *Turn) *Turn {
	t.Helper()
	select {
	case turn := <-turns:
		return turn
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
		return nil
	}
}

func renew(t *testing.T, run *agentclient.Run, req wire.RenewRequest) wire.RenewResponse {
	t.Helper()
	if run.Ack.Caps == nil || run.Ack.Caps.RenewSubject == "" {
		t.Fatalf("ack has no renewSubject: %+v", run.Ack.Caps)
	}
	data, _ := json.Marshal(req)
	msg, err := streamConn(t).Request(run.Ack.Caps.RenewSubject, data, 3*time.Second)
	if err != nil {
		t.Fatalf("renew request: %v", err)
	}
	var resp wire.RenewResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("renew reply: %v", err)
	}
	return resp
}

func drainRun(run *agentclient.Run) []wire.AgentEvent {
	var evs []wire.AgentEvent
	for ev := range run.Events {
		evs = append(evs, ev)
	}
	return evs
}

func TestRenewAccepted(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)

	prefix := wire.AgentSubject(a.cfg.Name, "renew.")
	if !strings.HasPrefix(run.Ack.Caps.RenewSubject, prefix) || len(run.Ack.Caps.RenewSubject) != len(prefix)+22 {
		t.Fatalf("renewSubject = %q, want %s<22 chars>", run.Ack.Caps.RenewSubject, prefix)
	}
	if !agentclient.ValidRenewSubject(a.cfg.Name, run.Ack.Caps.RenewSubject) {
		t.Fatalf("renewSubject %q rejected by the client validator", run.Ack.Caps.RenewSubject)
	}
	if turn.CurrentIDT() != "IDT-alice1.c" {
		t.Fatalf("CurrentIDT before renew = %q", turn.CurrentIDT())
	}
	resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-alice2.c"})
	if !resp.Applied {
		t.Fatalf("renew rejected: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-alice2.c" {
		t.Fatalf("CurrentIDT = %q, want the new token", turn.CurrentIDT())
	}
	if turn.Identity.IDT != "IDT-alice1.c" {
		t.Fatalf("Identity.IDT = %q, must keep the first token", turn.Identity.IDT)
	}
	close(release)
	drainRun(run)
}

func TestRenewRejectedOtherUser(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-bob.c"})
	if resp.Applied || resp.Reason == "" {
		t.Fatalf("token of another user accepted: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-alice1.c" {
		t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

func TestRenewRejectedOtherAccount(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-aliceOther.c"})
	if resp.Applied || resp.Reason == "" {
		t.Fatalf("token of another account accepted: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-alice1.c" {
		t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

func TestRenewRejectedInvalidToken(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	for _, tok := range []string{"IDT-forged.c", ""} {
		resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: tok})
		if resp.Applied || resp.Reason == "" {
			t.Fatalf("invalid token %q accepted: %+v", tok, resp)
		}
	}
	if turn.CurrentIDT() != "IDT-alice1.c" {
		t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

// A token that identity allowed (and the validator cached) but that has
// since been revoked must be rejected: renew validates fresh.
func TestRenewValidatesFresh(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	f := startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	// Cache an allow for IDT-aliceCached.c in session s1.
	first := chatAs(t, a, "IDT-aliceCached.c")
	waitTurn(t, turns)
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	f.revoke("IDT-aliceCached.c")
	before := f.callCount()
	resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-aliceCached.c"})
	if resp.Applied {
		t.Fatalf("revoked token accepted from cache: %+v", resp)
	}
	if f.callCount() != before+1 {
		t.Fatalf("renew must ask identity: calls %d → %d", before, f.callCount())
	}
	if turn.CurrentIDT() != "IDT-alice1.c" {
		t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(first)
	drainRun(run)
}

func TestRenewRejectedWrongRun(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	resp := renew(t, run, wire.RenewRequest{RunID: "run_other", IDT: "IDT-alice2.c"})
	if resp.Applied || resp.Reason == "" {
		t.Fatalf("renew for another run accepted: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-alice1.c" {
		t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

// Observe mode admits turns unverified; renew follows the same rule but
// still refuses a token identity resolves to a different user.
func TestRenewObserveMode(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj, observe: true})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-bob.c"}); resp.Applied {
		t.Fatalf("observe mode accepted another user's token: %+v", resp)
	}
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-alice2.c"}); !resp.Applied {
		t.Fatalf("observe mode rejected same user's token: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-alice2.c" {
		t.Fatalf("CurrentIDT = %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

// Validation off: the turn was admitted on its token alone; so is a renew.
func TestRenewValidationOff(t *testing.T) {
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true})
	run := chatAs(t, a, "IDT-x1.c")
	turn := waitTurn(t, turns)
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x2.c"}); !resp.Applied {
		t.Fatalf("renew rejected with validation off: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-x2.c" {
		t.Fatalf("CurrentIDT = %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

func TestNoRenewSubjectWithoutCapability(t *testing.T) {
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: false})
	subsBefore := a.nc.NumSubscriptions()
	run := chatAs(t, a, "IDT-x1.c")
	waitTurn(t, turns)
	if run.Ack.Caps != nil && run.Ack.Caps.RenewSubject != "" {
		t.Fatalf("ack announces renewSubject without capability: %+v", run.Ack.Caps)
	}
	if n := a.nc.NumSubscriptions(); n != subsBefore {
		t.Fatalf("subscriptions %d → %d during a run without renew", subsBefore, n)
	}
	close(release)
	drainRun(run)
}

func TestRenewSubjectClosedAfterRun(t *testing.T) {
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true})
	subsBefore := a.nc.NumSubscriptions()
	run := chatAs(t, a, "IDT-x1.c")
	waitTurn(t, turns)
	if n := a.nc.NumSubscriptions(); n != subsBefore+1 {
		t.Fatalf("subscriptions %d → %d, want one renew subscription", subsBefore, n)
	}
	close(release)
	drainRun(run)
	deadline := time.Now().Add(2 * time.Second)
	for a.nc.NumSubscriptions() != subsBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := a.nc.NumSubscriptions(); n != subsBefore {
		t.Fatalf("renew subscription leaked: %d → %d", subsBefore, n)
	}
	// The subject lives in the agent's space, where the service stays silent
	// on renew paths: a late renew gets no reply at all.
	data, _ := json.Marshal(wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x2.c"})
	_, err := streamConn(t).Request(run.Ack.Caps.RenewSubject, data, 300*time.Millisecond)
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("renew after run end: err = %v, want a timeout (no reply)", err)
	}
}

func TestOnRenewCalled(t *testing.T) {
	got := make(chan string, 2)
	release := make(chan struct{})
	ready := make(chan *Turn, 1)
	a, _, _ := startRenewAgent(t, testAgentOpts{renew: true, handler: func(ctx context.Context, turn *Turn, stream *Stream) error {
		turn.OnRenew(func(idt string) { got <- idt })
		ready <- turn
		<-release
		return nil
	}})
	run := chatAs(t, a, "IDT-x1.c")
	waitTurn(t, ready)
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x2.c"}); !resp.Applied {
		t.Fatalf("renew rejected: %+v", resp)
	}
	select {
	case idt := <-got:
		if idt != "IDT-x2.c" {
			t.Fatalf("OnRenew got %q", idt)
		}
	default:
		t.Fatal("OnRenew not called before the renew reply")
	}
	// A rejected renew does not call OnRenew.
	if resp := renew(t, run, wire.RenewRequest{RunID: "run_other", IDT: "IDT-x3.c"}); resp.Applied {
		t.Fatalf("wrong run accepted: %+v", resp)
	}
	select {
	case idt := <-got:
		t.Fatalf("OnRenew called on reject with %q", idt)
	default:
	}
	close(release)
	drainRun(run)
}

func TestIDTExpiryFromRenew(t *testing.T) {
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true})
	run := chatAs(t, a, "IDT-x1.c")
	turn := waitTurn(t, turns)
	if _, ok := turn.IDTExpiry(); ok {
		t.Fatal("IDTExpiry set before any renew")
	}
	exp := time.Now().Add(15 * time.Minute).Truncate(time.Millisecond)
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x2.c", ExpiresAtMs: exp.UnixMilli()}); !resp.Applied {
		t.Fatalf("renew rejected: %+v", resp)
	}
	got, ok := turn.IDTExpiry()
	if !ok || !got.Equal(exp) {
		t.Fatalf("IDTExpiry = %v %v, want %v", got, ok, exp)
	}
	// A renew without expiry clears it.
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x3.c"}); !resp.Applied {
		t.Fatalf("renew rejected: %+v", resp)
	}
	if _, ok := turn.IDTExpiry(); ok {
		t.Fatal("IDTExpiry kept after a renew without expiresAtMs")
	}
	close(release)
	drainRun(run)
}

// Observe mode: identity's observe-deny (valid token, function not granted)
// still names the user; a renew to another user is refused, whether the
// other side or the run itself was admitted on such a token.
func TestRenewObserveModeRefusesOtherUser(t *testing.T) {
	cases := []struct {
		name, first, other, same string
	}{
		{"renew token not granted", "IDT-alice1.c", "IDT-bobNoFn.c", "IDT-aliceNoFn.c"},
		{"run token not granted", "IDT-aliceNoFn.c", "IDT-bob.c", "IDT-alice2.c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			subj := "test.renew.identity." + strings.ReplaceAll(t.Name(), " ", "_")
			startFakeIdentity(t, subj)
			a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj, observe: true})
			run := chatAs(t, a, tc.first)
			turn := waitTurn(t, turns)
			if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: tc.other}); resp.Applied || resp.Reason != renewIdentityMismatch {
				t.Fatalf("observe mode accepted another user's token: %+v", resp)
			}
			if turn.CurrentIDT() != tc.first {
				t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
			}
			if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: tc.same}); !resp.Applied {
				t.Fatalf("observe mode rejected same user's token: %+v", resp)
			}
			close(release)
			drainRun(run)
		})
	}
}

// A panicking OnRenew hook is recovered: later hooks still run and the
// renew is still acknowledged.
func TestOnRenewHookPanicRecovered(t *testing.T) {
	got := make(chan string, 2)
	release := make(chan struct{})
	ready := make(chan *Turn, 1)
	a, _, _ := startRenewAgent(t, testAgentOpts{renew: true, handler: func(ctx context.Context, turn *Turn, stream *Stream) error {
		turn.OnRenew(func(string) { panic("hook boom") })
		turn.OnRenew(func(idt string) { got <- idt })
		ready <- turn
		<-release
		return nil
	}})
	run := chatAs(t, a, "IDT-x1.c")
	turn := waitTurn(t, ready)
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x2.c"}); !resp.Applied {
		t.Fatalf("renew after hook panic: %+v", resp)
	}
	select {
	case idt := <-got:
		if idt != "IDT-x2.c" {
			t.Fatalf("second hook got %q", idt)
		}
	default:
		t.Fatal("hook after the panicking one not called")
	}
	if turn.CurrentIDT() != "IDT-x2.c" {
		t.Fatalf("CurrentIDT = %q", turn.CurrentIDT())
	}
	close(release)
	drainRun(run)
}

// Observe mode: the caller resolved at the first fresh check is kept, so a
// later renew is still compared after the run's current token has expired.
func TestRenewObserveModeAfterTokenExpiry(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	f := startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj, observe: true})
	run := chatAs(t, a, "IDT-aliceNoFn.c") // admitted without ids
	turn := waitTurn(t, turns)
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-alice2.c"}); !resp.Applied {
		t.Fatalf("same user's token rejected: %+v", resp)
	}
	// Both tokens the run has held are now expired at identity.
	f.revoke("IDT-aliceNoFn.c")
	f.revoke("IDT-alice2.c")
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-bob.c"}); resp.Applied || resp.Reason != renewIdentityMismatch {
		t.Fatalf("another user's token accepted after expiry: %+v", resp)
	}
	if turn.CurrentIDT() != "IDT-alice2.c" {
		t.Fatalf("CurrentIDT changed on reject: %q", turn.CurrentIDT())
	}
	if resp := renew(t, run, wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-alice1.c"}); !resp.Applied {
		t.Fatalf("same user's token rejected after expiry: %+v", resp)
	}
	close(release)
	drainRun(run)
}
