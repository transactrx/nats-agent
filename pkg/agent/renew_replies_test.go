package agent

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// Renew subjects live in trx.agent.<name>.>, which every instance of the
// agent also queue-subscribes through its service. These tests count every
// reply a renew gets, not only the first one nc.Request returns.

// rawReply is one reply as the caller's connection saw it.
type rawReply struct {
	header nats.Header
	data   []byte
}

// allReplies sends one request to subject and returns every reply that
// arrives within wait.
func allReplies(t *testing.T, subject string, body []byte, wait time.Duration) []rawReply {
	t.Helper()
	nc := streamConn(t)
	inbox := nats.NewInbox()
	var mu sync.Mutex
	var got []rawReply
	sub, err := nc.Subscribe(inbox, func(m *nats.Msg) {
		mu.Lock()
		got = append(got, rawReply{header: m.Header, data: append([]byte(nil), m.Data...)})
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.PublishRequest(subject, inbox, body); err != nil {
		t.Fatal(err)
	}
	time.Sleep(wait)
	mu.Lock()
	defer mu.Unlock()
	return append([]rawReply(nil), got...)
}

// startOldInstance joins name's service queue group the way an instance
// built before the silent renew.> endpoint does (e.g. the previous release
// during a rolling deploy, or a regional twin on an older build): same base
// path and queue, standard endpoints only. It answers a renew subject with
// the framework's 404 envelope.
func startOldInstance(t *testing.T, name string) {
	t.Helper()
	svc, err := nats_service.NewLowLevel(wire.AgentPrefix+"."+name, "agent."+name, embedURL, "", "", 1024*2, 1024*300)
	if err != nil {
		t.Fatal(err)
	}
	silent := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		fwd := nats_service.NewForwardedError(msg.Path)
		return &fwd
	}
	for _, p := range []string{"card", "ping", "chat", "cancel"} {
		if err := svc.AddEndpoint(p, silent); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Shutdown() })
}

// Reproduction of the E2E failure: the run's instance applies the renew, but
// an older instance in the same queue group answers first with a 404
// envelope, which the caller read as a rejection.
func TestRenewWithOlderInstanceInQueueGroup(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turns)
	var applied atomic.Int32
	turn.OnRenew(func(string) { applied.Add(1) })
	startOldInstance(t, a.cfg.Name) // after the ack: the run lives on a

	body, _ := json.Marshal(wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-alice2.c"})
	for _, r := range allReplies(t, run.Ack.Caps.RenewSubject, body, 300*time.Millisecond) {
		t.Logf("raw reply: header=%v body=%s", r.header, r.data)
	}
	for i := 0; i < 20; i++ {
		if err := run.Renew(context.Background(), "IDT-alice2.c"); err != nil {
			t.Fatalf("renew %d: %v (the run's instance applied it: %d so far)", i, err, applied.Load())
		}
	}
	close(release)
	drainRun(run)
}

// Two current instances: the run lives on a; every renew is answered once,
// by a's run subscription; b's service stays silent.
func TestRenewTwoInstancesOneReply(t *testing.T) {
	subj := "test.renew.identity." + t.Name()
	startFakeIdentity(t, subj)
	a, turnsA, releaseA := startRenewAgent(t, testAgentOpts{renew: true, identity: subj})
	run := chatAs(t, a, "IDT-alice1.c")
	turn := waitTurn(t, turnsA)
	var applied atomic.Int32
	turn.OnRenew(func(string) { applied.Add(1) })
	_, turnsB, _ := startRenewAgent(t, testAgentOpts{renew: true, identity: subj, name: a.cfg.Name})

	body, _ := json.Marshal(wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-alice2.c"})
	for i := 0; i < 20; i++ {
		got := allReplies(t, run.Ack.Caps.RenewSubject, body, 200*time.Millisecond)
		if len(got) != 1 {
			t.Fatalf("renew %d: %d replies, want exactly 1: %+v", i, len(got), got)
		}
		if string(got[0].data) != `{"applied":true}` || len(got[0].header) != 0 {
			t.Fatalf("renew %d: reply header=%v body=%s, want the run's {\"applied\":true}", i, got[0].header, got[0].data)
		}
	}
	if n := applied.Load(); n != 20 {
		t.Fatalf("instance a applied %d renews, want 20", n)
	}
	select {
	case tb := <-turnsB:
		t.Fatalf("instance b ran a turn: %s", tb.RunID)
	default:
	}
	close(releaseA)
	drainRun(run)
}

// A renew to an ended run gets no reply at all from any current instance.
func TestRenewEndedRunTwoInstancesNoReply(t *testing.T) {
	a, turns, release := startRenewAgent(t, testAgentOpts{renew: true})
	run := chatAs(t, a, "IDT-x1.c")
	waitTurn(t, turns)
	startRenewAgent(t, testAgentOpts{renew: true, name: a.cfg.Name})
	close(release)
	drainRun(run)

	body, _ := json.Marshal(wire.RenewRequest{RunID: run.Ack.RunID, IDT: "IDT-x2.c"})
	for i := 0; i < 5; i++ {
		if got := allReplies(t, run.Ack.Caps.RenewSubject, body, 200*time.Millisecond); len(got) != 0 {
			t.Fatalf("renew to ended run: %d replies, want none: header=%v body=%s", len(got), got[0].header, got[0].data)
		}
	}
}
