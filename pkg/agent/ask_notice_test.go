package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/ask"
	"github.com/transactrx/nats-agent/pkg/wire"
)

var noticeAsk = ask.Ask{
	Title: "OneDrive is not connected",
	Mode:  ask.ModeNotice,
	Fields: []ask.Field{
		{Name: "connect", Type: ask.TypeLink, Label: "Connect OneDrive", Href: "/settings/onedrive"},
	},
}

// chatFeatures starts a run whose caller can ask and draws features.
func chatFeatures(t *testing.T, a *Agent, features ...string) *agentclient.Run {
	t.Helper()
	req := wire.ChatRequest{
		SessionID: "s1",
		Message:   wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}},
		UserInput: &wire.UserInputSupport{Supported: true, Features: features},
	}
	run, err := agentclient.NewFromConn(streamConn(t)).Chat(context.Background(), a.cfg.Name, req)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	return run
}

func oneAsk(a ask.Ask, results chan askResult) ChatHandler {
	return func(ctx context.Context, _ *Turn, s *Stream) error {
		ans, err := s.AskUser(ctx, a)
		results <- askResult{ans, err}
		return nil
	}
}

func TestNoticeAskDoesNotWait(t *testing.T) {
	results := make(chan askResult, 4)
	// A long live wait: a notice must not use it.
	a, _ := startAskAgent(t, askAgentOpts{userInput: true, wait: time.Hour, handler: oneAsk(noticeAsk, results)})
	run := chatFeatures(t, a, wire.FeatureLink, wire.FeatureNotice)
	start := time.Now()
	r := waitResult(t, results)
	if time.Since(start) > 3*time.Second {
		t.Fatalf("notice waited %s", time.Since(start))
	}
	if r.err != nil {
		t.Fatalf("err = %v", r.err)
	}
	if r.ans.AskID == "" || r.ans.Status != ask.StatusSubmitted || len(r.ans.Values) != 0 {
		t.Fatalf("answer = %+v", r.ans)
	}
	evs := drainRun(run)
	var opened, closed askEventPayload
	var kinds []string
	for _, ev := range evs {
		if ev.Type != wire.EventData {
			continue
		}
		kinds = append(kinds, ev.Kind)
		switch ev.Kind {
		case wire.DataKindAsk:
			opened = payloadOf(t, ev)
		case wire.DataKindAskClosed:
			closed = payloadOf(t, ev)
		}
	}
	if len(kinds) != 2 || kinds[0] != wire.DataKindAsk || kinds[1] != wire.DataKindAskClosed {
		t.Fatalf("data kinds %v, want [ask askClosed]", kinds)
	}
	if opened.AskID != r.ans.AskID || opened.Ask.Mode != ask.ModeNotice || opened.WaitMs != 0 {
		t.Fatalf("ask = %+v", opened)
	}
	if closed.AskID != r.ans.AskID || closed.Status != wire.AskClosedAnswered {
		t.Fatalf("askClosed = %+v", closed)
	}
}

func TestAskUserMissingFeatureUnsupported(t *testing.T) {
	person := ask.Ask{Title: "Who?", Fields: []ask.Field{{Name: "p", Label: "Person", Type: ask.TypePerson}}}
	link := ask.Ask{Title: "Pick", Fields: []ask.Field{
		{Name: "x", Type: ask.TypeText},
		{Name: "open", Type: ask.TypeLink, Label: "Open", Href: "/files"},
	}}
	cases := []struct {
		name     string
		ask      ask.Ask
		features []string
		ok       bool
	}{
		{"person without feature", person, []string{wire.FeatureLink, wire.FeatureNotice}, false},
		{"person with feature", person, []string{wire.FeaturePerson}, true},
		{"link without feature", link, []string{wire.FeaturePerson}, false},
		{"notice without notice", noticeAsk, []string{wire.FeatureLink}, false},
		{"notice without link", noticeAsk, []string{wire.FeatureNotice}, false},
		{"plain ask no features", testAsk, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			results := make(chan askResult, 4)
			a, _ := startAskAgent(t, askAgentOpts{userInput: true, wait: 200 * time.Millisecond, handler: oneAsk(c.ask, results)})
			run := chatFeatures(t, a, c.features...)
			r := waitResult(t, results)
			kinds := dataKinds(drainRun(run))
			if c.ok {
				if errors.Is(r.err, ErrUnsupported) || len(kinds) == 0 {
					t.Fatalf("err = %v kinds %v, want the ask shown", r.err, kinds)
				}
				return
			}
			if !errors.Is(r.err, ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", r.err)
			}
			if len(kinds) != 0 {
				t.Fatalf("data events %v, want none", kinds)
			}
		})
	}
}
