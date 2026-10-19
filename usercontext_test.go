package natsagent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// startContextAgent answers with its ContextLine, or — when asked "consult"
// — with the answer of agent `next`, consulted using the handler's own ctx
// (no explicit forwarding).
func startContextAgent(t *testing.T, name, next string) {
	t.Helper()
	a, err := agent.New(agent.Config{Name: name, Description: "user context test agent", UserContext: true, NATSURL: testURL})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
		if turn.Message.Content[0].Text != "consult" {
			stream.Text(turn.ContextLine() + " | loc=" + turn.Location().String())
			return nil
		}
		c := agentclient.NewFromConn(a.Conn())
		run, err := c.Chat(ctx, next, wire.ChatRequest{Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "line"}}}})
		if err != nil {
			return err
		}
		for ev := range run.Events {
			if ev.Type == wire.EventText {
				stream.Text(ev.TextDelta)
			}
		}
		return nil
	})
	if err := a.Start(); err != nil {
		t.Fatalf("agent.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
}

func chatText(t *testing.T, ctx context.Context, c *agentclient.Client, agentName string, req wire.ChatRequest) string {
	t.Helper()
	run, err := c.Chat(ctx, agentName, req)
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	var b strings.Builder
	for ev := range run.Events {
		if ev.Type == wire.EventText {
			b.WriteString(ev.TextDelta)
		}
		if ev.Type == wire.EventError {
			t.Fatalf("agent error: %s", ev.Error)
		}
	}
	return b.String()
}

func TestUserContextEndToEnd(t *testing.T) {
	startContextAgent(t, "ctxFront", "ctxBack")
	startContextAgent(t, "ctxBack", "")
	c := agentclient.NewFromConn(testConn(t))
	msg := func(s string) wire.Message {
		return wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: s}}}
	}

	card, err := c.Card(context.Background(), "ctxBack")
	if err != nil || !card.Capabilities.UserContext || card.ProtocolVersion != "1.3" {
		t.Fatalf("card: %+v %v", card, err)
	}

	// Direct: the agent sees the cleaned context.
	got := chatText(t, context.Background(), c, "ctxBack", wire.ChatRequest{
		Message: msg("line"),
		Context: &wire.UserContext{TimeZone: "America/Chicago", UserName: "Jane\nDoe <x>"},
	})
	if !strings.Contains(got, "The user asking is Jane Doe x (time zone America/Chicago)") || !strings.Contains(got, "loc=America/Chicago") {
		t.Fatalf("direct: %q", got)
	}

	// Bad zone: dropped, turn still served, UTC used.
	got = chatText(t, context.Background(), c, "ctxBack", wire.ChatRequest{Message: msg("line"), Context: &wire.UserContext{TimeZone: "Local"}})
	if !strings.Contains(got, "time zone is unknown") || !strings.Contains(got, "loc=UTC") {
		t.Fatalf("bad zone: %q", got)
	}

	// Via client ctx, then agent→agent consult: forwarded with no extra code.
	ctx := agentclient.WithUserContext(context.Background(), &wire.UserContext{TimeZone: "Europe/Madrid", UserName: "Ana"})
	got = chatText(t, ctx, c, "ctxFront", wire.ChatRequest{Message: msg("consult")})
	if !strings.Contains(got, "The user asking is Ana (time zone Europe/Madrid)") || !strings.Contains(got, "loc=Europe/Madrid") {
		t.Fatalf("consult: %q", got)
	}

	// An explicit request context wins over the ctx one.
	got = chatText(t, ctx, c, "ctxBack", wire.ChatRequest{Message: msg("line"), Context: &wire.UserContext{TimeZone: "Asia/Tokyo"}})
	if !strings.Contains(got, "loc=Asia/Tokyo") {
		t.Fatalf("explicit: %q", got)
	}
}
