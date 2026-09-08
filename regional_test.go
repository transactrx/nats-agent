package natsagent_test

import (
	"context"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

func TestRegionalRoutesKeepSessionsAndTurnsInTheirRegion(t *testing.T) {
	name := "regionalFixture"
	for _, region := range []string{"east", "west"} {
		store := newMemSessionStore()
		store.put("alice", wire.SessionGetResponse{SessionID: "session", Title: region})
		a, err := agent.New(agent.Config{Name: name, Region: region, Description: "regional test", NATSURL: testURL, IDTValidation: &agent.IDTValidation{Enabled: false}})
		if err != nil {
			t.Fatal(err)
		}
		a.UseSessionStore(store)
		a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
			if turn.Message.Content[0].Text == "block" {
				<-ctx.Done()
				return ctx.Err()
			}
			stream.Text(region)
			return nil
		})
		if err := a.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = a.Shutdown() })
	}
	client := agentclient.NewFromConn(testConn(t))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Ping waits for separately connected regional subscriptions to be ready.
	for _, region := range []string{"east", "west"} {
		alias := name + "_" + region
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, err := client.Ping(ctx, alias); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("regional route not ready")
			}
			time.Sleep(10 * time.Millisecond)
		}
		for range 5 {
			got, err := client.SessionGet(ctx, alias, "alice", "session")
			if err != nil || got.Title != region {
				t.Fatalf("%s session: %+v %v", region, got, err)
			}
		}
		if _, err := client.SessionGet(ctx, alias, "bob", "session"); err == nil {
			t.Fatal("foreign session accessible")
		}
		run, err := client.Chat(ctx, alias, wire.ChatRequest{UserID: "alice", SessionID: "session", Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hello"}}}})
		if err != nil {
			t.Fatal(err)
		}
		text := ""
		for ev := range run.Events {
			text += ev.TextDelta
		}
		if text != region {
			t.Fatalf("turn reached %q, wanted %q", text, region)
		}
	}
	blocked, err := client.Chat(ctx, name+"_west", wire.ChatRequest{UserID: "alice", SessionID: "session", Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "block"}}}})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := client.Cancel(ctx, name+"_west", blocked.Ack.RunID)
	if err != nil || !ok {
		t.Fatalf("regional cancel: %v %v", ok, err)
	}
	for range blocked.Events {
	}
	cards, err := client.Discover(ctx, wire.DiscoverFilter{}, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, card := range cards {
		if card.Name == name {
			count++
		}
		if card.Name == name+"_east" || card.Name == name+"_west" {
			t.Fatal("regional route advertised as separate agent")
		}
	}
	if count != 1 {
		t.Fatalf("logical agent count: %d", count)
	}
}
