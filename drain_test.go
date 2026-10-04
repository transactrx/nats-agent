package natsagent_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/tool"
	"github.com/transactrx/nats-agent/pkg/toolclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// drainAgent is one instance of a shared agent name whose chat handler
// reports which instance served it and blocks until released.
func drainAgent(t *testing.T, name, instance string, release <-chan struct{}, served chan<- string) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.Config{Name: name, Description: "drain test", NATSURL: testURL, IDTValidation: &agent.IDTValidation{}})
	if err != nil {
		t.Fatal(err)
	}
	a.OnChat(func(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
		served <- instance
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		stream.Text(instance)
		stream.Done(wire.StopEndTurn, nil)
		return nil
	})
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAgentDrainFinishesInFlightAndHandsOverNewRuns(t *testing.T) {
	name := "drainAgent" + time.Now().Format("150405000000")
	release := make(chan struct{})
	served := make(chan string, 4)
	first := drainAgent(t, name, "first", release, served)
	c := agentclient.NewFromConn(testConn(t))
	msg := wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}}

	run, err := c.Chat(context.Background(), name, wire.ChatRequest{Message: msg})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-served; got != "first" {
		t.Fatalf("first run served by %s", got)
	}
	if first.ActiveRuns() != 1 {
		t.Fatalf("want 1 active run, got %d", first.ActiveRuns())
	}
	// A second instance joins; the first starts draining.
	second := drainAgent(t, name, "second", release, served)
	t.Cleanup(func() { _ = second.Shutdown() })
	drained := make(chan int, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		drained <- first.Drain(ctx)
	}()
	time.Sleep(300 * time.Millisecond)

	// New runs go to the instance that is not draining.
	run2, err := c.Chat(context.Background(), name, wire.ChatRequest{Message: msg})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-served; got != "second" {
		t.Fatalf("a draining instance must not take new runs; served by %s", got)
	}
	select {
	case n := <-drained:
		t.Fatalf("drain returned early with %d active runs", n)
	default:
	}
	close(release)
	if n := <-drained; n != 0 {
		t.Fatalf("drain should end clean, %d runs left", n)
	}
	for _, r := range []*agentclient.Run{run, run2} {
		var done bool
		for ev := range r.Events {
			if ev.Type == wire.EventDone {
				done = true
			}
		}
		if !done {
			t.Fatal("in-flight run must complete during drain")
		}
	}
	_ = first.Shutdown()
}

type blockingTool struct {
	release chan struct{}
	host    string
	served  chan string
}

func (b *blockingTool) Name() string                { return "drainTool" }
func (b *blockingTool) Description() string         { return "blocks" }
func (b *blockingTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (b *blockingTool) Run(ctx context.Context, _ map[string]any) (string, error) {
	b.served <- b.host
	select {
	case <-b.release:
		return b.host, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestToolHostDrainWaitsForInFlightRuns(t *testing.T) {
	release := make(chan struct{})
	served := make(chan string, 4)
	h1 := tool.NewHostWithNATS(testURL, "", "")
	if err := h1.Register(&blockingTool{release: release, host: "h1", served: served}, &tool.Info{TimeoutSeconds: 30}); err != nil {
		t.Fatal(err)
	}
	if err := h1.Start(); err != nil {
		t.Fatal(err)
	}
	tc := toolclient.NewFromConn(testConn(t))
	var wg sync.WaitGroup
	results := make(chan string, 2)
	call := func() {
		defer wg.Done()
		resp, err := tc.Run(context.Background(), "drainTool", wire.ToolRunRequest{Input: map[string]any{}}, 30*time.Second)
		if err != nil {
			results <- "error: " + err.Error()
			return
		}
		results <- resp.Content[0].Text
	}
	wg.Add(1)
	go call()
	if got := <-served; got != "h1" {
		t.Fatalf("served by %s", got)
	}
	h2 := tool.NewHostWithNATS(testURL, "", "")
	if err := h2.Register(&blockingTool{release: release, host: "h2", served: served}, &tool.Info{TimeoutSeconds: 30}); err != nil {
		t.Fatal(err)
	}
	if err := h2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h2.Shutdown)
	drained := make(chan int64, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		drained <- h1.Drain(ctx)
	}()
	time.Sleep(300 * time.Millisecond)
	wg.Add(1)
	go call()
	if got := <-served; got != "h2" {
		t.Fatalf("a draining host must not take new runs; served by %s", got)
	}
	close(release)
	if n := <-drained; n != 0 {
		t.Fatalf("drain should end clean, %d runs left", n)
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r != "h1" && r != "h2" {
			t.Fatalf("in-flight tool run must complete: %s", r)
		}
	}
	h1.Shutdown()
}
