package agentclient

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// captureChat answers one chat request on name and hands its body on.
func captureChat(t *testing.T, name string) chan wire.ChatRequest {
	t.Helper()
	nc := testConn(t)
	got := make(chan wire.ChatRequest, 1)
	if _, err := nc.Subscribe(wire.AgentSubject(name, "chat"), func(m *nats.Msg) {
		var req wire.ChatRequest
		_ = json.Unmarshal(m.Data, &req)
		got <- req
		respondOK(t, m, wire.ChatAck{Accepted: true, RunID: "run-1", Agent: name, StreamSubject: req.StreamSubject})
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestChatWithUserInputFeatures(t *testing.T) {
	cases := []struct {
		o    ChatOptions
		want *wire.UserInputSupport
	}{
		{ChatOptions{UserInput: true, UserInputFeatures: wire.InputFeatures()},
			&wire.UserInputSupport{Supported: true, Features: []string{"person", "link", "notice"}}},
		{ChatOptions{UserInput: true}, &wire.UserInputSupport{Supported: true}},
		{ChatOptions{UserInputFeatures: wire.InputFeatures()}, nil},
	}
	for i, c := range cases {
		name := fmt.Sprintf("feat%d_%d", agentSeq.Add(1), i)
		got := captureChat(t, name)
		ctx, cancel := context.WithCancel(context.Background())
		if _, err := NewFromConn(testConn(t)).ChatWith(ctx, name, chatReq(), c.o); err != nil {
			cancel()
			t.Fatalf("case %d: %v", i, err)
		}
		select {
		case req := <-got:
			if !reflect.DeepEqual(req.UserInput, c.want) {
				t.Errorf("case %d: userInput = %+v, want %+v", i, req.UserInput, c.want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("case %d: no request", i)
		}
		cancel()
	}
}
