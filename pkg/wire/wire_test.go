package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAgentCardAccessRoundTrip(t *testing.T) {
	card := AgentCard{Name: "a", Description: "d", Access: &AgentAccess{AppID: "appX", FunctionID: "fnX"}}
	data, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	var back AgentCard
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.Access == nil || back.Access.AppID != "appX" || back.Access.FunctionID != "fnX" {
		t.Fatalf("access lost in round trip: %+v", back.Access)
	}
	// Absent access must marshal to no "access" key (legacy cards stay identical).
	data, _ = json.Marshal(AgentCard{Name: "b", Description: "d"})
	if string(data) != "" && containsKey(data, "access") {
		t.Fatalf("empty access must be omitted: %s", data)
	}
	if ProtocolVersion != "1.5" || CodeForbidden != 4031 || HeaderIDT != "X-TRX-IDT" {
		t.Fatalf("protocol constants: %s %d %s", ProtocolVersion, CodeForbidden, HeaderIDT)
	}
}

func containsKey(data []byte, key string) bool {
	var m map[string]json.RawMessage
	_ = json.Unmarshal(data, &m)
	_, ok := m[key]
	return ok
}

func TestAgentCardCategoryJSON(t *testing.T) {
	data, err := json.Marshal(AgentCard{Name: "a", Category: "Pharmacy"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"category":"Pharmacy"`) {
		t.Fatalf("category missing: %s", data)
	}
	// Empty category must be omitted (pre-1.4 cards stay identical).
	data, _ = json.Marshal(AgentCard{Name: "b"})
	if containsKey(data, "category") {
		t.Fatalf("empty category must be omitted: %s", data)
	}
}

func TestChatRequestBudgetOptional(t *testing.T) {
	data, err := json.Marshal(ChatRequest{StreamSubject: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if containsKey(data, "budget") {
		t.Fatalf("absent budget must be omitted: %s", data)
	}
	var req ChatRequest
	if err := json.Unmarshal([]byte(`{"message":{},"streamSubject":"s","budget":{"remainingMs":3000}}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.Budget == nil || req.Budget.RemainingMs != 3000 {
		t.Fatalf("budget = %+v, want remainingMs 3000", req.Budget)
	}
}

func TestOldJSONStillParses(t *testing.T) {
	var ack ChatAck
	if err := json.Unmarshal([]byte(`{"accepted":true,"runId":"r","sessionId":"s","agent":"a","instanceId":"i","streamSubject":"x"}`), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Caps != nil || !ack.Accepted || ack.RunID != "r" {
		t.Fatalf("v1.4 ack: %+v", ack)
	}
	var req ChatRequest
	if err := json.Unmarshal([]byte(`{"sessionId":"s","message":{"role":"user","content":[{"text":"hi"}]},"streamSubject":"x"}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.Budget != nil || req.StreamSubject != "x" {
		t.Fatalf("v1.4 request: %+v", req)
	}
	var ev AgentEvent
	if err := json.Unmarshal([]byte(`{"runId":"r","seq":3,"type":"done","stopReason":"endTurn"}`), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Reason != "" || ev.StopReason != StopEndTurn {
		t.Fatalf("v1.4 event: %+v", ev)
	}
	// New fields stay off the wire when unset.
	data, _ := json.Marshal(ChatAck{Accepted: true})
	if containsKey(data, "caps") {
		t.Fatalf("absent caps must be omitted: %s", data)
	}
	data, _ = json.Marshal(AgentEvent{Type: EventDone})
	if containsKey(data, "reason") {
		t.Fatalf("empty reason must be omitted: %s", data)
	}
	data, _ = json.Marshal(Capabilities{})
	if containsKey(data, "progress") || containsKey(data, "renew") {
		t.Fatalf("false progress/renew must be omitted: %s", data)
	}
}

func TestIsProgress(t *testing.T) {
	payload, _ := json.Marshal(ProgressPayload{Stage: "onedrive"})
	cases := []struct {
		ev   AgentEvent
		want bool
	}{
		{AgentEvent{Type: EventPing}, false},
		{AgentEvent{Type: EventText, TextDelta: "x"}, true},
		{AgentEvent{Type: EventToolUse}, true},
		{AgentEvent{Type: EventToolResult}, true},
		{AgentEvent{Type: EventData, Kind: DataKindProgress, Payload: payload}, true},
		{AgentEvent{Type: EventStatus}, true},
		{AgentEvent{Type: EventStart}, true},
	}
	for _, c := range cases {
		if got := c.ev.IsProgress(); got != c.want {
			t.Errorf("IsProgress(%s) = %v, want %v", c.ev.Type, got, c.want)
		}
	}
}

func TestStoredMessageIncomplete(t *testing.T) {
	data, err := json.Marshal(StoredMessage{Role: "assistant", Content: []ContentBlock{{Text: "partial"}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"incomplete"`) {
		t.Fatalf("complete message carries incomplete: %s", data)
	}
	var old StoredMessage
	if err := json.Unmarshal([]byte(`{"role":"assistant","content":[{"text":"x"}],"timestamp":"2026-11-12T10:00:00Z"}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Incomplete != "" {
		t.Fatalf("old JSON Incomplete = %q", old.Incomplete)
	}
	in := StoredMessage{Role: "assistant", Content: []ContentBlock{{Text: "partial"}}, Incomplete: ReasonTimeCap}
	data, _ = json.Marshal(in)
	if !strings.Contains(string(data), `"incomplete":"time_cap"`) {
		t.Fatalf("marshal = %s", data)
	}
	var out StoredMessage
	if err := json.Unmarshal(data, &out); err != nil || out.Incomplete != ReasonTimeCap {
		t.Fatalf("round trip = %+v %v", out, err)
	}
}
