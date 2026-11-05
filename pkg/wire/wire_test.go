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
	if ProtocolVersion != "1.4" || CodeForbidden != 4031 || HeaderIDT != "X-TRX-IDT" {
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
