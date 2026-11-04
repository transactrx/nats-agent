package agent

import "testing"

// New() connects to NATS, so build the Agent directly; Card() reads only cfg
// and the optional handlers.
func TestCardCarriesCategory(t *testing.T) {
	a := &Agent{cfg: Config{Name: "x", Description: "d", Category: "Operations"}}
	card := a.Card()
	if card.Category != "Operations" {
		t.Fatalf("Category = %q, want Operations", card.Category)
	}
	if card.ProtocolVersion != "1.4" {
		t.Fatalf("ProtocolVersion = %q, want 1.4", card.ProtocolVersion)
	}
}
