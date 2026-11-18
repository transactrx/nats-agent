package delegation

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/wire"
)

func TestDepth(t *testing.T) {
	tests := []struct {
		name string
		d    *wire.Delegation
		md   map[string]any
		want int
	}{
		{"nil nil", nil, nil, 0},
		{"field", &wire.Delegation{Depth: 2}, nil, 2},
		{"hops float", nil, map[string]any{"hops": float64(1)}, 1},
		{"max field", &wire.Delegation{Depth: 1}, map[string]any{"hops": float64(2)}, 2},
		{"negative field", &wire.Delegation{Depth: -3}, nil, 0},
		{"hops string", nil, map[string]any{"hops": "1"}, 0},
		{"hops bool", nil, map[string]any{"hops": true}, 0},
		{"hops negative", nil, map[string]any{"hops": float64(-1)}, 0},
		{"hops json.Number", nil, map[string]any{"hops": json.Number("2")}, 2},
		{"hops huge", nil, map[string]any{"hops": float64(1e30)}, math.MaxInt32},
		{"hops inf", nil, map[string]any{"hops": math.Inf(1)}, math.MaxInt32},
		{"hops nan", nil, map[string]any{"hops": math.NaN()}, 0},
		{"hops json.Number huge", nil, map[string]any{"hops": json.Number("1e30")}, math.MaxInt32},
		{"hops int64 huge", nil, map[string]any{"hops": int64(math.MaxInt64)}, math.MaxInt32},
		{"field huge", &wire.Delegation{Depth: math.MaxInt}, nil, math.MaxInt32},
		{"hops int", nil, map[string]any{"hops": 3}, 3},
		{"hops int64", nil, map[string]any{"hops": int64(4)}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Depth(tt.d, tt.md); got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}

func TestMarkerRoundTrip(t *testing.T) {
	ctx := context.Background()
	if _, ok := From(ctx); ok {
		t.Fatal("expected no marker")
	}
	want := Marker{Depth: 1, MayDelegate: true}
	ctx = With(ctx, want)
	if m, ok := From(ctx); !ok || m != want {
		t.Fatalf("got %+v %v", m, ok)
	}
	if m, ok := From(context.WithoutCancel(ctx)); !ok || m != want {
		t.Fatalf("WithoutCancel: %+v %v", m, ok)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if m, ok := From(cctx); !ok || m != want {
		t.Fatalf("WithCancel: %+v %v", m, ok)
	}
}

func TestWireJSON(t *testing.T) {
	b, _ := json.Marshal(wire.ChatRequest{SessionID: "s"})
	if strings.Contains(string(b), `"delegation"`) {
		t.Fatalf("unexpected delegation: %s", b)
	}
	b, _ = json.Marshal(wire.ChatRequest{Delegation: &wire.Delegation{Depth: 1}})
	if !strings.Contains(string(b), `"delegation":{"depth":1}`) {
		t.Fatalf("missing delegation: %s", b)
	}
	b, _ = json.Marshal(wire.ToolRunRequest{Delegation: &wire.Delegation{Depth: 2}})
	if !strings.Contains(string(b), `"delegation":{"depth":2}`) {
		t.Fatalf("missing tool delegation: %s", b)
	}
	var r wire.ChatRequest
	if err := json.Unmarshal([]byte(`{"sessionId":"s"}`), &r); err != nil || r.Delegation != nil {
		t.Fatalf("err=%v delegation=%v", err, r.Delegation)
	}
}
