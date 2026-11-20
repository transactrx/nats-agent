// Package delegation carries agent-to-agent hop depth through contexts and
// requests, so delegation chains can be bounded.
package delegation

import (
	"context"
	"encoding/json"
	"math"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// Marker is the delegation state of the turn running in a context.
type Marker struct {
	Depth       int
	MayDelegate bool
}

type ctxKey struct{}

// With returns ctx carrying m.
func With(ctx context.Context, m Marker) context.Context {
	return context.WithValue(ctx, ctxKey{}, m)
}

// From returns the marker in ctx, if any.
func From(ctx context.Context) (Marker, bool) {
	m, ok := ctx.Value(ctxKey{}).(Marker)
	return m, ok
}

// Depth returns max(0, d.Depth, metadata["hops"]), clamped to MaxInt32.
// Non-numeric hops are ignored; NaN counts as 0.
func Depth(d *wire.Delegation, metadata map[string]any) int {
	n := 0
	if d != nil {
		n = clamp(int64(d.Depth))
	}
	var h int
	switch v := metadata["hops"].(type) {
	case float64:
		h = clampFloat(v)
	case int:
		h = clamp(int64(v))
	case int64:
		h = clamp(v)
	case json.Number:
		// On ErrRange Float64 still returns +-Inf; use it. Syntax errors give 0.
		f, _ := v.Float64()
		h = clampFloat(f)
	}
	if h > n {
		n = h
	}
	return n
}

func clamp(v int64) int {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(v)
}

func clampFloat(f float64) int {
	if math.IsNaN(f) || f < 0 {
		return 0
	}
	if f >= math.MaxInt32 {
		return math.MaxInt32
	}
	return int(f)
}
