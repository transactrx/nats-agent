// Package delegation carries agent-to-agent hop depth through contexts and
// requests, so delegation chains can be bounded.
package delegation

import (
	"context"
	"encoding/json"

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

// Depth returns max(0, d.Depth, metadata["hops"]). Non-numeric hops are
// ignored.
func Depth(d *wire.Delegation, metadata map[string]any) int {
	n := 0
	if d != nil && d.Depth > n {
		n = d.Depth
	}
	var h int
	switch v := metadata["hops"].(type) {
	case float64:
		h = int(v)
	case int:
		h = v
	case int64:
		h = int(v)
	case json.Number:
		if f, err := v.Float64(); err == nil {
			h = int(f)
		}
	}
	if h > n {
		n = h
	}
	return n
}
