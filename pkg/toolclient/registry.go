package toolclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// RegistryOptions tunes how an agent watches the tool mesh.
type RegistryOptions struct {
	// Allow restricts the registry to these tool names. Empty = all tools.
	Allow []string
	// Deny always wins over Allow.
	Deny []string
	// Filter is applied on the discover sweep (server-side matching).
	Filter wire.DiscoverFilter
	// SweepInterval is how often to re-discover. Default 5m (spec
	// recommendation). Announcements arrive immediately regardless.
	SweepInterval time.Duration
	// StaleAfter drops a tool not seen (sweep or announce) for this long.
	// Default 3× SweepInterval.
	StaleAfter time.Duration
	// OnChange fires after the tool set changes (added, dropped, or card
	// updated). Called from registry goroutines; keep it fast.
	OnChange func()
}

// Registry maintains a live snapshot of the network tools an agent may use:
// an initial sweep plus announce subscriptions pick up new tools the moment
// they deploy, and periodic sweeps age dead ones out.
type Registry struct {
	client *Client
	opts   RegistryOptions

	mu    sync.Mutex
	tools map[string]registryEntry

	announceSub *nats.Subscription
	stop        chan struct{}
	stopOnce    sync.Once
}

type registryEntry struct {
	card     wire.ToolCard
	lastSeen time.Time
}

// NewRegistry creates a registry; call Start to begin watching.
func NewRegistry(client *Client, opts RegistryOptions) *Registry {
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = 5 * time.Minute
	}
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = 3 * opts.SweepInterval
	}
	return &Registry{
		client: client,
		opts:   opts,
		tools:  map[string]registryEntry{},
		stop:   make(chan struct{}),
	}
}

// Start does a synchronous initial sweep, then watches announcements and
// sweeps periodically until Stop.
func (r *Registry) Start(ctx context.Context) error {
	sub, err := r.client.nc.Subscribe(wire.ToolAnnounceSubject, func(msg *nats.Msg) {
		var card wire.ToolCard
		if err := json.Unmarshal(msg.Data, &card); err != nil || card.Name == "" {
			return
		}
		if !card.MatchesFilter(r.opts.Filter) || !r.allowed(card.Name) {
			return
		}
		r.absorb([]wire.ToolCard{card})
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", wire.ToolAnnounceSubject, err)
	}
	r.announceSub = sub

	r.sweep(ctx)

	go func() {
		ticker := time.NewTicker(r.opts.SweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				sweepCtx, cancel := context.WithTimeout(context.Background(), r.opts.SweepInterval/2)
				r.sweep(sweepCtx)
				cancel()
			}
		}
	}()
	return nil
}

// Stop ends watching. The last snapshot stays readable.
func (r *Registry) Stop() {
	r.stopOnce.Do(func() {
		close(r.stop)
		if r.announceSub != nil {
			_ = r.announceSub.Drain()
		}
	})
}

// Tools returns the current snapshot, sorted by name.
func (r *Registry) Tools() []wire.ToolCard {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]wire.ToolCard, 0, len(r.tools))
	for _, e := range r.tools {
		out = append(out, e.card)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Get returns one tool's card if present.
func (r *Registry) Get(name string) (wire.ToolCard, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.tools[name]
	return e.card, ok
}

// Run executes a registered tool, sizing the request timeout from the
// tool's advertised timeoutSeconds.
func (r *Registry) Run(ctx context.Context, name string, req wire.ToolRunRequest) (*wire.ToolRunResponse, error) {
	card, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("network tool %q is not in the registry", name)
	}
	timeout := time.Duration(card.TimeoutSeconds)*time.Second + runTimeoutMargin
	return r.client.Run(ctx, name, req, timeout)
}

func (r *Registry) allowed(name string) bool {
	for _, d := range r.opts.Deny {
		if d == name {
			return false
		}
	}
	if len(r.opts.Allow) == 0 {
		return true
	}
	for _, a := range r.opts.Allow {
		if a == name {
			return true
		}
	}
	return false
}

func (r *Registry) sweep(ctx context.Context) {
	cards, err := r.client.Discover(ctx, r.opts.Filter, 0)
	if err != nil {
		log.Printf("tool registry sweep failed: %v", err)
		return
	}
	var kept []wire.ToolCard
	for _, c := range cards {
		if r.allowed(c.Name) {
			kept = append(kept, c)
		}
	}
	r.absorb(kept)
	r.evictStale()
}

func (r *Registry) absorb(cards []wire.ToolCard) {
	changed := false
	now := time.Now()
	r.mu.Lock()
	for _, c := range cards {
		prev, existed := r.tools[c.Name]
		if !existed || prev.card.Version != c.Version || prev.card.Description != c.Description {
			changed = true
		}
		r.tools[c.Name] = registryEntry{card: c, lastSeen: now}
	}
	r.mu.Unlock()
	if changed && r.opts.OnChange != nil {
		r.opts.OnChange()
	}
}

func (r *Registry) evictStale() {
	cutoff := time.Now().Add(-r.opts.StaleAfter)
	changed := false
	r.mu.Lock()
	for name, e := range r.tools {
		if e.lastSeen.Before(cutoff) {
			delete(r.tools, name)
			changed = true
			log.Printf("tool registry: dropping stale tool %q (last seen %s)", name, e.lastSeen.Format(time.RFC3339))
		}
	}
	r.mu.Unlock()
	if changed && r.opts.OnChange != nil {
		r.opts.OnChange()
	}
}
