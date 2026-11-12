// Package progress watches a run's event stream for progress and signals when
// a window passes without any. Only progress extends the window: pings prove
// liveness, not progress, and never reset it.
package progress

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/transactrx/nats-agent/internal/progressclock"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// timer and clock are the test seam over the time package, shared with
// this module's other tests through internal/progressclock.
type (
	timer = progressclock.Timer
	clock = progressclock.Clock
)

func init() {
	progressclock.NewWatcher = func(window time.Duration, c progressclock.Clock) any { return newWatcher(window, c) }
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) timer { return time.AfterFunc(d, f) }

// Watcher closes Expired once no progress has been seen for the window. It is
// safe for concurrent use. Call Stop when the run ends; read Idle and
// LastStage before Stop.
type Watcher struct {
	window  time.Duration
	clock   clock
	expired chan struct{}

	mu    sync.Mutex
	last  time.Time
	stage string
	timer timer
	done  bool // expired or stopped: no timer pending, no further updates
}

// NewWatcher starts a watcher. window <= 0 never expires.
func NewWatcher(window time.Duration) *Watcher {
	return newWatcher(window, realClock{})
}

func newWatcher(window time.Duration, c clock) *Watcher {
	w := &Watcher{window: window, clock: c, expired: make(chan struct{}), last: c.Now()}
	if window > 0 {
		w.mu.Lock()
		w.timer = c.AfterFunc(window, w.fire)
		w.mu.Unlock()
	}
	return w
}

// fire runs when the latest deadline passes. Progress does not reset the
// timer; fire re-arms it for the remainder of the window instead.
func (w *Watcher) fire() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	idle := w.clock.Now().Sub(w.last)
	if idle >= w.window {
		w.done = true
		w.timer = nil
		close(w.expired)
		return
	}
	w.timer = w.clock.AfterFunc(w.window-idle, w.fire)
}

// Observe records ev as progress when ev.IsProgress(); a progress data event
// also sets the stage.
func (w *Watcher) Observe(ev *wire.AgentEvent) {
	if ev == nil || !ev.IsProgress() {
		return
	}
	stage := ""
	if ev.Type == wire.EventData && ev.Kind == wire.DataKindProgress {
		var p wire.ProgressPayload
		if json.Unmarshal(ev.Payload, &p) == nil {
			stage = p.Stage
		}
	}
	w.Touch(stage)
}

// Touch records progress from a source other than a stream frame. An empty
// stage keeps the previous one.
func (w *Watcher) Touch(stage string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	w.last = w.clock.Now()
	if stage != "" {
		w.stage = stage
	}
}

// Expired is closed once when the window passes with no progress. It is never
// closed after Stop or when the window is <= 0.
func (w *Watcher) Expired() <-chan struct{} { return w.expired }

// Idle is the time since the last progress (or since the watcher started).
func (w *Watcher) Idle() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.clock.Now().Sub(w.last)
}

// LastStage is the stage of the latest progress event or Touch, if any.
func (w *Watcher) LastStage() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stage
}

// Stop releases the timer. Safe to call more than once and after expiry.
func (w *Watcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.done = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
}
