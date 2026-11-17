// Package progressclock is the clock seam of progress.Watcher, shared inside
// this module only so other packages' tests can drive a real Watcher on a
// manual clock. Not part of the public API.
package progressclock

import (
	"sort"
	"sync"
	"time"
)

// Timer is a pending AfterFunc.
type Timer interface{ Stop() bool }

// Clock is what a Watcher reads time from.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
}

// NewWatcher builds a *progress.Watcher running on c. It returns any to
// avoid an import cycle; package progress sets it at init.
var NewWatcher func(window time.Duration, c Clock) any

// Fake is a manual Clock: Advance fires due timers synchronously, in order.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	c       *Fake
	at      time.Time
	f       func()
	stopped bool
}

// NewFake starts a manual clock at a fixed instant.
func NewFake() *Fake { return &Fake{now: time.Unix(1_000_000, 0)} }

func (c *Fake) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Fake) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

// Advance moves the clock by d, running every timer that comes due (also
// ones scheduled by timers fired on the way) outside the clock's lock.
func (c *Fake) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		sort.SliceStable(c.timers, func(i, j int) bool { return c.timers[i].at.Before(c.timers[j].at) })
		var due *fakeTimer
		for i, t := range c.timers {
			if t.stopped {
				continue
			}
			if !t.at.After(target) {
				due = t
				c.timers = append(c.timers[:i], c.timers[i+1:]...)
			}
			break
		}
		if due == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		due.stopped = true
		c.now = due.at
		c.mu.Unlock()
		due.f()
	}
}
