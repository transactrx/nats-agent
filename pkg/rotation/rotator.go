// Package rotation keeps a long run supplied with short-lived tokens: it
// redeems a new token before the current one runs low, pushes it to the
// agent, and retires the old one once it is no longer needed. It knows
// nothing about NATS or identity; every side effect goes through Config.
package rotation

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Token is one delegation token and when it expires.
type Token struct {
	Value        string
	ExpiresAt    time.Time
	RunSessionID string
}

// Config drives Run. Redeem and Push are required.
type Config struct {
	TTL          time.Duration // requested token life (IDT2_TTL)
	RotateBefore time.Duration // rotate when remaining <= this (8m)
	GiveUpBefore time.Duration // stop retrying when the current token has this left (2m)
	Redeem       func(ctx context.Context, ttl time.Duration) (Token, error)
	Push         func(ctx context.Context, t Token) error // deliver to the agent (renew, with ExpiresAtMs = t.ExpiresAt)
	Retire       func(ctx context.Context, old Token)     // called once old.ExpiresAt passed or overlap ended; optional
	Overlap      time.Duration                            // how long the old token must stay untouched (= RotateBefore)
}

// ErrRotationFailed is returned (wrapped with the last error) when no new
// token could be redeemed and pushed before the give-up point.
var ErrRotationFailed = errors.New("token rotation failed")

// Retry backoff after a failed Redeem or Push: 5s, 10s, 20s, then 30s.
const (
	backoffFirst = 5 * time.Second
	backoffMax   = 30 * time.Second
)

// clock is the test seam over the time package.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Run rotates first and its successors until ctx ends (nil) or a rotation
// fails (ErrRotationFailed). first is not pushed: the agent already has it.
// Each rotation is scheduled from the ExpiresAt the previous Redeem
// returned, never from TTL. A redeemed token that does not outlive the
// current one counts as a failed attempt.
func Run(ctx context.Context, cfg Config, first Token) error {
	return run(ctx, cfg, first, realClock{})
}

func run(ctx context.Context, cfg Config, first Token, c clock) error {
	if first.ExpiresAt.IsZero() {
		return errors.New("rotation: first token has no expiry")
	}
	if cfg.Redeem == nil || cfg.Push == nil {
		return errors.New("rotation: Redeem and Push are required")
	}
	// Pending retires end with Run.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cur := first
	for {
		if !sleep(ctx, c, cur.ExpiresAt.Sub(c.Now())-cfg.RotateBefore) {
			return nil
		}
		next, err := rotate(ctx, cfg, c, cur)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if cfg.Retire != nil {
			go retire(ctx, cfg, c, cur)
		}
		cur = next
	}
}

// rotate redeems and pushes one successor of cur, retrying with backoff
// until cur.ExpiresAt - GiveUpBefore.
func rotate(ctx context.Context, cfg Config, c clock, cur Token) (Token, error) {
	giveUp := cur.ExpiresAt.Add(-cfg.GiveUpBefore)
	backoff := backoffFirst
	var pending *Token // redeemed but not yet pushed
	for {
		var err error
		if pending == nil {
			var t Token
			t, err = cfg.Redeem(ctx, cfg.TTL)
			if err == nil && !t.ExpiresAt.After(cur.ExpiresAt) {
				err = fmt.Errorf("redeemed token expires %s, not after current %s", t.ExpiresAt, cur.ExpiresAt)
			}
			if err == nil {
				pending = &t
			}
		}
		if pending != nil {
			if err = cfg.Push(ctx, *pending); err == nil {
				return *pending, nil
			}
		}
		if ctx.Err() != nil {
			return Token{}, ctx.Err()
		}
		left := giveUp.Sub(c.Now())
		if left <= 0 {
			return Token{}, fmt.Errorf("%w: %v", ErrRotationFailed, err)
		}
		if !sleep(ctx, c, min(backoff, left)) {
			return Token{}, ctx.Err()
		}
		if !c.Now().Before(giveUp) {
			return Token{}, fmt.Errorf("%w: %v", ErrRotationFailed, err)
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// retire calls cfg.Retire for old once Overlap has passed, unless ctx ends
// first.
func retire(ctx context.Context, cfg Config, c clock, old Token) {
	if !sleep(ctx, c, cfg.Overlap) {
		return
	}
	cfg.Retire(ctx, old)
}

// sleep waits d (immediately when d <= 0); false when ctx ended first.
func sleep(ctx context.Context, c clock, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case <-c.After(d):
		return ctx.Err() == nil
	}
}
