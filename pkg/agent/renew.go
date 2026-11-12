package agent

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// renewSubjectPrefix + 22 random chars is a run's private renew subject.
const renewSubjectPrefix = "_RENEW."

// RenewResponse.Reason values when a renew is refused.
const (
	renewBadRequest       = "bad_request"
	renewWrongRun         = "wrong_run"
	renewInvalidToken     = "invalid_token"
	renewIdentityMismatch = "identity_mismatch"
)

// tokenHolder is a run's current IDT, swapped by accepted renews. Safe for
// concurrent use.
type tokenHolder struct {
	mu      sync.Mutex
	current string
	expiry  time.Time // zero = unknown
	hooks   []func(idt string)
	closed  bool // run ended: renews are ignored
}

// CurrentIDT is the latest accepted token of this run (the first one until a
// renew is accepted). Identity.IDT always keeps the first token. Use it for
// every new onward call; calls already in flight keep the token they began
// with.
func (t *Turn) CurrentIDT() string {
	if t.tok == nil {
		return t.Identity.IDT
	}
	t.tok.mu.Lock()
	defer t.tok.mu.Unlock()
	return t.tok.current
}

// OnRenew registers fn, called with the new token after each accepted renew
// (before the renew is acknowledged). fn must not block for long.
func (t *Turn) OnRenew(fn func(idt string)) {
	if t.tok == nil || fn == nil {
		return
	}
	t.tok.mu.Lock()
	t.tok.hooks = append(t.tok.hooks, fn)
	t.tok.mu.Unlock()
}

// IDTExpiry is the expiry of the current token as carried by the last
// accepted renew; false before the first renew or when it carried none.
func (t *Turn) IDTExpiry() (time.Time, bool) {
	if t.tok == nil {
		return time.Time{}, false
	}
	t.tok.mu.Lock()
	defer t.tok.mu.Unlock()
	return t.tok.expiry, !t.tok.expiry.IsZero()
}

// swap installs a renewed token and returns the hooks to call; false when
// the run already ended.
func (h *tokenHolder) swap(token string, expiresAtMs int64) ([]func(string), bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, false
	}
	h.current = token
	h.expiry = time.Time{}
	if expiresAtMs > 0 {
		h.expiry = time.UnixMilli(expiresAtMs)
	}
	return slices.Clone(h.hooks), true
}

func (h *tokenHolder) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

func (h *tokenHolder) close() {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
}

func newRenewSubject() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return renewSubjectPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// subscribeRenew opens turn's private renew subject on this instance's
// connection. The caller unsubscribes when the run ends.
func (a *Agent) subscribeRenew(turn *Turn) (string, *nats.Subscription, error) {
	subject, err := newRenewSubject()
	if err != nil {
		return "", nil, err
	}
	sub, err := a.nc.Subscribe(subject, func(m *nats.Msg) {
		resp := a.applyRenew(turn, m.Data)
		if resp == nil || m.Reply == "" {
			return
		}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	if err != nil {
		return "", nil, err
	}
	return subject, sub, nil
}

// applyRenew decides one renew request. nil = the run has ended: no reply.
func (a *Agent) applyRenew(turn *Turn, body []byte) *wire.RenewResponse {
	if turn.tok.isClosed() {
		return nil
	}
	logger := turn.Logger
	if logger == nil {
		logger = log.Default()
	}
	reject := func(reason string) *wire.RenewResponse {
		logger.Printf("run %s: renew rejected reason=%s", turn.RunID, reason)
		return &wire.RenewResponse{Applied: false, Reason: reason}
	}
	var req wire.RenewRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return reject(renewBadRequest)
	}
	if req.RunID != turn.RunID {
		return reject(renewWrongRun)
	}
	token := strings.TrimSpace(req.IDT)
	if token == "" {
		return reject(renewInvalidToken)
	}
	// Fresh check: a cached allow must not admit a token revoked since.
	id, aerr := a.idt.AuthorizeFresh(token, turn.SessionID)
	if aerr != nil {
		return reject(renewInvalidToken)
	}
	if !sameCaller(turn.Identity, id) {
		return reject(renewIdentityMismatch)
	}
	hooks, ok := turn.tok.swap(token, req.ExpiresAtMs)
	if !ok {
		return nil
	}
	for _, fn := range hooks {
		fn(token)
	}
	logger.Printf("run %s: renew applied user=%s account=%s verified=%v", turn.RunID, id.UserID, id.AccountID, id.Verified)
	return &wire.RenewResponse{Applied: true}
}

// sameCaller reports whether next may replace orig's token. A verified run
// needs a verified token for the same user and account. An unverified run
// (validation off, observe-only, fail-open) follows its admission rule, but
// a token that resolves to a different user or account is still refused.
func sameCaller(orig, next Identity) bool {
	if orig.Verified {
		return next.Verified && next.UserID == orig.UserID && next.AccountID == orig.AccountID
	}
	if orig.UserID != "" && next.UserID != "" {
		return next.UserID == orig.UserID && next.AccountID == orig.AccountID
	}
	return true
}
