// Package durable is an execution-lifecycle proof, not a production agent host.
package durable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNotFound     = errors.New("session not found")
	ErrConflict     = errors.New("revision conflict")
	ErrBusy         = errors.New("session has an active run")
	ErrLeaseLost    = errors.New("worker no longer owns the run")
	ErrUncertain    = errors.New("step outcome unknown; reconciliation required")
	ErrUnauthorized = errors.New("execution grant expired or revoked")
)

type Scope struct{ Tenant, User, Agent, Session string }

func (s Scope) Key() string {
	b, _ := json.Marshal(s)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (s Scope) valid() bool {
	return s.Tenant != "" && s.User != "" && s.Agent != "" && s.Session != ""
}

// References identify immutable artifacts; never store credentials here.
type Environment struct{ ImageDigest, ManifestRef, WorkspaceRef string }
type Step struct {
	InputHash, OutputRef, State string
	Epoch                       uint64
}
type Run struct {
	ID, RequestHash, State, Owner      string
	Epoch                              uint64
	LeaseUntil, Deadline, GrantExpires time.Time
	GrantID                            string
	CancelRequested                    bool
	Steps                              map[string]Step
}
type Session struct {
	Scope       Scope
	Revision    uint64
	Environment Environment
	Run         *Run
}

// Store must return independent copies and implement linearizable CAS within
// one authoritative region. Eventual global-table replicas are insufficient.
type Store interface {
	Load(context.Context, string) (Session, error)
	CAS(context.Context, string, uint64, Session) error
}
type Authorizer interface {
	Allowed(context.Context, Scope, string) (bool, error)
}
type Runtime struct {
	Store     Store
	Authority Authorizer
	Now       func() time.Time
	// Wake is best effort. Failure cannot change durable acceptance.
	Wake func(context.Context, string) error
}
type Claim struct {
	Scope        Scope
	RunID, Owner string
	Epoch        uint64
}

func (r Runtime) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
func hash(s string) string { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
func (r Runtime) authorized(ctx context.Context, s Session) error {
	if s.Run == nil || !r.now().Before(s.Run.GrantExpires) || r.Authority == nil {
		return ErrUnauthorized
	}
	ok, err := r.Authority.Allowed(ctx, s.Scope, s.Run.GrantID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrUnauthorized
	}
	return nil
}
func pinnedImage(image string) bool {
	_, digest, ok := strings.Cut(image, "@sha256:")
	if !ok || len(digest) != 64 {
		return false
	}
	b, err := hex.DecodeString(digest)
	return err == nil && len(b) == 32
}

func (r Runtime) Create(ctx context.Context, scope Scope, env Environment) error {
	if !scope.valid() || !pinnedImage(env.ImageDigest) || env.ManifestRef == "" {
		return fmt.Errorf("complete scope and pinned environment required")
	}
	return r.Store.CAS(ctx, scope.Key(), 0, Session{Scope: scope, Revision: 1, Environment: env})
}

// Submit deduplicates the current run only. Production needs a retained request
// ledger and separate history records before accepting subsequent turns.
func (r Runtime) Submit(ctx context.Context, scope Scope, id, input, grant string, deadline, grantExpires time.Time) error {
	if id == "" || grant == "" || !r.now().Before(deadline) || !r.now().Before(grantExpires) {
		return fmt.Errorf("run id, grant and future deadlines required")
	}
	s, err := r.Store.Load(ctx, scope.Key())
	if err != nil {
		return err
	}
	if s.Run != nil {
		if s.Run.ID == id {
			if s.Run.RequestHash != hash(input) || s.Run.GrantID != grant {
				return ErrConflict
			}
			return r.authorized(ctx, s)
		}
		if s.Run.State != "succeeded" && s.Run.State != "cancelled" && s.Run.State != "failed" {
			return ErrBusy
		}
	}
	s.Run = &Run{ID: id, RequestHash: hash(input), State: "queued", GrantID: grant, GrantExpires: grantExpires, Deadline: deadline, Steps: map[string]Step{}}
	if err = r.authorized(ctx, s); err != nil {
		return err
	}
	if err = r.update(ctx, s); err != nil {
		return err
	}
	if r.Wake != nil {
		_ = r.Wake(ctx, scope.Key())
	}
	return nil
}

// Claim increments a fence on every ownership change. This fences checkpoints,
// NOT arbitrary writes to a shared filesystem. Each attempt needs isolated
// scratch storage restored from the last committed immutable workspace.
func (r Runtime) Claim(ctx context.Context, scope Scope, owner string, ttl time.Duration) (Claim, error) {
	if owner == "" || ttl <= 0 {
		return Claim{}, fmt.Errorf("owner and positive lease required")
	}
	s, err := r.Store.Load(ctx, scope.Key())
	if err != nil {
		return Claim{}, err
	}
	run := s.Run
	if run == nil {
		return Claim{}, ErrNotFound
	}
	if run.State != "queued" && (run.State != "running" || r.now().Before(run.LeaseUntil)) {
		return Claim{}, ErrBusy
	}
	if run.CancelRequested || !r.now().Before(run.Deadline) {
		return Claim{}, ErrLeaseLost
	}
	if err = r.authorized(ctx, s); err != nil {
		return Claim{}, err
	}
	run.Owner = owner
	run.Epoch++
	run.State = "running"
	run.LeaseUntil = minTime(r.now().Add(ttl), run.Deadline, run.GrantExpires)
	if err = r.update(ctx, s); err != nil {
		return Claim{}, err
	}
	return Claim{Scope: scope, RunID: run.ID, Owner: owner, Epoch: run.Epoch}, nil
}
func minTime(values ...time.Time) time.Time {
	out := values[0]
	for _, v := range values[1:] {
		if v.Before(out) {
			out = v
		}
	}
	return out
}
func (r Runtime) owned(ctx context.Context, c Claim) (Session, error) {
	s, err := r.Store.Load(ctx, c.Scope.Key())
	if err != nil {
		return s, err
	}
	run := s.Run
	if run == nil || run.ID != c.RunID || run.Owner != c.Owner || run.Epoch != c.Epoch || run.State != "running" || run.CancelRequested || !r.now().Before(run.LeaseUntil) || !r.now().Before(run.Deadline) {
		return s, ErrLeaseLost
	}
	if err = r.authorized(ctx, s); err != nil {
		return s, err
	}
	return s, nil
}
func (r Runtime) update(ctx context.Context, s Session) error {
	old := s.Revision
	s.Revision++
	return r.Store.CAS(ctx, s.Scope.Key(), old, s)
}
func (r Runtime) Heartbeat(ctx context.Context, c Claim, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("positive lease required")
	}
	s, err := r.owned(ctx, c)
	if err != nil {
		return err
	}
	s.Run.LeaseUntil = minTime(r.now().Add(ttl), s.Run.Deadline, s.Run.GrantExpires)
	return r.update(ctx, s)
}

// BeginStep persists intent before a model/tool call. An interrupted 'started'
// step cannot be blindly replayed: reconcile an idempotency key or pause.
func (r Runtime) BeginStep(ctx context.Context, c Claim, id, input string) (Step, error) {
	s, err := r.owned(ctx, c)
	if err != nil {
		return Step{}, err
	}
	if id == "" {
		return Step{}, fmt.Errorf("step id required")
	}
	fingerprint := hash(input)
	if step, ok := s.Run.Steps[id]; ok {
		if step.InputHash != fingerprint {
			return Step{}, ErrConflict
		}
		if step.State == "completed" {
			return step, nil
		}
		return step, ErrUncertain
	}
	step := Step{InputHash: fingerprint, State: "started", Epoch: c.Epoch}
	s.Run.Steps[id] = step
	return step, r.update(ctx, s)
}

// CommitStep publishes output and environment version atomically. The artifact
// uploader must flush and verify the immutable objects before this operation.
func (r Runtime) CommitStep(ctx context.Context, c Claim, id, outputRef, workspaceRef, manifestRef string) error {
	s, err := r.owned(ctx, c)
	if err != nil {
		return err
	}
	step, ok := s.Run.Steps[id]
	if !ok || step.State != "started" || step.Epoch != c.Epoch || outputRef == "" {
		return ErrConflict
	}
	step.State = "completed"
	step.OutputRef = outputRef
	s.Run.Steps[id] = step
	if workspaceRef != "" {
		s.Environment.WorkspaceRef = workspaceRef
	}
	if manifestRef != "" {
		s.Environment.ManifestRef = manifestRef
	}
	return r.update(ctx, s)
}
func (r Runtime) Finish(ctx context.Context, c Claim) error {
	s, err := r.owned(ctx, c)
	if err != nil {
		return err
	}
	for _, step := range s.Run.Steps {
		if step.State != "completed" {
			return ErrUncertain
		}
	}
	s.Run.State = "succeeded"
	s.Run.Owner = ""
	return r.update(ctx, s)
}

// Cancel is explicit. The authenticated API must verify caller permissions.
// Keep the session busy until the sandbox is confirmed stopped; a lease does
// not terminate a process that may still be changing files or invoking tools.
func (r Runtime) Cancel(ctx context.Context, scope Scope, runID string) error {
	s, err := r.Store.Load(ctx, scope.Key())
	if err != nil {
		return err
	}
	if s.Run == nil || s.Run.ID != runID {
		return ErrNotFound
	}
	if s.Run.State == "succeeded" || s.Run.State == "failed" || s.Run.State == "cancelled" {
		return nil
	}
	s.Run.CancelRequested = true
	return r.update(ctx, s)
}
