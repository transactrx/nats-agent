package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type memoryStore struct {
	mu   sync.Mutex
	rows map[string]Session
}

func clone(s Session) Session {
	b, _ := json.Marshal(s)
	var out Session
	_ = json.Unmarshal(b, &out)
	return out
}
func (m *memoryStore) Load(ctx context.Context, k string) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rows[k]
	if !ok {
		return s, ErrNotFound
	}
	return clone(s), nil
}
func (m *memoryStore) CAS(ctx context.Context, k string, v uint64, next Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.rows[k]
	if s.Revision != v || next.Revision != v+1 || next.Scope.Key() != k {
		return ErrConflict
	}
	m.rows[k] = clone(next)
	return nil
}

type auth struct{ revoked atomic.Bool }

func (a *auth) Allowed(context.Context, Scope, string) (bool, error) { return !a.revoked.Load(), nil }
func storeForTest(t *testing.T) Store {
	t.Helper()
	if table := os.Getenv("DURABLE_PROBE_TABLE"); table != "" {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return DynamoStore{Client: dynamodb.NewFromConfig(cfg), Table: table}
	}
	return &memoryStore{rows: map[string]Session{}}
}
func setup(t *testing.T) (Runtime, Scope, *time.Time, *auth) {
	t.Helper()
	now := time.Now().UTC()
	a := &auth{}
	r := Runtime{Store: storeForTest(t), Authority: a, Now: func() time.Time { return now }}
	s := Scope{Tenant: "test-tenant", User: "test-user", Agent: "test-agent", Session: fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())}
	must(t, r.Create(context.Background(), s, Environment{ImageDigest: "registry/runtime@sha256:" + strings.Repeat("a", 64), ManifestRef: "s3://test/manifest-v1"}))
	return r, s, &now, a
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func submit(t *testing.T, r Runtime, s Scope, id string) {
	t.Helper()
	must(t, r.Submit(context.Background(), s, id, "request", "grant-1", r.now().Add(time.Hour), r.now().Add(time.Hour)))
}
func claim(t *testing.T, r Runtime, s Scope, owner string) Claim {
	t.Helper()
	c, err := r.Claim(context.Background(), s, owner, time.Minute)
	must(t, err)
	return c
}

func TestDetachedRecoveryAndNextTurn(t *testing.T) {
	r, s, now, _ := setup(t)
	ctx := context.Background()
	r.Wake = func(context.Context, string) error { return errors.New("NATS disconnected") }
	browser, cancel := context.WithCancel(ctx)
	must(t, r.Submit(browser, s, "r1", "request", "grant-1", now.Add(time.Hour), now.Add(time.Hour)))
	cancel()
	first := claim(t, r, s, "worker-a")
	_, err := r.BeginStep(ctx, first, "install-package", "package==1.2.3")
	must(t, err)
	must(t, r.CommitStep(ctx, first, "install-package", "s3://test/install-result", "s3://test/workspace-v2", "s3://test/manifest-v2"))
	*now = now.Add(2 * time.Minute)
	restarted := Runtime{Store: r.Store, Authority: r.Authority, Now: r.Now} // no browser or original worker state
	second := claim(t, restarted, s, "worker-b")
	if second.Epoch <= first.Epoch {
		t.Fatal("fence did not advance")
	}
	if err = r.Heartbeat(ctx, first, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if err = r.CommitStep(ctx, first, "install-package", "bad", "bad", "bad"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale checkpoint: %v", err)
	}
	step, err := restarted.BeginStep(ctx, second, "install-package", "package==1.2.3")
	must(t, err)
	if step.State != "completed" || step.OutputRef != "s3://test/install-result" {
		t.Fatal("completed step lost")
	}
	must(t, restarted.Finish(ctx, second))
	submit(t, restarted, s, "r2")
	state, err := r.Store.Load(ctx, s.Key())
	must(t, err)
	if state.Environment.ManifestRef != "s3://test/manifest-v2" || state.Environment.WorkspaceRef != "s3://test/workspace-v2" {
		t.Fatal("next turn lost environment")
	}
}
func TestOnlyOneClaimant(t *testing.T) {
	r, s, _, _ := setup(t)
	submit(t, r, s, "r1")
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := r.Claim(context.Background(), s, fmt.Sprint(i), time.Minute)
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrBusy) && !errors.Is(err, ErrConflict) {
				t.Errorf("claim: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d claimants won", wins.Load())
	}
}
func TestUnknownOutcomeRequiresReconciliation(t *testing.T) {
	r, s, now, _ := setup(t)
	submit(t, r, s, "r1")
	first := claim(t, r, s, "a")
	_, err := r.BeginStep(context.Background(), first, "external-action", "input")
	must(t, err)
	*now = now.Add(2 * time.Minute)
	second := claim(t, r, s, "b")
	_, err = r.BeginStep(context.Background(), second, "external-action", "input")
	if !errors.Is(err, ErrUncertain) {
		t.Fatalf("must not repeat unknown action: %v", err)
	}
	if err = r.CommitStep(context.Background(), second, "external-action", "guessed-result", "", ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("replacement must reconcile an old attempt's result explicitly: %v", err)
	}
	if err = r.Finish(context.Background(), second); !errors.Is(err, ErrUncertain) {
		t.Fatalf("cannot finish unknown action: %v", err)
	}
}
func TestAdmissionAndIdentity(t *testing.T) {
	r, s, now, a := setup(t)
	ctx := context.Background()
	submit(t, r, s, "r1")
	submit(t, r, s, "r1")
	if err := r.Submit(ctx, s, "r1", "changed", "grant-1", now.Add(time.Hour), now.Add(time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed request: %v", err)
	}
	if err := r.Submit(ctx, s, "r2", "request", "grant-1", now.Add(time.Hour), now.Add(time.Hour)); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent turn: %v", err)
	}
	c := claim(t, r, s, "a")
	a.revoked.Store(true)
	if err := r.Heartbeat(ctx, c, time.Minute); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked grant: %v", err)
	}
	a.revoked.Store(false)
	must(t, r.Cancel(ctx, s, "r1"))
	if err := r.Heartbeat(ctx, c, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("cancelled run: %v", err)
	}
	if _, err := r.Claim(ctx, s, "b", time.Minute); err == nil {
		t.Fatal("cancelled run reclaimed")
	}
}
func TestGrantExpiryAndScopeIsolation(t *testing.T) {
	r, s, now, _ := setup(t)
	ctx := context.Background()
	must(t, r.Submit(ctx, s, "r1", "request", "grant-1", now.Add(time.Hour), now.Add(10*time.Second)))
	c := claim(t, r, s, "a")
	*now = now.Add(11 * time.Second)
	if err := r.Heartbeat(ctx, c, time.Minute); err == nil {
		t.Fatal("expired grant kept alive")
	}
	if _, err := r.Claim(ctx, s, "b", time.Minute); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expired grant reclaimed: %v", err)
	}
	other := s
	other.Tenant = "another-tenant"
	if _, err := r.Store.Load(ctx, other.Key()); !errors.Is(err, ErrNotFound) {
		t.Fatal("tenant scopes overlap")
	}
}
func TestCancelledSubmissionIsNotAccepted(t *testing.T) {
	r, s, now, _ := setup(t)
	called := false
	r.Wake = func(context.Context, string) error { called = true; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Submit(ctx, s, "r1", "request", "grant-1", now.Add(time.Hour), now.Add(time.Hour)); err == nil {
		t.Fatal("cancelled submission acknowledged")
	}
	if called {
		t.Fatal("notification sent without durable acceptance")
	}
}

// Opt-in process-loss proof against a scratch DynamoDB table. The child exits
// without cleanup after checkpointing; the parent reconstructs all runtime
// state from DynamoDB using a fresh SDK client.
func TestProcessRecovery(t *testing.T) {
	if os.Getenv("DURABLE_PROBE_TABLE") == "" {
		t.Skip("set DURABLE_PROBE_TABLE for AWS proof")
	}
	scope := Scope{Tenant: "test-tenant", User: "test-user", Agent: "test-agent", Session: os.Getenv("DURABLE_PROBE_SESSION")}
	if scope.Session != "" {
		r := Runtime{Store: storeForTest(t), Authority: &auth{}}
		ctx := context.Background()
		must(t, r.Create(ctx, scope, Environment{ImageDigest: "runtime@sha256:" + strings.Repeat("a", 64), ManifestRef: "s3://test/locked-packages"}))
		submit(t, r, scope, "r1")
		c := claim(t, r, scope, "child")
		_, err := r.BeginStep(ctx, c, "model-1", "input")
		must(t, err)
		must(t, r.CommitStep(ctx, c, "model-1", "s3://test/model-output", "s3://test/workspace-1", ""))
		os.Exit(23)
	}
	scope.Session = fmt.Sprintf("process-%d", time.Now().UnixNano())
	child := exec.Command(os.Args[0], "-test.run=^TestProcessRecovery$", "-test.v")
	child.Env = append(os.Environ(), "DURABLE_PROBE_SESSION="+scope.Session)
	output, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("child failed: %v %s", err, output)
	}
	ctx := context.Background()
	store := storeForTest(t)
	state, err := store.Load(ctx, scope.Key())
	must(t, err)
	r := Runtime{Store: store, Authority: &auth{}, Now: func() time.Time { return state.Run.LeaseUntil.Add(time.Second) }}
	c := claim(t, r, scope, "replacement")
	step, err := r.BeginStep(ctx, c, "model-1", "input")
	must(t, err)
	if step.OutputRef != "s3://test/model-output" {
		t.Fatal("lost committed output")
	}
	must(t, r.Finish(ctx, c))
}

func TestEnvironmentRequiresPinnedImage(t *testing.T) {
	r := Runtime{Store: storeForTest(t), Authority: &auth{}}
	s := Scope{Tenant: "test", User: "test", Agent: "test", Session: fmt.Sprint(time.Now().UnixNano())}
	for _, image := range []string{"runtime:latest", "runtime@sha256:bad", ""} {
		if err := r.Create(context.Background(), s, Environment{ImageDigest: image, ManifestRef: "s3://test/manifest"}); err == nil {
			t.Fatalf("accepted mutable/invalid image %q", image)
		}
	}
}
