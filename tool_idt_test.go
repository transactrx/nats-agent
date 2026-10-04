package natsagent_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/idt"
	"github.com/transactrx/nats-agent/pkg/tool"
	"github.com/transactrx/nats-agent/pkg/toolclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// whoamiTool is a CallTool: it reports the call it received and the token
// forwarded on its context.
type whoamiTool struct {
	mu    sync.Mutex
	calls []tool.Call
	block chan struct{} // when non-nil, RunCall waits on it
}

func (*whoamiTool) Name() string                { return "whoami" }
func (*whoamiTool) Description() string         { return "Reports the verified caller." }
func (*whoamiTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (*whoamiTool) Run(context.Context, map[string]any) (string, error) {
	return "", errors.New("Run must not be called on a CallTool")
}

func (w *whoamiTool) RunCall(ctx context.Context, call tool.Call) ([]wire.ToolResultContent, error) {
	if w.block != nil {
		<-w.block
	}
	w.mu.Lock()
	w.calls = append(w.calls, call)
	w.mu.Unlock()
	return []wire.ToolResultContent{
		{Text: call.Identity.UserID + "|" + call.SessionID + "|" + idt.TokenFromContext(ctx)},
		{JSON: []byte(`{"verified":` + map[bool]string{true: "true", false: "false"}[call.Identity.Verified] + `}`)},
	}, nil
}

func (w *whoamiTool) seen() []tool.Call {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]tool.Call(nil), w.calls...)
}

func startSecuredTool(t *testing.T, w *whoamiTool, identitySubject string, maxConcurrent int) {
	t.Helper()
	h := tool.NewHostWithNATS(testURL, "", "")
	if err := h.SetIDTValidation(idt.Validation{Enabled: true, Subject: identitySubject, Timeout: 2 * time.Second, CacheTTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := h.Register(w, &tool.Info{Access: &wire.AgentAccess{AppID: "appTest", FunctionID: "fnTest"}, MaxConcurrent: maxConcurrent}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := h.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(h.Shutdown)
}

func assertToolError(t *testing.T, err error, status, code int, msg string) {
	t.Helper()
	var se *toolclient.ServiceError
	if !errors.As(err, &se) {
		t.Fatalf("want toolclient.ServiceError, got %v", err)
	}
	if se.Status != status || se.ApiStatusCode != code || (msg != "" && se.ErrorMessage != msg) {
		t.Fatalf("want %d/%d/%s, got %d/%d/%s", status, code, msg, se.Status, se.ApiStatusCode, se.ErrorMessage)
	}
}

func TestToolRunIDTAllowedDeniedMissing(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})
	w := &whoamiTool{}
	startSecuredTool(t, w, subj, 0)
	tc := toolclient.NewFromConn(testConn(t))

	// Allowed: the verified user replaces the caller-asserted userId, and
	// the token reaches the tool's context for onward calls. The legacy
	// agentclient.WithIDT helper sets the same token.
	ctx := agentclient.WithIDT(context.Background(), "IDT-good.c")
	resp, err := tc.Run(ctx, "whoami", wire.ToolRunRequest{ToolUseID: "tu1", SessionID: "s1", UserID: "mallory", Input: map[string]any{}}, 0)
	if err != nil {
		t.Fatalf("allowed run: %v", err)
	}
	if resp.Status != wire.ToolStatusSuccess || len(resp.Content) != 2 || resp.Content[0].Text != "alice|s1|IDT-good.c" || string(resp.Content[1].JSON) != `{"verified":true}` {
		t.Fatalf("bad response: %+v", resp)
	}
	if calls := w.seen(); len(calls) != 1 || calls[0].ToolUseID != "tu1" || !calls[0].Identity.Verified || calls[0].Identity.AccountID != "acc1" {
		t.Fatalf("tool saw %+v", calls)
	}

	for _, tc2 := range []struct{ token, reason string }{
		{"IDT-bad.c", "DENIED_FN"},
		{"revoked.x", "TOKEN_REVOKED"},
		{"", "MISSING_IDT"},
	} {
		_, err := tc.Run(idt.WithToken(context.Background(), tc2.token), "whoami", wire.ToolRunRequest{Input: map[string]any{}}, 0)
		assertToolError(t, err, 403, wire.CodeForbidden, tc2.reason)
	}
	if got := len(w.seen()); got != 1 {
		t.Fatalf("denied runs must not execute the tool, ran %d times", got)
	}

	card, err := tc.Card(context.Background(), "whoami")
	if err != nil || card.Access == nil || card.Access.FunctionID != "fnTest" || card.ProtocolVersion != "1.2" {
		t.Fatalf("card must be public with access and protocol 1.2: %+v %v", card, err)
	}
}

func TestToolHostRefusesValidationWithoutAccess(t *testing.T) {
	h := tool.NewHostWithNATS(testURL, "", "")
	if err := h.SetIDTValidation(idt.Validation{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := h.Register(echoTool{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(); err == nil {
		h.Shutdown()
		t.Fatal("Start must refuse IDT validation for a tool without Access")
	}
	if err := h.Register(&whoamiTool{}, &tool.Info{Access: &wire.AgentAccess{AppID: "a"}}); err == nil {
		t.Fatal("Register must refuse a partial Access")
	}
}

func TestToolRunBusyAndContextCancel(t *testing.T) {
	subj := "test.identity.validate." + t.Name()
	startFakeIdentity(t, subj, map[string]string{"IDT-good.c": "alice"})
	w := &whoamiTool{block: make(chan struct{})}
	startSecuredTool(t, w, subj, 1)
	tc := toolclient.NewFromConn(testConn(t))
	ctx := idt.WithToken(context.Background(), "IDT-good.c")

	// The first run occupies the only slot. The caller gives up after
	// 300ms: Run must return promptly with the context error.
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := tc.Run(short, "whoami", wire.ToolRunRequest{Input: map[string]any{}}, 0)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
		t.Fatalf("want prompt context deadline error, got %v after %s", err, time.Since(start))
	}

	// The tool is still running (it is not interrupted), so a second run is
	// refused for capacity.
	_, err = tc.Run(ctx, "whoami", wire.ToolRunRequest{Input: map[string]any{}}, 5*time.Second)
	assertToolError(t, err, 429, wire.CodeBusy, "")

	close(w.block)
	deadline := time.Now().Add(5 * time.Second)
	for len(w.seen()) != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	resp, err := tc.Run(ctx, "whoami", wire.ToolRunRequest{Input: map[string]any{}}, 5*time.Second)
	if err != nil || resp.Status != wire.ToolStatusSuccess {
		t.Fatalf("slot must free after the first run finishes: %+v %v", resp, err)
	}
}
